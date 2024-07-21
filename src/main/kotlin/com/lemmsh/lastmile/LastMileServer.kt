package com.lemmsh.lastmile

import com.google.common.hash.Hashing
import com.google.protobuf.GeneratedMessageV3
import com.lemmsh.lastmile.Lastmile.Manifest
import com.lemmsh.lastmile.Lastmile.Version
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashMap
import kotlin.concurrent.read
import kotlin.concurrent.write

interface ServerPayloadAdapter<PayloadMessage: GeneratedMessageV3, Key: Any> {
    fun setManifest(manifest: Manifest): PayloadMessage
    fun setVersion(value: PayloadMessage, version: Version): PayloadMessage
    fun key(value: PayloadMessage): Key
}

class ClientStat(val requestReceived: Long,
                 var sequenceSent: Long = 0,
                 var epoch: Long = 0)

data class LastMileServerConfiguration(
    val slowestAllowedDownloadRateKeysPerSecond: Double,
)

class LastMileServer<PayloadMessage: GeneratedMessageV3, Key: Any>(
    private val adapter: ServerPayloadAdapter<PayloadMessage, Key>,
    val name: String,
    val configuration: LastMileServerConfiguration
) {
    @Volatile private var currentEpoch: Long = newEpoch()
    @Volatile private var initialized: Boolean = false

    private var updateLog: LinkedHashMap<Key, CacheEntry<PayloadMessage>> = LinkedHashMap()
    private val updateLogLock: ReentrantReadWriteLock = ReentrantReadWriteLock()
    private var currentSequence: Long = 0L

    private val clientRegistry: ConcurrentMap<UUID, ClientStat> = ConcurrentHashMap()
    private val logger = LoggerFactory.getLogger(javaClass.canonicalName + ".$name")

    fun push(value: PayloadMessage) = updateLogLock.write {
        val key = adapter.key(value)
        updateLog.remove(key)
        currentSequence++
        updateLog.put(key, CacheEntry(value, currentSequence))
    }
    fun pushAll(data: Iterable<PayloadMessage>) = updateLogLock.write {
        for (d in data) {
            val key = adapter.key(d)
            updateLog.remove(key)
            currentSequence++
            updateLog.put(key, CacheEntry(d, currentSequence))
        }
    }
    fun declareInitialized() {
        initialized = true
        logger.info("The cache is initialized")
    }

    fun newEpoch(): Long {
        val s = Hashing.sha256().hashString(InetAddress.getLocalHost().hostName, Charsets.UTF_8)
        return Math.abs(SecureRandom(s.asBytes()).nextLong())
    }
    fun time() = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())

    fun invalidateClients() {
        currentEpoch = newEpoch()
        currentSequence = 0L
        logger.info("Cache invalidation requested. The clients will receive an invalidation command on the next update, the new epoch is $currentEpoch")
    }
    fun size(): Int = updateLogLock.read {
        return updateLog.size
    }

    fun connections() = clientRegistry.toMap()

    private fun currentSequence() = updateLogLock.read { updateLog.values.lastOrNull()?.sequence?:0L }

    private fun regClient(requestId: UUID, lastSeqSeenByClient: Long = 0, clientEpoch: Long = 0) {
        val clientStat = clientRegistry[requestId] ?: ClientStat(requestReceived = time())
        clientStat.epoch = clientEpoch
        clientStat.sequenceSent = lastSeqSeenByClient
        clientRegistry.put(requestId, clientStat)
    }
    private fun deregClient(requestId: UUID) {
        clientRegistry.remove(requestId)
    }
    private fun isSlowConsumer(streamed: Long, streamStarted: Long, now: Long): Boolean {
        val rate = 1000 * streamed.toDouble()/(now - streamStarted)
        logger.trace("rate {}, streamed so far {}", rate, streamed)
        return rate <= configuration.slowestAllowedDownloadRateKeysPerSecond
    }

    fun dataStream(request: Lastmile.StreamRequest,
                   requestId: UUID,
                   filter: ((PayloadMessage) -> Boolean)?
    ): Flow<PayloadMessage> {
        logger.debug(
            "New request {} of epoch {}, starting from the sequence {}",
            requestId,
            request.maxKnownVersionOrNull?.epoch,
            request.maxKnownVersionOrNull?.sequence
        )
        regClient(requestId)

        return flow {
            while (!initialized) {
                delay(1000)
            }
            var clientEpoch = request.maxKnownVersionOrNull?.epoch?:0L
            var lastSequenceSeenByClient = request.maxKnownVersionOrNull?.sequence?:0L
            regClient(requestId, lastSequenceSeenByClient, clientEpoch)

            while (true) {
                if (clientEpoch != currentEpoch) {
                    lastSequenceSeenByClient = 0L
                    clientEpoch = currentEpoch
                    regClient(requestId, lastSequenceSeenByClient, clientEpoch)
                    logger.debug("client epoch changed for {} to {}", requestId, clientEpoch)
                } //epoch changed, resending everything

                //here we're implementing eager copy-on-write array, actually. The overhead is the array structure itself
                //we take slow consumers into account because we may accumulate a lot of the copies because of them
                val unfilteredCopy: List<CacheEntry<PayloadMessage>> = updateLogLock.read {
                    if (currentSequence() <= lastSequenceSeenByClient) listOf()
                    else updateLog.values.filter { it.sequence > lastSequenceSeenByClient } //this does a copy
                }
                val copy = if (filter == null) unfilteredCopy else unfilteredCopy.filter { filter.invoke(it.value) }
                if (copy.isNotEmpty()) {
                    val maxSequence = copy.last().sequence
                    if (lastSequenceSeenByClient == 0L) {
                        emit(adapter.setManifest(
                            manifest {
                                epoch = clientEpoch
                                maxKnownSequence = maxSequence
                            }
                        ))
                    }
                    val streamStarted = time()
                    var streamedSoFar: Long = 0L
                    logger.debug("going to send {} items", copy.size)
                    for (item in copy) {
                        emit(adapter.setVersion(
                            item.value,
                            version {
                                epoch = clientEpoch
                                sequence = item.sequence
                            }
                        ))
                        lastSequenceSeenByClient = item.sequence
                        streamedSoFar++
                        if (isSlowConsumer(streamedSoFar, streamStarted, time())) {
                            logger.info("the request ${requestId} is being processed too slow, dropping that for the client to reconnect")
                            deregClient(requestId)
                            return@flow //close the stream, since the consumer is slow and should reconnect
                        }
                        if (clientEpoch != currentEpoch) {
                            deregClient(requestId)
                            return@flow //close the stream, since epoch changed in the middle of an operation
                        }
                    }
                }
                if (filter != null && unfilteredCopy.isNotEmpty()) { //we set the last sequence according to the unfiltered copy in order not to filter every time in a case when nothing changes between the cycles
                    lastSequenceSeenByClient = unfilteredCopy.last().sequence
                }
                //here we have sent all the available data,
                // if stream is requested, we start over again after a delay
                // otherwise we close the connection
                if (request.streaming) {
                    logger.debug("Request {} - all {} updates sent", requestId, copy.size)
                    delay(1000)
                } else {
                    logger.debug("Request {} completed - {} updates sent", requestId, copy.size)
                    deregClient(requestId)
                    return@flow
                }
            }
        }
    }

}

data class CacheEntry<PayloadMessage>(
    val value: PayloadMessage, val sequence: Long
)