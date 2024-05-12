package com.lemmsh.lastmile

import com.google.protobuf.GeneratedMessageV3
import com.lemmsh.lastmile.Lastmile.Manifest
import com.lemmsh.lastmile.Lastmile.Version
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.collections.ArrayList
import kotlin.concurrent.read
import kotlin.concurrent.write

interface ServerPayloadAdapter<PayloadMessage: GeneratedMessageV3, Key: Any, Value: Any> {
    fun payloadFromManifest(manifest: Manifest): PayloadMessage
    fun payloadFromUpdate(key: Key, value: Value, version: Version): PayloadMessage
}

class ClientStat(val requestReceived: Long,
                 var sequenceSent: Long = 0,
                 var epoch: Long = 0)

data class LastMileServerConfiguration(
    val gcWhenThisMuchUniqKeysAmongAllRecords: Double,
    val slowestAllowedDownloadRateKeysPerSecond: Double,
)

class LastMileServer<PayloadMessage: GeneratedMessageV3, Key: Any, Value: Any>(
    private val adapter: ServerPayloadAdapter<PayloadMessage, Key, Value>,
    val name: String,
    val configuration: LastMileServerConfiguration
) {
    @Volatile private var currentEpoch: Long = System.currentTimeMillis()
    @Volatile private var initialized: Boolean = false

    private var updateLog: ArrayList<CacheEntry<Key, Value>> = ArrayList()
    private val updateLogLock: ReentrantReadWriteLock = ReentrantReadWriteLock()

    private val clientRegistry: ConcurrentMap<UUID, ClientStat> = ConcurrentHashMap()
    private val logger = LoggerFactory.getLogger(javaClass.canonicalName + ".$name")

    fun push(key: Key, value: Value) = updateLogLock.write {
        val currentSequence = currentSequence()
        updateLog.add(CacheEntry(key, value, currentSequence + 1))
    }
    fun pushAll(data: Iterable<Pair<Key, Value>>) = updateLogLock.write {
        var currentSequence = currentSequence()
        for (d in data) {
            updateLog.add(CacheEntry(d.first, d.second, currentSequence + 1))
            currentSequence++
        }
    }
    fun declareInitialized() {
        initialized = true
        logger.info("The cache is initialized")
    }
    fun invalidateClients() {
        currentEpoch = System.currentTimeMillis()
        logger.info("Cache invalidation requested. The clients will receive an invalidation command on the next update, the new epoch is $currentEpoch")
    }
    fun size(): Int = updateLogLock.read {
        return updateLog.size
    }

    fun gc() {
        val gcNeeded = updateLogLock.read {
            val v = updateLog.groupBy { it.key }.mapValues { it.value.size }.values
            val distinct = v.size
            val total = v.sum()
            distinct.toDouble()/total.toDouble() < configuration.gcWhenThisMuchUniqKeysAmongAllRecords
        }
        if (gcNeeded) {
            updateLogLock.write {
                logger.info("starting GC")
                val size = updateLog.size
                val knownKeys = mutableSetOf<Key>()
                val markedForRemoval = mutableSetOf<Int>()
                for (i in size-1 downTo 0) {
                    val key = updateLog[i].key
                    if (knownKeys.contains(key)) markedForRemoval.add(i)
                    knownKeys.add(key)
                }
                val newUpdateLog: ArrayList<CacheEntry<Key, Value>> = ArrayList(((updateLog.size - markedForRemoval.size)/configuration.gcWhenThisMuchUniqKeysAmongAllRecords).toInt())
                for (i in 0 .. size-1) {
                    if (!markedForRemoval.contains(i)) {
                        newUpdateLog.add(updateLog[i])
                    }
                }
                updateLog = newUpdateLog
                logger.info("GC complete. The log size was ${size}, it is now ${updateLog.size}")
            }
        } else {
            logger.debug("GC is not needed")
        }
    }
    fun connections() = clientRegistry.toMap()

    private fun currentSequence() = updateLogLock.read { updateLog.lastOrNull()?.sequence?:0L }

    private fun regClient(requestId: UUID, lastSeqSeenByClient: Long = 0, clientEpoch: Long = 0) {
        val clientStat = clientRegistry[requestId] ?: ClientStat(requestReceived = System.currentTimeMillis())
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
                   filter: ((Key, Value) -> Boolean)?
    ): Flow<PayloadMessage> {
        logger.debug(
            "New request {} of epoch {}, starting from the sequence {}",
            requestId,
            request.maxKnownVersionOrNull?.epoch,
            request.maxKnownVersionOrNull?.sequence
        )
        regClient(requestId)

        return flow {
            if (!initialized) {
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
                val unfilteredCopy: List<CacheEntry<Key, Value>> = updateLogLock.read {
                    if ((updateLog.lastOrNull()?.sequence ?: 0L) <= lastSequenceSeenByClient) listOf<CacheEntry<Key, Value>>()
                    else updateLog.filter { it.sequence > lastSequenceSeenByClient } //this does a copy
                }
                val copy = if (filter == null) unfilteredCopy else unfilteredCopy.filter { filter.invoke(it.key, it.value) }
                if (copy.isNotEmpty()) {
                    val maxSequence = copy.last().sequence
                    if (lastSequenceSeenByClient == 0L) {
                        emit(adapter.payloadFromManifest(
                            manifest {
                                epoch = clientEpoch
                                maxKnownSequence = maxSequence
                            }
                        ))
                    }
                    val streamStarted = System.currentTimeMillis()
                    var streamedSoFar: Long = 0L
                    logger.debug("going to send {} items", copy.size)
                    for (item in copy) {
                        emit(adapter.payloadFromUpdate(
                            item.key,
                            item.value,
                            version {
                                epoch = clientEpoch
                                sequence = item.sequence
                            }
                        ))
                        lastSequenceSeenByClient = item.sequence
                        streamedSoFar++
                        if (isSlowConsumer(streamedSoFar, streamStarted, System.currentTimeMillis())) {
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

data class CacheEntry<Key: Any, Value: Any>(
    val key: Key, val value: Value, val sequence: Long
)