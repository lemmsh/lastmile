package com.lemmsh.lastmile

import com.google.protobuf.GeneratedMessageV3
import com.lemmsh.lastmile.Lastmile.Manifest
import com.lemmsh.lastmile.Lastmile.StreamRequest
import com.lemmsh.lastmile.Lastmile.Version
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


interface ClientPayloadAdapter<PayloadMessage: GeneratedMessageV3, Key: Any> {
    fun getManifest(payload: PayloadMessage): Manifest?
    fun getVersion(payload: PayloadMessage): Version?
    fun getKey(payload: PayloadMessage): Key
    fun connect(request: Lastmile.StreamRequest): Flow<PayloadMessage>
}
data class ClientCacheEntry<Value: Any>(
    val value: Value, val sequence: Long, val epoch: Long
)

data class LastMileClientConfiguration(
    val reconnectBackoffMillis: Long = 1000,
    val reconnectAttempts: Int = 5,
    val reconnectBackoffMillisNoData: Long = 10000,
    val streaming: Boolean = false
)

/**
 * there can be two strategies on the disconnects / epoch change
 * 1. allow reading possibly stale data, until the cache initializes with a new epoch
 * 2. prohibit such reading
 *
 * here we choose the first one, as we prioritize availability
 */
class LastMileClient<PayloadMessage: GeneratedMessageV3, Key: Any> (
    private val adapter: ClientPayloadAdapter<PayloadMessage, Key>,
    private val coroutineScope: CoroutineScope,
    val name: String,
    val configuration: LastMileClientConfiguration
) {

    private val logger = LoggerFactory.getLogger(javaClass.canonicalName + ".$name")
    private var currentEpoch: Long? = null
    private var maxSeenSequence: Long? = null
    private var initializationWatermark: Long? = null
    @Volatile private var initialized: Boolean = false
    private val nearCache: ConcurrentMap<Key, ClientCacheEntry<PayloadMessage>> = ConcurrentHashMap()
    private val metadataLock: ReentrantLock = ReentrantLock() //it may be not necessary...

    fun data(): ClientCacheData<Key, PayloadMessage>? {
        if (!initialized) return null
        else return ClientCacheData(nearCache)
    }

    fun connect(): Job {
        val job = coroutineScope.launch {
            doConnectAttempt(configuration.reconnectBackoffMillis, configuration.reconnectAttempts)
        }
        return job
    }

    private tailrec suspend fun doConnectAttempt(backoff: Long, remainingAttempts: Int) {
        var newBackoff: Long
        var newRemainingAttempts: Int
        try {
            val flow = adapter.connect(request = createRequest())
            listen(flow)
            newBackoff = configuration.reconnectBackoffMillis
            newRemainingAttempts = configuration.reconnectAttempts
            logger.info("Reached end of stream from the server; there are ${nearCache.size} items in the near cache. Reconnecting after ${configuration.reconnectBackoffMillisNoData} millis")
            if (initialized) {
                gc()
            }
            delay(configuration.reconnectBackoffMillisNoData)
        } catch (e: Exception) {
            newBackoff = backoff * 2
            newRemainingAttempts = remainingAttempts - 1
            logger.error("Got an error, will reconnect in ${newBackoff} millis, still ${newRemainingAttempts} left", e)
            if (newRemainingAttempts <= 0) throw e
            delay(newBackoff)
        }
        doConnectAttempt(newBackoff, remainingAttempts = newRemainingAttempts)
    }

    private fun createRequest(): StreamRequest = metadataLock.withLock {
        return streamRequest {
            maxKnownVersion = version {
                epoch = currentEpoch?:0
                sequence = maxSeenSequence?:0
            }
            streaming = configuration.streaming
        }
    }

    suspend fun listen(updates: Flow<PayloadMessage>) {
        updates.collect {
            listen(it)
        }
    }

    private fun listen(payload: PayloadMessage) {
        val manifest = adapter.getManifest(payload)
        val version = adapter.getVersion(payload)
        when {
            manifest != null -> processManifest(manifest)
            version != null -> update(payload, version)
        }
    }

    private fun processManifest(manifest: Manifest) = metadataLock.withLock {
        currentEpoch = manifest.epoch
        initializationWatermark = manifest.maxKnownSequence
    }

    private fun update(payload: PayloadMessage, version: Version) = metadataLock.withLock {
        if (version.epoch != currentEpoch) {
            throw CacheClientException("Unexpected epoch change in the message of version ${version}. The epoch used to be $currentEpoch.")
        }
        if (initializationWatermark == null) {
            throw CacheClientException("Unexpected data payload before manifest. Need to reconnect")
        }
        this.nearCache.put(adapter.getKey(payload), ClientCacheEntry(payload,
            version.sequence, version.epoch))
        maxSeenSequence = version.sequence
        if (maxSeenSequence!! >= initializationWatermark!! && !initialized) {
            gc()
            initialized = true
            logger.info("The cache is fully initialized")
        }
    }

    private fun gc() = metadataLock.withLock {
        for (key in nearCache.keys.toList()) {
            if (nearCache[key]?.epoch != currentEpoch) nearCache.remove(key)
        }
    }

}

class CacheClientException(message: String): RuntimeException(message)

class ClientCacheData<Key: Any, Value: Any>(private val data: ConcurrentMap<Key, ClientCacheEntry<Value>>) {
    fun get(key: Key): Value? {
        return data[key]?.value
    }
}