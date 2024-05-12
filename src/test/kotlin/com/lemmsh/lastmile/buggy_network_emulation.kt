package com.lemmsh.lastmile

import com.lemmsh.lastmile_client.ClientData.CountryCapitalPayload
import com.lemmsh.lastmile_client.CountryCapitalCacheGrpcKt
import com.lemmsh.lastmile_client.countryCapitalPayload
import io.grpc.*
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.ChannelOption
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.*

object SlowConsumerTest {

    val scope = CoroutineScope(Dispatchers.Default)
    fun largeArray(): List<Long> = (1 .. 1e6.toLong()).toList()
    val logger = LoggerFactory.getLogger(javaClass)

    @JvmStatic
    fun main(args: Array<String>) {
        val c = SlowConsumerTestContext(scope)
        c.startServer()

        runBlocking {
            delay(3000)
        }

        c.cacheServer.push("UK", LargeObject("London", largeArray()))
        c.cacheServer.push("USA", LargeObject("New York", largeArray()))
        c.cacheServer.declareInitialized()

        for (i in 1..100) {
            c.cacheServer.push("Country${i%10}", LargeObject(i.toString(), largeArray()))
        }
        c.startClient()

        runBlocking {
            delay(10000)
        }

        if (c.fastCacheClient.data() != null) {
            //happens when the client side has initialized (that is, got whole server state)
            logger.info("success in the fast cache")
        } else {
            logger.error("fail the fast cache")
        }

        runBlocking {
            delay(60000)
        }


        c.stopClient()
        c.stopServer()
    }
}

class SlowConsumerTestContext(val scope: CoroutineScope) {
    val cacheServer = LastMileServer(
        SlowCountryServerAdapter(),
        "the_cache",
        LastMileServerConfiguration(
            0.8,
            1.0,
        )
    )
    val grpcWrapper = CountryCacheServiceImpl(cacheServer)
    val name = "lastmile-test-${UUID.randomUUID()}"
    val server = run {
        val builder = NettyServerBuilder.forPort(8082)
            .directExecutor()
            .addService(grpcWrapper)
        builder.build()
    }
    val channel = run {
        val builder = NettyChannelBuilder.forAddress("localhost", 8082)
            .usePlaintext()
        builder.build()
    }
    val stub = CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineStub(channel)

    val adapter = SlowCountryClientAdapter(stub)
    val cacheClient = LastMileClient(
        adapter, scope, "slow_client", LastMileClientConfiguration(
            reconnectBackoffMillis = 1000,
            reconnectAttempts = 2,
            reconnectBackoffMillisNoData = 1,
            streaming = true
        )
    )

    val channelFast = run {
        val builder = NettyChannelBuilder.forAddress("localhost", 8082)
            .usePlaintext()
        builder.build()
    }
    val stubFast = CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineStub(channelFast)
    val adapterFast = CountryClientAdapter(stubFast)
    val fastCacheClient = LastMileClient(
        adapterFast, scope, "fast_client", LastMileClientConfiguration(
            reconnectBackoffMillis = 1000,
            reconnectAttempts = 2,
            reconnectBackoffMillisNoData = 1,
            streaming = true
        )
    )

    fun startServer(): Unit {
        server.start()
    }

    var j: Job? = null
    var jj: Job? = null
    fun startClient(): Unit {
        j = cacheClient.connect()
        jj = fastCacheClient.connect()
    }

    fun stopServer() {
        server.shutdown()
        server.awaitTermination()
    }

    fun stopClient() {
        runBlocking {
            j?.cancelAndJoin()
            jj?.cancelAndJoin()
        }
    }

}
data class LargeObject(val capital: String, val array: List<Long>)
class SlowCountryClientAdapter(val stub: CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineStub):
    ClientPayloadAdapter<CountryCapitalPayload, String, LargeObject> {
    override fun getManifest(payload: CountryCapitalPayload): Lastmile.Manifest? {
        return if (payload.hasManifest()) payload.manifest else null
    }

    override fun getVersion(payload: CountryCapitalPayload): Lastmile.Version? {
        return if (payload.hasVersion()) payload.version else null
    }

    override fun getKey(payload: CountryCapitalPayload): String {
        return payload.country
    }

    override fun getValue(payload: CountryCapitalPayload): LargeObject {
        return LargeObject(payload.capital, payload.hugePayloadList)
    }

    override fun connect(request: Lastmile.StreamRequest): Flow<CountryCapitalPayload> {
        return stub.dataStream(request).map {
            delay(2000)
            it
        }
    }
}
class SlowCountryServerAdapter: ServerPayloadAdapter<CountryCapitalPayload, String, LargeObject> {
    override fun payloadFromManifest(manifest: Lastmile.Manifest): CountryCapitalPayload {
        return countryCapitalPayload {
            this.manifest = manifest
        }
    }

    override fun payloadFromUpdate(key: String, value: LargeObject, version: Lastmile.Version): CountryCapitalPayload {
        return countryCapitalPayload {
            country = key
            capital = value.capital
            hugePayload.addAll(value.array)
            this.version = version
        }
    }

}

