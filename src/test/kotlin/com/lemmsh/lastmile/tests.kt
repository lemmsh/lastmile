package com.lemmsh.lastmile

import com.lemmsh.lastmile_client.ClientData.CountryCapitalPayload
import com.lemmsh.lastmile_client.CountryCapitalCacheGrpcKt
import io.grpc.*
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.ChannelOption
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class LastMileTests {

    val scope = CoroutineScope(Dispatchers.Default)

    @Test
    fun testNormalOperation() {
        val c = TestContext(scope)
        c.startServer()
        c.startClient()

        c.cacheServer.push("UK", "London")
        c.cacheServer.push("USA", "New York")
        c.cacheServer.declareInitialized()
        runBlocking {
            delay(1000)
        }
        assertEquals("London", c.cacheClient.data()?.get("UK"))
        assertEquals("New York", c.cacheClient.data()?.get("USA"))

        c.cacheServer.push("USA", "Washington")
        runBlocking {
            delay(1000)
        }
        assertEquals("Washington", c.cacheClient.data()?.get("USA"))


        c.stopClient()
        c.stopServer()
    }


    @Test
    fun testGC() {
        val c = TestContext(scope)
        c.startServer()
        c.startClient()

        c.cacheServer.push("UK", "London")
        c.cacheServer.push("USA", "New York")
        c.cacheServer.declareInitialized()
        runBlocking {
            delay(1000)
        }
        assertEquals("London", c.cacheClient.data()?.get("UK"))
        assertEquals("New York", c.cacheClient.data()?.get("USA"))

        c.cacheServer.push("USA", "Washington")
        c.cacheServer.push("Germany", "Berlin1")
        c.cacheServer.push("Germany", "Berlin2")
        c.cacheServer.push("Germany", "Berlin3")
        c.cacheServer.push("Germany", "Berlin4")
        c.cacheServer.push("Germany", "Berlin5")
        c.cacheServer.push("Germany", "Berlin6")
        c.cacheServer.push("Germany", "Berlin")
        runBlocking {
            delay(1000)
        }
        assertEquals(10, c.cacheServer.size())
        assertEquals("Berlin", c.cacheClient.data()?.get("Germany"))
        c.cacheServer.gc()
        assertEquals(3, c.cacheServer.size())

        assertEquals("London", c.cacheClient.data()?.get("UK"))
        assertEquals("Berlin", c.cacheClient.data()?.get("Germany"))
        assertEquals("Washington", c.cacheClient.data()?.get("USA"))

        c.stopClient()
        c.stopServer()
    }

    @Test
    fun testServerRestart() {
        val c = RestartsTestContext(scope)
        c.restartServer()
        c.startClient()

        c.cacheServer!!.push("UK", "London")
        c.cacheServer!!.push("USA", "Washington")
        c.cacheServer!!.declareInitialized()
        runBlocking {
            delay(1000)
        }
        assertEquals("London", c.cacheClient.data()?.get("UK"))
        assertEquals("Washington", c.cacheClient.data()?.get("USA"))

        //here the server went down, and then up, and it's not initialized. So the expected behaviour is
        //that we'll have the last known state still available on the client
        c.restartServer()
        runBlocking {
            delay(1000)
        }
        assertEquals("London", c.cacheClient.data()?.get("UK"))

        //now, the server side cache is getting initialized with new data
        c.cacheServer!!.push("France", "Paris")
        c.cacheServer!!.declareInitialized()
        runBlocking {
            delay(1000)
        }
        assertEquals("Paris", c.cacheClient.data()?.get("France"))
        assertEquals(null, c.cacheClient.data()?.get("UK"))
        assertEquals(null, c.cacheClient.data()?.get("USA"))

        c.stopClient()
        c.stopServer()
    }

    @Test
    fun testStreamNormalOperation() {
        val c = TestContext(scope, streaming = true)
        c.startServer()
        c.startClient()
        runBlocking {
            delay(1000)
        }
        val connectionId = c.cacheServer.connections().keys.toList().first()
        c.cacheServer.push("UK", "London")
        c.cacheServer.push("USA", "New York")
        c.cacheServer.declareInitialized()
        runBlocking {
            delay(1000)
        }
        assertEquals("London", c.cacheClient.data()?.get("UK"))
        assertEquals("New York", c.cacheClient.data()?.get("USA"))

        assertEquals(connectionId, c.cacheServer.connections().keys.toList().first())

        c.cacheServer.push("USA", "Washington")
        runBlocking {
            delay(1000)
        }
        assertEquals("Washington", c.cacheClient.data()?.get("USA"))

        assertEquals(connectionId, c.cacheServer.connections().keys.toList().first())

        c.stopClient()
        c.stopServer()
    }

    @Test
    fun testStreamEpochChange() {
        val c = TestContext(scope, streaming = true)
        c.startServer()
        c.startClient()
        runBlocking {
            delay(1000)
        }
        val connectionId = c.cacheServer.connections().keys.toList().first()
        c.cacheServer.push("UK", "London")
        c.cacheServer.push("USA", "New York")
        c.cacheServer.declareInitialized()
        runBlocking {
            delay(1000)
        }
        assertEquals("London", c.cacheClient.data()?.get("UK"))
        assertEquals("New York", c.cacheClient.data()?.get("USA"))

        assertEquals(connectionId, c.cacheServer.connections().keys.toList().first())

        c.cacheServer.push("USA", "Washington")

        c.cacheServer.invalidateClients()
        runBlocking {
            delay(2000)
        }
        assertEquals("London", c.cacheClient.data()?.get("UK"))
        assertEquals("Washington", c.cacheClient.data()?.get("USA"))

        assertEquals(connectionId, c.cacheServer.connections().keys.toList().first())

        c.stopClient()
        c.stopServer()
    }

}

class TestContext(val scope: CoroutineScope,
                  val minRate: Double = 100.0,
                  val gcJustification: Double = 0.8,
                  val reconnectBackoff: Long = 100,
                  val reconnectOnNoNewDataBackoff: Long = 100,
                  val reconnectAttempts: Int = 2,
                  val streaming: Boolean = false,
                  val clientInterceptor: ClientInterceptor? = null,
                  val nettyBufferSizes: Int? = null,
                  val outboundObjectBufferSize: Int = 100,
    ) {
    val cacheServer = LastMileServer(
        CountryServerAdapter(),
        "the_cache",
        LastMileServerConfiguration(
            gcJustification,
            minRate,
        )
    )
    val grpcWrapper = CountryCacheServiceImpl(cacheServer)
    val name = "lastmile-test-${UUID.randomUUID()}"
    val server = run {
        val builder = NettyServerBuilder.forPort(8082)
        .directExecutor() 
        .addService(grpcWrapper) 
        if (nettyBufferSizes != null) {
            builder.withChildOption(ChannelOption.SO_SNDBUF, nettyBufferSizes)
            builder.withChildOption(ChannelOption.SO_RCVBUF, nettyBufferSizes)
        }
        builder.build()
    }
    val channel = run {
        val builder = NettyChannelBuilder.forAddress("localhost", 8082)
            .usePlaintext()
        if (clientInterceptor != null) {
            builder.intercept(clientInterceptor)
        }
        if (nettyBufferSizes != null) {
            builder.withOption(ChannelOption.SO_SNDBUF, nettyBufferSizes)
            builder.withOption(ChannelOption.SO_RCVBUF, nettyBufferSizes)
        }
        builder.build()
    }
    val stub = CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineStub(channel)

    val adapter = CountryClientAdapter(stub)
    val cacheClient = LastMileClient(
        adapter, scope, "client", LastMileClientConfiguration(
            reconnectBackoffMillis = reconnectBackoff,
            reconnectAttempts = reconnectAttempts,
            reconnectBackoffMillisNoData = reconnectOnNoNewDataBackoff,
            streaming = streaming
        )
    )

    fun startServer(): Unit {
        server.start()
    }

    var j: Job? = null
    fun startClient(): Unit {
        j = cacheClient.connect()
    }

    fun stopServer() {
        server.shutdown()
        server.awaitTermination()
    }

    fun stopClient() {
        runBlocking {
            j?.cancelAndJoin()
        }
    }

}

class RestartsTestContext(val scope: CoroutineScope,
                  val minRate: Double = 100.0,
                  val gcJustification: Double = 0.8,
                  val reconnectBackoff: Long = 100,
                  val reconnectOnNoNewDataBackoff: Long = 100,
                  val reconnectAttempts: Int = 10,
                  val outboundObjectBufferSize: Int = 100,
) {
    val name = "lastmile-test-${UUID.randomUUID()}"

    var cacheServer: LastMileServer<CountryCapitalPayload, String, String>? = null
    var grpcWrapper: CountryCacheServiceImpl? = null
    var server: Server? = null

    val channel = ManagedChannelBuilder.forAddress("localhost", 8082)
        .usePlaintext()
        .build()
    val stub = CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineStub(channel)

    val adapter = CountryClientAdapter(stub)
    val cacheClient = LastMileClient(
        adapter, scope, "client", LastMileClientConfiguration(
            reconnectBackoffMillis = reconnectBackoff,
            reconnectAttempts = reconnectAttempts,
            reconnectBackoffMillisNoData = reconnectOnNoNewDataBackoff
        )
    )


    var j: Job? = null
    fun startClient(): Unit {
        j = cacheClient.connect()
    }

    fun stopServer() {
        server?.shutdown()
        server?.awaitTermination()
    }

    fun restartServer() {
        stopServer()
        cacheServer = LastMileServer(
            CountryServerAdapter(),
            "the_cache",
            LastMileServerConfiguration(
                minRate,
                gcJustification,
            )
        )
        grpcWrapper = CountryCacheServiceImpl(cacheServer!!)
        server = ServerBuilder.forPort(8082)
            .directExecutor() // Execute calls directly in the calling thread
            .addService(grpcWrapper) // Add our service implementation
            .build()
        server!!.start()
    }

    fun stopClient() {
        runBlocking {
            j?.cancelAndJoin()
        }
    }

}



