package com.lemmsh.lastmile

import com.lemmsh.lastmile_client.ClientData.CountryCapitalPayload
import com.lemmsh.lastmile_client.CountryCapitalCacheGrpcKt
import com.lemmsh.lastmile_client.countryCapitalPayload
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import java.util.*

object LocalRun {

    @JvmStatic
    fun main(args: Array<String>) {
        val scope = CoroutineScope(Dispatchers.Default)

        val theCache = LastMileServer<CountryCapitalPayload, String>(
            CountryServerAdapter(),
            "the_cache",
            LastMileServerConfiguration(
                10.0
            )
        )
        theCache.push(countryCapitalPayload {
            country = "a"
            capital = "1"
        })
        theCache.push(countryCapitalPayload {
            country = "b"
            capital = "2"
        })
        theCache.push(countryCapitalPayload {
            country = "c"
            capital = "42"
        })
        theCache.declareInitialized()


        val grpcWrapper = CountryCacheServiceImpl(theCache)
        val server = InProcessServerBuilder.forName("lastmile-test")
            .directExecutor() // Execute calls directly in the calling thread
            .addService(grpcWrapper) // Add our service implementation
            .build()
        server.start()

        val channel = InProcessChannelBuilder.forName("lastmile-test")
            .directExecutor()
            .build()
        val stub = CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineStub(channel)

        val adapter = CountryClientAdapter(stub)
        val client = LastMileClient(
            adapter, scope, "client", LastMileClientConfiguration(
                reconnectBackoffMillis = 1000, reconnectAttempts = 3
            )
        )

        val j = client.connect()

        runBlocking { j.join() }

        server.shutdown()
        server.awaitTermination()

    }

}

class CountryClientAdapter(val stub: CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineStub):
    ClientPayloadAdapter<CountryCapitalPayload, String> {
    override fun getManifest(payload: CountryCapitalPayload): Lastmile.Manifest? {
        return if (payload.hasManifest()) payload.manifest else null
    }

    override fun getVersion(payload: CountryCapitalPayload): Lastmile.Version? {
        return if (payload.hasVersion()) payload.version else null
    }

    override fun getKey(payload: CountryCapitalPayload): String {
        return payload.country
    }

    override fun connect(request: Lastmile.StreamRequest): Flow<CountryCapitalPayload> {
        return stub.dataStream(request)
    }
}

class CountryServerAdapter: ServerPayloadAdapter<CountryCapitalPayload, String> {
    override fun setManifest(manifest: Lastmile.Manifest): CountryCapitalPayload {
        return countryCapitalPayload {
            this.manifest = manifest
        }
    }

    override fun key(value: CountryCapitalPayload): String {
        return value.country
    }

    override fun setVersion(value: CountryCapitalPayload, version: Lastmile.Version): CountryCapitalPayload {
        return value.toBuilder().setVersion(version).build()
    }

}

class CountryCacheServiceImpl(val cacheImpl: LastMileServer<CountryCapitalPayload, *>):
    CountryCapitalCacheGrpcKt.CountryCapitalCacheCoroutineImplBase() {

    override fun dataStream(request: Lastmile.StreamRequest): Flow<CountryCapitalPayload> {
        return cacheImpl.dataStream(request, UUID.randomUUID(), null)
    }
}