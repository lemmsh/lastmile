
This is an implementation of a Last Mile streaming cache.

# The Problem

Imagine a scenario where you need to provide a client application with an API that maintains a near cache on their system, without having control over the client. The cache must meet the following requirements:

1. Have authentication, authorization, and encryption.
2. Support cross-platform clients.
3. Ensure eventual consistency.
4. Allow control over compatibility.
5. Be resilient to slowly reading clients.
6. Be resilient to server crashes.

# The LastMile Cache

This library addresses requirements 3, 4, 5, and 6 directly, facilitates requirement 2, and leverages standard tools to implement requirement 1.

## Usage

The library offers a server-side cache implementation through `LastMileServer.kt`, written in Kotlin and utilizing `ServerPayloadAdapter` for parameterization. To integrate this cache, one should establish a gRPC context and incorporate `LastMileServer.kt`. For practical examples, refer to the test implementations in `local.kt` and `tests.kt`. The `LastMileClient`, a reference client implementation, is designed for JVM environments.

## Design

Each cache instance defines its protocol using proto definitions, which include elements from the core LastMile proto. This design ensures API compatibility is transparent.

`LastMileServer` supports an event log where updates are sequentially stored along with their sequence numbers. The sequence number contains an epoch which represents a session. The epoch changes when the client re-connects to a different server instance or when the server wants to force the cache invalidation.
Clients connect and receive a stream of these updates in chronological order, each tagged with an epoch to manage data consistency across changes. Clients use the sequence numver to figure out that they've downloaded the cache in full. 

To prevent concurrency issues, each client receives a copy of the event log. To optimize memory usage, especially during frequent updates, the server employs a garbage collection method (`gc()`) that retains only the most recent updates for each key. Slow consumers, who might otherwise exhaust server memory with their log copies, are required to reconnect if their download speed falls below a certain threshold.

Updates are only emitted from the cache once it is fully populated, as declared by the server-side code. This prevents the distribution of incomplete data. Clients maintain their data across reconnections unless they successfully re-fetch the entire data set from the server.
