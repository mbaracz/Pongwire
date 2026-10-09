# jwebsockets

Tiny production-ready WebSocket server for Java, powered by Netty.

## Quick Start

Add the core dependency to your project:

```xml

<dependency>
    <groupId>pl.mbaracz</groupId>
    <artifactId>jwebsockets-core</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Start an echo server:

```java
var server = new WebSocketServer<String, Void>("/chat")
    .configure(config -> config
        .setMessageDecoder(PlainTextMessageDecoder.INSTANCE)
        .setMessageEncoder(PlainTextMessageEncoder.INSTANCE)
    )
    .onMessage((session, message) ->
        session.sendMessage("echo: " + message)
    )
    .listen(8080);
```

Connect to:

```text
ws://localhost:8080/chat
```

That's it. No framework, container or application server required.

## Why jwebsockets?

- Netty-based
- Typed messages and session context
- Built-in pub/sub
- Heartbeat and idle timeout
- Backpressure handling
- TLS and permessage-deflate
- Graceful shutdown
- RFC 6455, Autobahn-tested

## Conformance

jwebsockets is tested with the [Autobahn WebSocket Testsuite](https://github.com/crossbario/autobahn-testsuite)
in the [conformance workflow](.github/workflows/conformance.yml).

| Result        | Cases |
|---------------|------:|
| OK            |   386 |
| NON-STRICT    |     2 |
| INFORMATIONAL |     3 |
| FAILED        |     0 |

The supported test set excludes group 9 cases requiring messages above the default 1 MiB message limit, and cases
13.3-13.6 requiring `server_max_window_bits`, which the current compression implementation does not support.

## JSON and Pub/Sub

For JSON support, add the Jackson extension:

```xml

<dependency>
    <groupId>pl.mbaracz</groupId>
    <artifactId>jwebsockets-jackson</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## Authentication and session context

Configure the Jackson codec, authenticate the upgrade, and use the pub/sub features:

```java
var server = new WebSocketServer<ChatMessage, User>("/chat")
    .configure(config -> config
        .setMessageDecoder(new JsonMessageDecoder<>(ChatMessage.class))
        .setMessageEncoder(new JsonMessageEncoder<>())
    )
    .onUpgrade((request, response) ->
        UpgradeResult.accept(authenticate(request))
    );

server.onMessage((session, message) ->
    server.publish("chat", message).exceptionally(failure -> {
        log.warn("Could not publish the message", failure);
        return null;
    })
);
```

The stages returned by `publish`, `subscribe` and `unsubscribe` are the only place where topic broker failures are
reported, so handle them.

`UpgradeRequest` gives access to the request path, query parameters, cookies, and headers.

`UpgradeResult.accept(context)` accepts the upgrade and stores the context in the new session, while `UpgradeResult.reject()` ends the upgrade using the configured `UpgradeResponse`.

The same context is available later through `session.getContext()`.

The library keeps the reference as-is and does not manage the object's mutability.

## Threading model

Callbacks run on the session's Netty event loop by default, so they should not block.

Use `callbackExecutor` to move `onOpen`, `onMessage`, `onWritabilityChanged`, and `onClose` to another executor:

```java
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

server.configure(config -> config.setCallbackExecutor(executor));
```

- Callbacks for the same session are still executed one at a time and in order. Different sessions may run concurrently.
- `onError` always runs on the Netty event loop.
- `sendMessage` and `sendMessageAsync` are safe to call from other threads and concurrently. Concurrent sends do not have a defined relative order.
- The stages returned by `sendMessageAsync` and `close` usually complete on the event loop. Use the `*Async` methods of
  the stage, e.g. `thenRunAsync`, for continuations that block.
- The application owns the configured executor and is responsible for shutting it down.

## Production behavior

### Error handling

Failures are reported to `onError` and to the observer, with the session or `null` before the session was opened.

| Failure                                          | `onError` receives                        | Logged at |
|--------------------------------------------------|-------------------------------------------|-----------|
| Message the decoder cannot decode                | `MessageDecodeException` with the payload | DEBUG     |
| Exception thrown by a callback or the encoder    | The exception                             | ERROR     |
| Connection reset or WebSocket protocol violation | The exception                             | DEBUG     |
| Callback rejected by the callback executor       | `RejectedExecutionException`              | ERROR     |

By default (`closeOnException` is `false`) the session stays open after a failure and the failed message is dropped.
With `closeOnException` enabled, an open session is closed with `1007` when its message cannot be decoded and with
`1011` on other server-side failures. A broken connection or a protocol violation closes the connection without a
close frame of its own. Netty already answered the violation with `1002` or `1009`.

Independently of this setting, a session whose callback the executor rejects is closed with `1013`, an exception thrown
by `onUpgrade` is answered with `500`, and a send to a session that is already closed only fails the stage returned by
`sendMessageAsync`.

### Heartbeat and idle timeout

Heartbeat is disabled by default. When enabled, inbound data delays the next ping, but only a pong received within
`heartbeatTimeout` (10 seconds by default) answers a sent ping.

`idleTimeout` is separate and resets only on successfully decoded text or binary messages. Control frames do not reset
it.

### Backpressure

| Policy             | Behavior                                                                                  |
|--------------------|-------------------------------------------------------------------------------------------|
| `BUFFER` (default) | Keeps accepting sends into Netty's outbound buffer while the connection is unwritable.    |
| `REJECT_NEW`       | Fails new sends with `BackpressureException` until the connection becomes writable again. |

`unwritableTimeout` can close connections that stay unwritable for too long. It is disabled by default.

### Limits

Upgrade request headers are limited to 8 KiB by default. Frames and complete reassembled messages are limited to 1 MiB.

Exceeding a WebSocket size limit closes the connection with code `1009`. `maxFrameSize` must not exceed
`maxMessageSize`.

### Shutdown

`stop()` stops accepting new connections, sends active sessions a `1001` (Going Away) close frame, waits up to
`closeTimeout` (5 seconds by default), then force-closes remaining connections and shuts down the event loops.
`onClose` may still `broadcast` while the server is stopping. With a `callbackExecutor`, `onClose` callbacks can run
after `stop()` returns, so shut the executor down gracefully (`shutdown()` and `awaitTermination()`) after `stop()`.
If the server channel closes unexpectedly, `isRunning()` returns `false`, but `stop()` must still be called to release
the event loops.

## Code quality

`mvn verify` checks formatting with Spotless and enforces the existing JaCoCo coverage threshold. Run
`mvn spotless:apply` to fix import ordering, trailing whitespace, and missing final newlines locally.

## License

jwebsockets is licensed under the [Apache License 2.0](LICENSE).
