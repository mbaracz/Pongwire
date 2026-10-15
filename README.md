# Pongwire

Tiny production-ready WebSocket server for Java, powered by Netty.

## Quick Start

Add the core dependency to your project:

```xml

<dependency>
    <groupId>pl.mbaracz</groupId>
    <artifactId>pongwire-core</artifactId>
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

## Why Pongwire?

- Netty-based
- Typed messages and session context
- Built-in pub/sub
- Heartbeat and idle timeout
- Backpressure handling
- TLS and permessage-deflate
- Graceful shutdown
- RFC 6455, Autobahn-tested

## Conformance

Pongwire is tested with the [Autobahn WebSocket Testsuite](https://github.com/crossbario/autobahn-testsuite)
in the [conformance workflow](.github/workflows/conformance.yml).

| Result        | Cases |
|---------------|------:|
| OK            |   422 |
| NON-STRICT    |     2 |
| INFORMATIONAL |     3 |
| FAILED        |     0 |

The supported test set excludes group 9 cases requiring messages above the default 1 MiB message limit, and cases
13.3 and 13.5 requiring a `server_max_window_bits` below 15, which the JDK's zlib does not support.
A compressed message whose first fragment inflates to nothing, which RFC 7692 allows, is rejected with `1002` by
Netty's decompressor.

## JSON and Pub/Sub

For JSON support, add the Jackson extension:

```xml

<dependency>
    <groupId>pl.mbaracz</groupId>
    <artifactId>pongwire-jackson</artifactId>
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

The JSON decoder accepts a single UTF-8 encoded value and rejects `null`, trailing content and other encodings. Without
a mapper, the codecs use Jackson's defaults with java.time support. A mapper passed to the decoder handles input from
clients, so it must not enable polymorphic typing, e.g. `activateDefaultTyping`, which lets a client choose the classes
the server instantiates.

The stages returned by `publish`, `subscribe`, `unsubscribe` and `unsubscribeAllTopics` are where the failures of
these broker operations are reported, so handle them. The unsubscribe the server runs itself when the last subscriber
of a topic disconnects logs its failure at WARN. A published message is encoded when the broker delivers it, which may
be after the stage completes, so do not change it until every delivery is done.

`UpgradeRequest` gives access to the request path, query parameters, cookies, and headers.

`UpgradeResult.accept(context)` accepts the upgrade and stores the context in the new session, while `UpgradeResult.reject()` ends the upgrade using the configured `UpgradeResponse`.

The same context is available later through `session.getContext()`.

The library keeps the reference as-is and does not manage the object's mutability.

### Allowed origins

Origins are not checked by default.
A server that authenticates the upgrade with a cookie must restrict them, because a browser attaches the cookie also
when another web page opens the connection (Cross-Site WebSocket Hijacking):

```java
server.configure(config -> config
    .setAllowedOrigins("https://app.example.com")
    .setAllowedOriginPattern(Pattern.compile("https://[a-z0-9-]+\\.preview\\.example\\.com"))
);
```

An origin is accepted if it is in the list or matches the pattern, others get `403`.
Browsers always send `Origin`, native and server clients usually do not, so a request without it is rejected once
origins are checked, unless `setAllowMissingOrigin(true)` allows it.

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
- The stages returned by `publish`, `subscribe` and `unsubscribe` complete on whichever thread completes the broker
  operation, which can be an event loop, a thread of the broker, the calling thread for an operation that needs no
  broker call, or the timeout thread of `CompletableFuture` after the broker operation timeout.
  Calling `stop()` from such a continuation fails on an event loop, so use `thenRunAsync` for it as well.
- The message decoder runs on the event loop, and so does the encoder for `sendMessage` and `sendMessageAsync`, also
  with a `callbackExecutor`, so large messages with an expensive codec delay the other sessions of that loop.
  `broadcast` encodes on its calling thread and a topic message is encoded on the thread the broker delivers it on,
  so the encoder must support concurrent calls.
- The application owns the configured executor and is responsible for shutting it down.

## Production behavior

### Error handling

Failures are reported to `onError` and to the observer, with the session or `null` before the session was opened.
Protocol violations Netty's validators answer themselves, e.g. invalid UTF-8 with `1007`, a reserved bit or a message
too large with `1002` or `1009`, only reach `onClose` with that status.

| Failure                                          | `onError` receives                        | Logged at |
|--------------------------------------------------|-------------------------------------------|-----------|
| Message the decoder cannot decode                | `MessageDecodeException` with the payload | DEBUG     |
| Exception thrown by a callback or the encoder    | The exception                             | ERROR     |
| Connection reset or WebSocket protocol violation | The exception                             | DEBUG     |
| Callback rejected by the callback executor       | `RejectedExecutionException`              | ERROR     |

By default (`closeOnException` is `false`) the session stays open after a failure of the application, i.e. of a
callback or of the codec, and the failed message is dropped.
With `closeOnException` enabled, an open session is closed with `1007` when its message cannot be decoded and with
`1011` on other server-side failures. A broken connection closes without a close frame, and a protocol violation
closes with its own status whatever the setting, e.g. `1002` for a malformed frame, `1007` for invalid UTF-8 or
corrupted compressed data and `1009` for a message too large.

Independently of this setting, a session whose callback the executor rejects is closed with `1013` and the callbacks
queued behind the rejected one are dropped, except `onClose`, which still runs. An exception thrown by `onUpgrade` is
answered with `500`, and a send to a session that is already closed only fails the stage returned by
`sendMessageAsync`.

### Heartbeat and idle timeout

Heartbeat is disabled by default. When enabled, inbound data delays the next ping, but only a pong received within
`heartbeatTimeout` (10 seconds by default) answers a sent ping.

`idleTimeout` is separate and resets only on successfully decoded text or binary messages. Control frames do not reset
it.

Neither timer is randomized.
Sessions that connected together, e.g. after a restart, reach their idle timeout together, so clients should
reconnect with a random delay.

### Backpressure

| Policy             | Behavior                                                                                  |
|--------------------|-------------------------------------------------------------------------------------------|
| `BUFFER` (default) | Keeps accepting sends into Netty's outbound buffer while the connection is unwritable.    |
| `REJECT_NEW`       | Fails new sends with `BackpressureException` until the connection becomes writable again. |

`unwritableTimeout` closes connections that stay unwritable for too long. It is disabled by default. It cleans up
stalled connections, it does not cap their buffers: every send accepted under `BUFFER` is held until the timeout fires,
so enough broadcasts within the timeout can still exhaust memory.

With the defaults, `BUFFER` and no `unwritableTimeout`, nothing limits the outbound buffer of a client that stops
reading. Every broadcast or topic message adds to it until the server runs out of memory, so a public server should set
`unwritableTimeout` and keep the rate of its broadcasts in check, or use `REJECT_NEW`.

A message larger than the high write buffer watermark (64 KiB by default) makes the connection unwritable until the
transport took most of it, and Netty counts the encoded frame with its bookkeeping, not the payload alone. Under
`REJECT_NEW` the sends executed meanwhile fail although the client reads fine, so set the high watermark with
`setWriteBufferWaterMark` above the largest message the server sends plus the burst it sends at once.

### Limits

Upgrade request headers are limited to 8 KiB by default, and so are the request line and a request body. Frames and complete reassembled messages are limited to 1 MiB.

Exceeding a WebSocket size limit closes the connection with code `1009`. `maxFrameSize` must not exceed
`maxMessageSize`.
With compression, `maxFrameSize` limits the compressed size on the wire, and a decompressed frame is limited by
`maxMessageSize`.

The number of connections is not limited by default. `maxConnections` caps the open connections and
`maxConnectionsPerAddress` those of one IP address, counted from the TCP accept, and a connection past a limit is
closed right away.
Size them to the memory available: a connection holds up to `maxFrameSize` plus `maxMessageSize` of inbound data,
2 MiB with the defaults, and the compressor and decompressor take a few hundred KiB more. Outbound data comes on top
and is unbounded under the default `BUFFER` policy, see Backpressure, as are the messages queued for the callback
executor up to `maxPendingCallbacks`, and TLS adds its own buffers.

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

Pongwire is licensed under the [Apache License 2.0](LICENSE).
