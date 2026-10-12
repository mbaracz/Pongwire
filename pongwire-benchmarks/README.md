# pongwire benchmarks

This module contains two different kinds of benchmark:

- `benchmarks.jar` contains the JMH microbenchmarks.
- `pongwire-load.jar` is an end-to-end load benchmark using real TCP/WebSocket connections on loopback.

Build both executables from the repository root:

```bash
mvn -pl pongwire-benchmarks -am package -DskipTests
```

Run the load benchmark:

```bash
java -jar pongwire-benchmarks/target/pongwire-load.jar \
    --connections 10000 \
    --messages-per-client 10 \
    --payload-size 128
```

The benchmark opens every connection, waits for every handshake attempt, keeps the connections idle for 10 seconds,
sends echo messages, and calls `server.stop()` while the clients are still connected.
It reports measurements and correctness counts but does not enforce machine-dependent throughput thresholds.
A non-zero exit status indicates a functional failure such as a failed handshake, disconnected client, missing echo, or
remaining server session.

Because the server and clients run in one process, every loopback connection needs roughly two file descriptors.
The benchmark checks the process limit before starting and prints a suggested `ulimit -n` value when it is too low.
Large runs can also require a wider ephemeral port range and OS/JVM tuning.
