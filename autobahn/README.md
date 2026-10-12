# Autobahn conformance tests

Checks the WebSocket protocol implementation using
the [Autobahn WebSocket Testsuite](https://github.com/crossbario/autobahn-testsuite).

The suite runs in Docker as a client (`fuzzingclient`) and connects to `AutobahnTestServer` (
`pongwire-core/src/test/java/pl/mbaracz/pongwire`), an echo server that sends every message back with the same payload
and frame type: text as text and binary as binary.

These tests are not run by `mvn test`. Run them manually when needed.

## Requirements

- Java 25 and Maven
- Docker with Docker Compose

## Running

1. Start the test server on port 9001 from the project root, or run `AutobahnTestServer` directly from the IDE:

   ```sh
   mvn -pl pongwire-core test-compile org.codehaus.mojo:exec-maven-plugin:3.6.4:java \
       -Dexec.mainClass=pl.mbaracz.pongwire.AutobahnTestServer \
       -Dexec.classpathScope=test
   ```

   It prints `Pongwire listening on :9001` when ready.

2. In another terminal, run the test suite:

   ```sh
   cd autobahn
   docker compose up --abort-on-container-exit
   ```

3. Open `reports/servers/index.html` to inspect the results, then stop the test server with Ctrl+C.

## Configuration

The test server enables `permessage-deflate`, so the compression cases `12.*` and `13.*` run too.

`config/fuzzingclient.json` runs all cases except:

- `9.*`: limits and performance. These cases are slow and send messages of up to 16 MiB, which exceeds the default
  `maxMessageSize` of 1 MiB.
- `13.3.*` and `13.5.*`: compression offers with `server_max_window_bits` below 15. The server declines them, because
  the JDK's zlib compresses only with the full 15-bit window, and Autobahn reports a declined offer as unimplemented.

The container reaches the host server through `host.docker.internal`.

Docker Desktop provides this hostname on macOS and Windows. `docker-compose.yml` maps it to the host gateway so the same
configuration also works on Linux.

## Reports

Generated reports are ignored by Git.

On Linux, the container may create them as root. To remove generated reports, run:

```sh
docker compose run --rm autobahn rm -rf /reports/servers
```