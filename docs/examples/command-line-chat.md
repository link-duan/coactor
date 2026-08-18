# Command-line chat example

This example demonstrates bidirectional Sessions and `ActorRuntime::broadcast` with a multi-client command-line chat room. Each room maps to a `chat-room/<room-id>` Actor Address.

## Start the local object store

Start RustFS and create the example bucket:

```bash
docker compose up -d --wait
```

The example contains its local RustFS credentials, bucket, prefix, and Server endpoint, so no environment variables are required. The S3 API listens on `http://127.0.0.1:9000`, and the RustFS console listens on `http://127.0.0.1:9001`.

## Start the Server

Run the Server in its own terminal:

```bash
cargo run -p coactor --example chat_server
```

The Server writes startup, join, message, leave, and shutdown logs to standard output.

## Join a room

Open the same room from two more terminals with different usernames:

```bash
cargo run -p coactor --example chat_client -- lobby alice
cargo run -p coactor --example chat_client -- lobby bob
```

Enter a line to broadcast it to every live Session of the room Actor. Enter `/quit`, press `Ctrl+C`, or send EOF to leave. The Client waits for its own `Left` Event before shutting down so the Server logs the departure and other callers receive it.

Usernames must be unique within a room and contain 1–32 printable characters. Messages may contain up to 1,000 printable characters.

## Runtime behavior

Room membership exists only in the Active Actor's memory. Owner or Gateway failure ends existing Sessions, and availability failover starts a new empty room state. Actions and Events retain CoActor's in-memory, at-most-once delivery semantics.

`ActorRuntime::broadcast` targets every live Session of the Actor. A Session waiting for its `Join` result can therefore briefly receive another room Event; the provided Client handles Events until its own `Joined` or `Error` arrives.

## Run the integration test

The integration test starts RustFS, runs the production Server and Clients, verifies chat delivery plus `/quit`, EOF, and `Ctrl+C` leave paths, and cleans up its Compose resources:

```bash
python3 scripts/test-chat-example.py
```

## Stop the local environment

```bash
docker compose down
```

Add `--volumes` to also delete the local RustFS data.

Source files:

- [`coactor/examples/chat_server.rs`](../../coactor/examples/chat_server.rs)
- [`coactor/examples/chat_client.rs`](../../coactor/examples/chat_client.rs)
- [`coactor/examples/chat/protocol.rs`](../../coactor/examples/chat/protocol.rs)
- [`coactor/examples/chat/storage.rs`](../../coactor/examples/chat/storage.rs)
