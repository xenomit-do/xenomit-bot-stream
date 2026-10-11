# xenomit-bot-stream

Standalone read-only SSE gateway for live `xenomit-botd` status updates.
`botd` owns the game client and publishes a complete, replaceable status frame
over a private TCP connection. This process fans those frames out to SSE
clients; slow or disconnected readers cannot block the bot.

## Run

```powershell
$env:XENOMIT_STREAM_TOKEN = "replace-with-a-long-random-token"
cargo run --release
```

Defaults bind HTTP on `127.0.0.1:8090` and bot ingest on `127.0.0.1:8091`.
Set `XENOMIT_STREAM_HTTP_ADDR` and `XENOMIT_STREAM_INGEST_ADDR` to expose the
service on other interfaces. Keep the ingest port on a trusted network.

Configure `botd` with:

```text
XENOMIT_BOTD_STREAM_ADDR=127.0.0.1:8091
XENOMIT_BOTD_STREAM_TOKEN=<same token>
```

The publisher is optional. If either setting is absent or invalid, `botd`
continues normally with streaming disabled. It reconnects when the service
restarts and retains only its latest pending status while disconnected.

## Read the stream

```sh
curl -N -H "Authorization: Bearer $XENOMIT_STREAM_TOKEN" \
  http://127.0.0.1:8090/stream
```

Each SSE `bot.update` event contains JSON with `schema`, `type`, `bot_id`,
`tenant`, `epoch`, monotonic per-process `sequence`, observation time, and a
`status` object (phase, module, activity, uptime, balances, rates, and
human-intervention state). It contains no credentials or session identifiers. The stream is
best effort: if a client falls behind it may miss intermediate status frames;
each subsequent frame is a full state update.

`GET /healthz` is an unauthenticated liveness endpoint. `/stream` requires the
same bearer token used for bot ingest. The service requires a non-empty
`XENOMIT_STREAM_TOKEN` at startup.

## Protocol

The bot opens TCP to the ingest listener, sends `AUTH <token>\n`, then sends
one JSON object per line. Frames over 1 MiB or malformed JSON are rejected or
discarded. The ingest connection is producer-only; the HTTP endpoint is
consumer-only.
