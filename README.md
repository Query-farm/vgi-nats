<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query NATS Servers and JetStream Streams, Consumers and KV Stores in DuckDB

> **vgi-nats** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-nats/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-nats/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

NATS keeps its state in two places: the monitoring endpoints that describe the servers, and
JetStream, which stores streams, consumers, key-value buckets and objects. Both answer in JSON
meant for one tool at a time. This worker turns them into **SQL tables in DuckDB**: join consumer
lag with stream sizes, count messages per subject, decode JSON payloads, diff KV history, and
check which servers are slow, all next to your Parquet, CSV or warehouse tables.

It is **read-only**. It never publishes, never acknowledges, never purges or deletes messages,
and never creates durable consumers. Reading messages uses short-lived *ephemeral* consumers
with no acknowledgements on limits-retention streams, and the stream's "get message" API (no
consumer at all) on work-queue and interest streams, so durable consumers, ack state and
retention are never touched.

- **Monitoring**: servers (`/varz`), client connections (`/connz`, with server-side state,
  account, user, sort and paging), cluster routes, super-cluster gateways, leaf nodes,
  subscriptions (with the server's own "who receives this subject" match), accounts, per-account
  traffic, JetStream usage and health. From the HTTP monitoring port, or from the `$SYS` request
  API when only NATS protocol access is available, one row per server in the cluster.
- **JetStream**: account usage and limits, every stream's configuration and state, per-subject
  message counts, consumers with delivered/ack-floor positions and **lag**.
- **Messages**: `messages(stream)` reads stored messages oldest first, with `subject`, `sequence`
  and `timestamp` filters pushed into the read and every pushed predicate re-applied exactly.
- **KV and object stores**: buckets, latest values or the full revision history, objects with
  their metadata and (on request) their content.

## Quick start

```sql
INSTALL vgi FROM community;     -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository
LOAD vgi;
ATTACH 'nats' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-nats@v0.2.0');

CREATE SECRET (TYPE nats, SERVERS 'nats://localhost:4222', USER 'app', PASSWORD '...',
               MONITORING_URL 'http://localhost:8222');

-- Lagging consumers, worst first.
SELECT stream, consumer, lag, num_ack_pending, num_redelivered
FROM nats.main.consumers() WHERE lag > 0 ORDER BY lag DESC;

-- Created orders per region in the last hour, decoded from JSON payloads.
SELECT json_extract_string(payload_text, '$.region') AS region, count(*) AS orders
FROM nats.main.messages('ORDERS', subject := 'orders.*.created')
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY region ORDER BY orders DESC;
```

The secret type is registered by the worker, so run `CREATE SECRET` after `ATTACH`.

<details><summary>Other ways to run it</summary>

- **DuckDB 2.0**:
  `SET allow_extension_repositories = 'allowed'; CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'; INSTALL vgi FROM query_farm; LOAD vgi FROM query_farm;`
  then attach as above.
- **Docker** (HTTP): `docker run -p 8000:8000 ghcr.io/query-farm/vgi-nats:0.2.0`, then
  `ATTACH 'nats' (TYPE vgi, LOCATION 'http://localhost:8000')`.
- **Go**: `go install github.com/Query-farm/vgi-nats/cmd/vgi-nats-worker@v0.2.0`,
  then attach the binary by absolute path.
- **From source**: `make build` (Go 1.26+, pure Go, no cgo), then
  `ATTACH 'nats' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-nats-worker')`.
</details>

## Secrets and configuration

Every function except `subject_matches` reads a `nats` DuckDB secret. Nothing is read
implicitly: no `~/.nats` contexts, no `nsc` stores, no environment variables.

| Field | Meaning |
|---|---|
| `SERVERS` | Comma-separated `nats://`, `tls://`, `ws://` or `wss://` URLs. Default `nats://127.0.0.1:4222`. |
| `TOKEN` | Token authentication. |
| `USER`, `PASSWORD` | User/password authentication. |
| `NKEY_SEED` | NKey authentication: the user seed (`SU...`). |
| `CREDS` | Decentralized (JWT) authentication: the **contents** of a `.creds` file (user JWT plus seed). |
| `TLS_CA`, `TLS_CERT`, `TLS_KEY` | PEM text: a CA to verify the server, and a client certificate and key for mutual TLS. |
| `MONITORING_URL` | Comma-separated HTTP monitoring base URLs, one per server (`http://n1:8222,http://n2:8222`). |
| `JETSTREAM_DOMAIN` | JetStream domain to address (leaf nodes, multi-domain deployments). |

Use exactly one of `TOKEN`, `USER`, `NKEY_SEED` and `CREDS`. Secret values (`PASSWORD`,
`TOKEN`, `NKEY_SEED`, `CREDS`, `TLS_KEY`) are redacted in `duckdb_secrets()` and never stored
in scan state, logged or returned.

With several secrets, give the extra ones a `SCOPE` (any string, e.g. `SCOPE 'nats://sys'`) so
they never compete with the default, and pick one per call with `secret_name := '<name>'`:

```sql
CREATE SECRET nats_sys (TYPE nats, SERVERS 'nats://n1:4222', USER 'sys', PASSWORD '...', SCOPE 'nats://sys');
SELECT server_name, version, connections FROM nats.main.server_info(secret_name := 'nats_sys');
```

### Which credentials each function needs

- **JetStream, KV and object-store functions** (`account_info`, `streams`, `stream_subjects`,
  `consumers`, `messages`, `kv_buckets`, `kv_entries`, `object_stores`, `objects`) read the
  account the secret authenticates as. They need permission to call `$JS.API.>` (INFO, LIST,
  NAMES, `STREAM.MSG.GET`, and `CONSUMER.CREATE` / `CONSUMER.DELETE` for the scan's own
  ephemeral consumers) and to subscribe to `_INBOX.>`.
- **Monitoring functions** use the HTTP endpoints when `MONITORING_URL` is set: they need only
  network access to the monitoring port (8222), which shows every account. Without
  `MONITORING_URL` they send `$SYS.REQ.SERVER.PING.<ENDPOINT>` requests, which require a user
  of the **system account** (`system_account` in the server config, or the operator's system
  account in JWT mode); every server in the cluster answers, so you get one row per server.
  An application-account user without `MONITORING_URL` gets an error saying exactly this.

## What's inside

All functions live in `nats.main`. Monitoring rows start with `server_id` and `server_name`.
Times are `TIMESTAMPTZ`, durations are `*_seconds DOUBLE`, limits are `NULL` when unlimited,
and open-ended documents (`varz`, `config`, account `detail`) are JSON text.

| Function | Source | Rows |
|---|---|---|
| `server_info()` | `/varz` | one per server: version, cluster, JetStream, uptime, memory, CPU, counts, traffic, limits |
| `connections(state, sort, account, user, cid, subject, skip, max_rows)` | `/connz` | one per client connection |
| `routes()` | `/routez` | cluster routes with RTT and traffic |
| `gateways()` | `/gatewayz` | inbound and outbound super-cluster gateways |
| `leafnodes(account)` | `/leafz` | leaf-node connections |
| `subscriptions(account, test_subject)` | `/subsz?subs=1` | the interest graph, one per subscription |
| `subscription_stats()` | `/subsz` | interest-graph and match-cache statistics |
| `accounts()` | `/accountz` | accounts with JetStream status, connections, imports and exports |
| `account_stats(include_unused)` | `/accstatz` | per-account connections and message/byte counters |
| `jetstream_servers()` | `/jsz` | per-server JetStream storage, memory, API and meta-cluster state |
| `jsz_streams(account)`, `jsz_consumers(account)` | `/jsz?accounts&streams&consumers` | every account's streams and consumers (operator view) |
| `health(js_enabled_only, js_server_only)` | `/healthz` | health status per server |
| `account_info()` | `$JS.API.INFO` | the account's JetStream usage and limits |
| `streams()` | `$JS.API.STREAM.LIST` | configuration and state of every stream (`kind`: stream / kv / object_store) |
| `stream_subjects(stream, filter)` | stream info with `subjects_filter` | distinct subjects with message counts |
| `consumers(stream)` | `$JS.API.CONSUMER.LIST` | configuration, delivered/ack-floor positions, `num_pending` = `lag` |
| `messages(stream, subject, start_seq, start_time, max_msgs)` | ephemeral consumer | stored messages, oldest first |
| `kv_buckets()` | `KV_*` streams | KV buckets with history depth, TTL, size |
| `kv_entries(bucket, key, include_history, include_deleted)` | ephemeral consumer on `$KV.<bucket>.<key>` | latest values, or every revision |
| `object_stores()` | `OBJ_*` streams | object stores with object count and size |
| `objects(bucket, include_deleted, include_content, max_content_bytes)` | object metadata | objects; `content` only on request |
| `subject_matches(pattern, subject)` | local | scalar: NATS `*` / `>` wildcard matching |

### Messaging vocabulary

`messages()` follows the column contract shared by the Query.Farm messaging workers
(vgi-rabbitmq, vgi-kafka, vgi-kinesis, vgi-pubsub), so their reads `UNION ALL`:

| Column | Type | NATS meaning |
|---|---|---|
| `stream`, `subject` | `VARCHAR` | Source columns: the stream, and the subject the message was published on. |
| `sequence` | `BIGINT` | The stream sequence (unique, increasing). |
| `timestamp` | `TIMESTAMPTZ` | When the stream stored the message. |
| `key` | `VARCHAR` | The subject again, as the vocabulary's routing key. |
| `payload` | `BLOB` | Raw payload bytes. |
| `payload_text` | `VARCHAR` | The payload as text when valid UTF-8, else `NULL`; decode with `json_extract`. |
| `payload_bytes` | `BIGINT` | Payload size. |
| `headers` | `MAP(VARCHAR, VARCHAR)` | NATS headers; a header with several values is a JSON array of strings. |
| `redelivered` | `BOOLEAN` | Always `NULL`: a stream read has no delivery history. |

**Consumer lag** is `consumers().lag` (and `jsz_consumers().lag`): the consumer's `num_pending`,
messages matching its filter that it has not been delivered yet. `num_ack_pending` counts
delivered but unacknowledged messages.

## What pushes down

- `messages`: `subject` (argument, NATS wildcards allowed) and `WHERE subject = ...` /
  `subject IN (...)` (or the same on `key`) become the ephemeral consumer's filter subjects;
  `start_seq` and `WHERE sequence >, >=, =, BETWEEN, <, <=` choose the start sequence and the
  stop sequence; `start_time` and a lower bound on `timestamp` choose a start time
  (`DeliverByStartTime`); `max_msgs` caps the messages read. DuckDB does not re-check rows a
  table function returns, so the worker **re-evaluates every pushed predicate exactly** on every
  row (including ones the read cannot narrow, such as `payload_text LIKE '%x%'` or
  `headers['X-Tenant'] = 'acme'`); the read only has to return a superset.
- `kv_entries`: `key` (exact key or wildcard) becomes the filter subject; latest values use
  `DeliverLastPerSubject`.
- `connections`: `state`, `sort`, `account`, `user`, `cid`, `skip` (offset) and `max_rows` go to
  `/connz`; `subject` is matched against each connection's subscriptions (and sent as
  `filter_subject` in `$SYS` mode with `account`).
- `subscriptions`: `account` and `test_subject` go to `/subsz`. `leafnodes`, `jsz_streams`,
  `jsz_consumers`: `account`. `account_stats`: `include_unused`. `health`: the JetStream
  check options. `stream_subjects`: `filter` is the server's `subjects_filter`.

### Scans, batches and termination

`messages` and `kv_entries` read in batches of up to 2,000 messages or 32 MiB. On a
limits-retention stream each batch creates one ephemeral consumer (`AckNone`, memory storage, one replica, 30 s inactivity
threshold) starting at the cursor, fetches the batch, and deletes that consumer; if the
worker dies mid-batch the server removes it after 30 s. On a **work-queue or interest** stream,
where any consumer takes part in retention, `messages` instead reads one message per
`STREAM.MSG.GET` request (`next_by_subj` from the cursor): slower, but it cannot remove or
acknowledge anything. There, `start_time` is applied by skipping older messages. The cursor (next sequence) lives in the
scan state, so a scan resumes correctly over the HTTP transport. A scan stops at the stream's
last sequence when it started, so it terminates on a busy stream. Monitoring pages
(`connections`, `subscriptions`) are fetched 1,024 at a time with the offset in the scan state.

## Examples

```sql
-- Streams by size, without the KV and object-store backing streams.
SELECT stream, subjects, messages, bytes, consumer_count
FROM nats.main.streams() WHERE kind = 'stream' ORDER BY bytes DESC;

-- Messages per subject without reading them.
SELECT subject, messages FROM nats.main.stream_subjects('ORDERS', filter := 'orders.*.created');

-- Headers and payloads of one tenant's messages after sequence 1,000,000.
SELECT sequence, subject, payload_text
FROM nats.main.messages('ORDERS')
WHERE sequence > 1000000 AND headers['X-Tenant'] = 'acme'
ORDER BY sequence LIMIT 100;

-- Who would receive a message on orders.eu.created?
SELECT account, subject, queue_group, cid
FROM nats.main.subscriptions(test_subject := 'orders.eu.created');

-- History of a KV key.
SELECT revision, operation, value_text, created
FROM nats.main.kv_entries('config', key := 'app.color', include_history := true) ORDER BY revision;

-- Heaviest clients per server, sorted and limited by the server.
SELECT server_name, name, account, out_bytes
FROM nats.main.connections(sort := 'bytes_to', max_rows := 10);

-- Is any server unhealthy?
SELECT server_name, status, error FROM nats.main.health() WHERE NOT healthy;
```

## How it is tested

The SQL suite (`test/sql/*.test`, 590+ assertions) runs through the real `vgi` extension in
Haybarn's `haybarn-unittest`, over stdio, the `launch:` socket pool and HTTP, against
**`bin/natsfixture`**: a Go program that embeds `nats-server` (Apache-2.0,
`github.com/nats-io/nats-server/v2/server`) in-process, so neither CI nor a developer needs
Docker. It starts two servers on random loopback ports:

- `vgi-nats-fixture`: JetStream and HTTP monitoring on, two accounts: `SYS` (the system
  account, user `sys`) and `APP` (JetStream, user `app` and an NKey user). It publishes
  deterministic data: an `ORDERS` stream (10 messages with headers, a binary and an empty
  payload, and a time boundary printed between sequence 5 and 6), a multi-subject memory stream,
  a 25,000-message `BULK` stream for multi-batch scans, a work-queue stream with an idle durable
worker (read without removing anything), durable consumers with known
  delivery state, a KV bucket with history and a delete, and an object store with a
  multi-chunk object and a deleted one.
- `vgi-nats-token`: token authentication only, no JetStream.

The harness (`ci/run-integration.sh`) starts the fixture, exports the URLs it prints, runs the
suite and stops it. Monitoring responses are live from the embedded server (no recordings to
go stale), and assertions only use stable fields. Go unit tests (`make test-unit`) start their
own in-process servers to check paging (every sequence exactly once across batches with the
state round-tripped, filters, stop sequence on a growing stream, no consumer left behind),
latest-per-key KV reads, `$SYS` detection, HTTP monitoring decoding, secret validation and
auth options.

`make check` runs everything: gofmt, `go vet`, golangci-lint, unit tests, the SQL suite over
all three transports, and vgi-lint at `fail_on = "info"` (100/100). CI runs the same; its
vgi-lint step uses `execute: "false"` because every table function needs a live NATS server.

## Known limitations

- **Timestamp filters in a non-UTC session.** vgi-go 0.42.0 rejects a pushed
  `timestamp` comparison when the session `TimeZone` is not UTC ("comparison operands have
  incompatible types"): the literal arrives typed with the session zone and the column is
  UTC. Use `start_time := ...` instead, or `SET TimeZone = 'UTC'`.
- `messages` never reads past the stream's last sequence at scan start; tail new messages by
  re-running with `start_seq` from the last `sequence` you saw.
- `kv_entries` without history re-reads the last-per-key set in one pass; very large buckets
  are read in one request set. Mirrored or sourced KV buckets with renamed subjects are not
  decoded.
- `max_msgs` counts messages read, before predicates the read cannot push down.
- Monitoring through `$SYS` waits up to 2 s for every server to answer (0.3 s after the last
  reply).
- `objects(include_content := true)` downloads each object; keep `max_content_bytes` small on
  large stores.

## Development

See [AGENTS.md](AGENTS.md) for the layout, the recipe for adding a function and the sharp
edges. `make help` lists the targets; `make template-diff` shows upstream starter changes
(this worker started from vgi-starter-go v1.3.0).

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). NATS is a CNCF project; this
worker is not affiliated with or endorsed by Synadia or the NATS maintainers. It uses the
Apache-2.0 `nats.go` client; the test fixture embeds the Apache-2.0 `nats-server`, which is not
linked into the worker binary.
