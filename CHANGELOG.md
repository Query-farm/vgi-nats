# Changelog

## 0.1.0 (unreleased)

First version of the NATS worker (from vgi-starter-go v1.3.0).

- Monitoring over HTTP or `$SYS`: server_info, connections, routes, gateways, leafnodes,
  subscriptions, subscription_stats, accounts, account_stats, jetstream_servers, jsz_streams,
  jsz_consumers, health.
- JetStream (read-only): account_info, streams, stream_subjects, consumers (with lag),
  messages (pushdown of subject, sequence and timestamp; exact re-evaluation), kv_buckets,
  kv_entries, object_stores, objects; subject_matches scalar.
- `nats` secret: token, user/password, NKey seed, creds contents, TLS, monitoring URLs, domain.

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
