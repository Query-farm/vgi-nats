// JetStream management reads for the secret's account: account usage,
// streams, stream subjects, consumers, KV buckets, object stores and objects.
// Every call is a read-only JetStream API request ($JS.API.*.INFO / LIST /
// NAMES); nothing is created, updated, acknowledged or deleted.

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/nats-io/nats.go/jetstream"
)

// wire renders a JetStream enum as its JSON wire value ("limits", "file", "explicit", ...).
func wire(v json.Marshaler) any {
	b, err := v.MarshalJSON()
	if err != nil {
		return nil
	}
	var s string
	if json.Unmarshal(b, &s) != nil || s == "" {
		return nil
	}
	return s
}

func u64(v uint64) int64 {
	if v > 1<<63-1 {
		return 1<<63 - 1
	}
	return int64(v)
}

const jsDocLLM = " Reads the JetStream API of the account the secret authenticates as (JETSTREAM_DOMAIN selects a domain); read-only."

// jsArgs is the argument struct of argument-less JetStream functions.
type jsArgs = monArgs

// ---------------------------------------------------------------- account_info

var accountInfoColumns = []arrow.Field{
	column("domain", str, "JetStream domain; NULL when none.", true),
	column("memory_bytes", i64, "Memory storage in use.", false),
	column("storage_bytes", i64, "File storage in use.", false),
	column("reserved_memory_bytes", i64, "Memory reserved by stream limits.", false),
	column("reserved_storage_bytes", i64, "File storage reserved by stream limits.", false),
	column("streams", i64, "Streams in the account.", false),
	column("consumers", i64, "Consumers in the account.", false),
	column("max_memory_bytes", i64, "Account memory limit; NULL when unlimited.", true),
	column("max_storage_bytes", i64, "Account storage limit; NULL when unlimited.", true),
	column("max_streams", i64, "Stream limit; NULL when unlimited.", true),
	column("max_consumers", i64, "Consumer limit; NULL when unlimited.", true),
	column("max_ack_pending", i64, "Per-consumer ack-pending limit; NULL when unlimited.", true),
	column("api_total", i64, "JetStream API requests made by the account.", false),
	column("api_errors", i64, "JetStream API requests that failed.", false),
	column("api_level", i64, "JetStream API level of the server.", false),
	column("tiers", str, "Per-replica-tier usage and limits as JSON (clustered accounts with tiered limits); NULL when not tiered.", true),
}

var accountInfoExamples = []vgi.CatalogExample{
	{SQL: "SELECT streams, consumers, storage_bytes, max_storage_bytes FROM nats.main.account_info()",
		Description: "JetStream usage and limits of the account"},
	{SQL: "SELECT round(100.0 * storage_bytes / max_storage_bytes, 1) AS pct_storage_used FROM nats.main.account_info() WHERE max_storage_bytes IS NOT NULL",
		Description: "Share of the account's storage limit in use"},
}

func newAccountInfo() vgi.TableFunction {
	return newSimple(&simpleFunc[jsArgs]{
		name: "account_info", columns: accountInfoColumns,
		meta: meta("JetStream Account Usage", "jetstream", "jetstream account info, usage, limits, quota, storage, memory, api",
			"One row: the account's JetStream usage and limits ($JS.API.INFO): domain, memory/storage in use and reserved, "+
				"streams, consumers, limits (NULL = unlimited), API request totals and errors, tiers JSON. Named arg: secret_name."+jsDocLLM,
			"## account_info()\n\nJetStream usage and limits of the account.",
			"JetStream usage and limits of the secret's account", accountInfoColumns, accountInfoExamples),
		fetch: single(func(ctx context.Context, c *config, _ jsArgs) ([][]any, error) {
			js, err := c.jetStream()
			if err != nil {
				return nil, err
			}
			ai, err := js.AccountInfo(ctx)
			if err != nil {
				return nil, jsError(err)
			}
			var tiers any
			if len(ai.Tiers) > 0 {
				tiers = rawJSON(ai.Tiers)
			}
			l := ai.Limits
			return [][]any{{optString(ai.Domain), u64(ai.Memory), u64(ai.Store), u64(ai.ReservedMemory), u64(ai.ReservedStore),
				int64(ai.Streams), int64(ai.Consumers), limit(l.MaxMemory), limit(l.MaxStore), limit(int64(l.MaxStreams)),
				limit(int64(l.MaxConsumers)), limit(int64(l.MaxAckPending)), u64(ai.API.Total), u64(ai.API.Errors),
				int64(ai.API.Level), tiers}}, nil
		}),
	})
}

// ---------------------------------------------------------------- streams

// streamKind classifies a stream by name: KV buckets and object stores are streams too.
func streamKind(name string) string {
	switch {
	case strings.HasPrefix(name, "KV_"):
		return "kv"
	case strings.HasPrefix(name, "OBJ_"):
		return "object_store"
	}
	return "stream"
}

var streamsColumns = []arrow.Field{
	column("stream", str, "Stream name.", false),
	column("kind", str, "'stream', 'kv' (a KV bucket's KV_<bucket> stream) or 'object_store' (OBJ_<bucket>).", false),
	column("description", str, "Stream description.", true),
	column("subjects", strLst, "Subjects the stream captures (empty for mirrors).", false),
	column("retention", str, "'limits', 'interest' or 'workqueue'.", true),
	column("storage", str, "'file' or 'memory'.", true),
	column("replicas", i64, "Configured replica count.", false),
	column("max_msgs", i64, "Message limit; NULL when unlimited.", true),
	column("max_bytes", i64, "Byte limit; NULL when unlimited.", true),
	column("max_age_seconds", f64, "Maximum message age in seconds; NULL when unlimited.", true),
	column("max_msgs_per_subject", i64, "Per-subject message limit; NULL when unlimited.", true),
	column("max_msg_size", i64, "Largest message accepted; NULL when unlimited.", true),
	column("discard", str, "'old' or 'new': what happens when a limit is hit.", true),
	column("duplicate_window_seconds", f64, "De-duplication window in seconds.", true),
	column("compression", str, "'none' or 's2'.", true),
	column("allow_direct", boolT, "Whether direct-get is enabled.", false),
	column("sealed", boolT, "Whether the stream is sealed (read-only).", false),
	column("deny_delete", boolT, "Whether message deletion is denied.", false),
	column("deny_purge", boolT, "Whether purging is denied.", false),
	column("allow_rollup", boolT, "Whether rollup headers are allowed.", false),
	column("mirror", str, "Name of the stream this one mirrors; NULL when not a mirror.", true),
	column("sources", strLst, "Names of streams this one sources from.", false),
	column("created", tsT, "When the stream was created.", true),
	column("messages", i64, "Messages in the stream.", false),
	column("bytes", i64, "Bytes in the stream.", false),
	column("first_seq", i64, "Sequence of the oldest message.", false),
	column("first_ts", tsT, "Timestamp of the oldest message; NULL when empty.", true),
	column("last_seq", i64, "Sequence of the newest message.", false),
	column("last_ts", tsT, "Timestamp of the newest message; NULL when empty.", true),
	column("num_subjects", i64, "Distinct subjects holding messages.", false),
	column("num_deleted", i64, "Interior deleted messages.", false),
	column("consumer_count", i64, "Consumers on the stream.", false),
	column("cluster", str, "Cluster the stream is placed in; NULL when not clustered.", true),
	column("leader", str, "Server leading the stream; NULL when not clustered.", true),
	column("metadata", strMap, "User metadata on the stream config.", false),
	column("config", str, "The complete stream configuration as JSON.", false),
}

var streamsExamples = []vgi.CatalogExample{
	{SQL: "SELECT stream, subjects, messages, bytes, consumer_count FROM nats.main.streams() WHERE kind = 'stream' ORDER BY bytes DESC",
		Description: "Streams by size, without the KV and object-store backing streams"},
	{SQL: "SELECT stream, last_ts, now() - last_ts AS idle FROM nats.main.streams() ORDER BY idle DESC LIMIT 5",
		Description: "Streams that have gone longest without a new message"},
}

func durationLimit(d time.Duration) any {
	if d <= 0 {
		return nil
	}
	return d.Seconds()
}

func streamRow(si *jetstream.StreamInfo) []any {
	cfg, st := si.Config, si.State
	var mirror any
	if cfg.Mirror != nil {
		mirror = cfg.Mirror.Name
	}
	sources := []string{}
	for _, s := range cfg.Sources {
		if s != nil {
			sources = append(sources, s.Name)
		}
	}
	var cl, leader any
	if si.Cluster != nil {
		cl, leader = optString(si.Cluster.Name), optString(si.Cluster.Leader)
	}
	md := cfg.Metadata
	if md == nil {
		md = map[string]string{}
	}
	var firstTS, lastTS any
	if st.Msgs > 0 {
		firstTS, lastTS = optTime(st.FirstTime), optTime(st.LastTime)
	}
	return []any{cfg.Name, streamKind(cfg.Name), optString(cfg.Description), strList(cfg.Subjects), wire(cfg.Retention),
		wire(cfg.Storage), int64(cfg.Replicas), limit(cfg.MaxMsgs), limit(cfg.MaxBytes), durationLimit(cfg.MaxAge),
		limit(cfg.MaxMsgsPerSubject), limit(int64(cfg.MaxMsgSize)), wire(cfg.Discard), durationLimit(cfg.Duplicates),
		wire(cfg.Compression), cfg.AllowDirect, cfg.Sealed, cfg.DenyDelete, cfg.DenyPurge, cfg.AllowRollup, mirror, sources,
		optTime(si.Created), u64(st.Msgs), u64(st.Bytes), u64(st.FirstSeq), firstTS, u64(st.LastSeq), lastTS,
		u64(st.NumSubjects), int64(st.NumDeleted), int64(st.Consumers), cl, leader, md, rawJSON(cfg).(string)}
}

// listStreams returns every stream's info, sorted by name.
func listStreams(ctx context.Context, js jetstream.JetStream) ([]*jetstream.StreamInfo, error) {
	l := js.ListStreams(ctx)
	var out []*jetstream.StreamInfo
	for si := range l.Info() {
		out = append(out, si)
	}
	if err := l.Err(); err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
		return nil, jsError(err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Config.Name < out[j].Config.Name })
	return out, nil
}

func newStreams() vgi.TableFunction {
	return newSimple(&simpleFunc[jsArgs]{
		name: "streams", columns: streamsColumns,
		meta: meta("JetStream Streams", "jetstream", "streams, jetstream, stream info, retention, storage, limits, messages, last sequence",
			"One row per stream in the account ($JS.API.STREAM.LIST): configuration (subjects LIST, retention, storage, "+
				"replicas, limits with NULL = unlimited, discard, duplicate window, compression, flags, mirror, sources, "+
				"metadata MAP, full config JSON) and state (messages, bytes, first/last seq and ts, num_subjects, num_deleted, "+
				"consumer_count, cluster leader). kind tells plain streams from KV (KV_*) and object-store (OBJ_*) backing "+
				"streams. Named arg: secret_name."+jsDocLLM,
			"## streams()\n\nEvery stream's configuration and state.",
			"Every JetStream stream with configuration and state", streamsColumns, streamsExamples),
		fetch: single(func(ctx context.Context, c *config, _ jsArgs) ([][]any, error) {
			js, err := c.jetStream()
			if err != nil {
				return nil, err
			}
			infos, err := listStreams(ctx, js)
			if err != nil {
				return nil, err
			}
			rows := make([][]any, 0, len(infos))
			for _, si := range infos {
				rows = append(rows, streamRow(si))
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- stream_subjects

type subjectsArgs struct {
	Stream     string  `vgi:"pos=0,doc=Stream name."`
	Filter     string  `vgi:"name=filter,default=>,doc=Subject filter with NATS wildcards (default '>': every subject), applied by the server."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a subjectsArgs) secret() string { return deref(a.SecretName) }

var subjectsColumns = []arrow.Field{
	column("stream", str, "Stream name.", false),
	column("subject", str, "A subject holding messages in the stream.", false),
	column("messages", i64, "Messages currently stored on the subject.", false),
}

var subjectsExamples = []vgi.CatalogExample{
	{SQL: "SELECT subject, messages FROM nats.main.stream_subjects('ORDERS') ORDER BY messages DESC, subject",
		Description: "Message count per subject in the ORDERS stream"},
	{SQL: "SELECT split_part(subject, '.', 2) AS region, sum(messages) AS msgs FROM nats.main.stream_subjects('ORDERS', filter := 'orders.*.created') GROUP BY region ORDER BY region",
		Description: "Created orders per region, counted by the server"},
}

func newStreamSubjects() vgi.TableFunction {
	return newSimple(&simpleFunc[subjectsArgs]{
		name: "stream_subjects", columns: subjectsColumns,
		meta: meta("Stream Subjects", "jetstream", "stream subjects, subject counts, cardinality, per-subject messages",
			"One row per distinct subject stored in a stream with its message count ($JS.API.STREAM.INFO with "+
				"subjects_filter, paged by the client). Args: stream (positional), filter (named; NATS wildcards, pushed down, "+
				"default '>'), secret_name. Cheap way to count messages per subject without reading them."+jsDocLLM,
			"## stream_subjects(stream, filter := '>')\n\nSubjects in a stream with per-subject message counts.",
			"Distinct subjects in a stream with message counts, filtered by the server", subjectsColumns, subjectsExamples),
		fetch: single(func(ctx context.Context, c *config, a subjectsArgs) ([][]any, error) {
			js, err := c.jetStream()
			if err != nil {
				return nil, err
			}
			s, err := js.Stream(ctx, a.Stream)
			if err != nil {
				return nil, streamErr(a.Stream, err)
			}
			si, err := s.Info(ctx, jetstream.WithSubjectFilter(a.Filter))
			if err != nil {
				return nil, jsError(err)
			}
			subs := sortedKeys(si.State.Subjects)
			rows := make([][]any, 0, len(subs))
			for _, subj := range subs {
				rows = append(rows, []any{a.Stream, subj, u64(si.State.Subjects[subj])})
			}
			return rows, nil
		}),
	})
}

func streamErr(name string, err error) error {
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("stream %q not found in this account", name)
	}
	return jsError(err)
}

// ---------------------------------------------------------------- consumers

type consumersArgs struct {
	Stream     *string `vgi:"name=stream,doc=Only this stream's consumers (default: every stream)."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a consumersArgs) secret() string { return deref(a.SecretName) }

var consumersColumns = []arrow.Field{
	column("stream", str, "Stream the consumer reads.", false),
	column("consumer", str, "Consumer name.", false),
	column("durable", boolT, "Whether the consumer is durable (survives without subscribers).", false),
	column("description", str, "Consumer description.", true),
	column("push", boolT, "Whether it is a push consumer (has a deliver subject).", false),
	column("deliver_subject", str, "Push consumer's deliver subject; NULL for pull consumers.", true),
	column("deliver_group", str, "Push consumer's queue group.", true),
	column("filter_subjects", strLst, "Subjects the consumer is filtered to (empty: all).", false),
	column("deliver_policy", str, "'all', 'last', 'new', 'by_start_sequence', 'by_start_time' or 'last_per_subject'.", true),
	column("ack_policy", str, "'explicit', 'all' or 'none'.", true),
	column("ack_wait_seconds", f64, "Redelivery timeout in seconds.", true),
	column("max_deliver", i64, "Delivery attempts before giving up; NULL when unlimited.", true),
	column("max_ack_pending", i64, "Outstanding-ack limit; NULL when unlimited.", true),
	column("replay_policy", str, "'instant' or 'original'.", true),
	column("inactive_threshold_seconds", f64, "Idle time before an ephemeral consumer is removed.", true),
	column("created", tsT, "When the consumer was created.", true),
	column("delivered_consumer_seq", i64, "Consumer sequence of the last delivery.", false),
	column("delivered_stream_seq", i64, "Stream sequence of the last delivered message.", false),
	column("last_delivered", tsT, "When the last message was delivered; NULL if never.", true),
	column("ack_floor_consumer_seq", i64, "Consumer sequence below which everything is acknowledged.", false),
	column("ack_floor_stream_seq", i64, "Stream sequence below which everything is acknowledged.", false),
	column("last_ack", tsT, "When the ack floor last moved; NULL if never.", true),
	column("num_pending", i64, "Messages matching the consumer not yet delivered.", false),
	column("lag", i64, "Consumer lag: the same value as num_pending, named as in the other messaging workers.", false),
	column("num_ack_pending", i64, "Delivered messages awaiting acknowledgement.", false),
	column("num_redelivered", i64, "Messages delivered more than once.", false),
	column("num_waiting", i64, "Outstanding pull requests.", false),
	column("paused", boolT, "Whether the consumer is paused.", false),
	column("push_bound", boolT, "Whether a push consumer has an active subscriber.", false),
	column("leader", str, "Server leading the consumer; NULL when not clustered.", true),
	column("config", str, "The complete consumer configuration as JSON.", false),
}

var consumersExamples = []vgi.CatalogExample{
	{SQL: "SELECT stream, consumer, lag, num_ack_pending, num_redelivered FROM nats.main.consumers() WHERE lag > 0 ORDER BY lag DESC",
		Description: "Lagging consumers, worst first"},
	{SQL: "SELECT consumer, delivered_stream_seq, ack_floor_stream_seq, lag FROM nats.main.consumers(stream := 'ORDERS') ORDER BY consumer",
		Description: "Delivery position and lag of the ORDERS consumers"},
}

func consumerRow(ci *jetstream.ConsumerInfo) []any {
	cfg := ci.Config
	filters := cfg.FilterSubjects
	if len(filters) == 0 && cfg.FilterSubject != "" {
		filters = []string{cfg.FilterSubject}
	}
	var leader any
	if ci.Cluster != nil {
		leader = optString(ci.Cluster.Leader)
	}
	return []any{ci.Stream, ci.Name, cfg.Durable != "", optString(cfg.Description), cfg.DeliverSubject != "",
		optString(cfg.DeliverSubject), optString(cfg.DeliverGroup), strList(filters), optString(cfg.DeliverPolicy.String()),
		wire(cfg.AckPolicy), seconds(cfg.AckWait), limit(int64(cfg.MaxDeliver)), limit(int64(cfg.MaxAckPending)),
		wire(cfg.ReplayPolicy), seconds(cfg.InactiveThreshold), optTime(ci.Created), u64(ci.Delivered.Consumer),
		u64(ci.Delivered.Stream), optTimePtr(ci.Delivered.Last), u64(ci.AckFloor.Consumer), u64(ci.AckFloor.Stream),
		optTimePtr(ci.AckFloor.Last), u64(ci.NumPending), u64(ci.NumPending), int64(ci.NumAckPending),
		int64(ci.NumRedelivered), int64(ci.NumWaiting), ci.Paused, ci.PushBound, leader, rawJSON(cfg).(string)}
}

func newConsumers() vgi.TableFunction {
	return newSimple(&simpleFunc[consumersArgs]{
		name: "consumers", columns: consumersColumns,
		meta: meta("JetStream Consumers", "jetstream", "consumers, lag, num_pending, ack floor, redelivered, durable, pull, push",
			"One row per consumer ($JS.API.CONSUMER.LIST per stream): configuration (durable, push/pull, filter_subjects, "+
				"deliver/ack/replay policy, ack_wait, max_deliver, max_ack_pending) and state: delivered and ack-floor "+
				"consumer/stream sequences with times, num_pending (= lag: matching messages not yet delivered), "+
				"num_ack_pending, num_redelivered, num_waiting, paused, push_bound. Named args: stream (default all streams), "+
				"secret_name. Listing consumers never changes their state."+jsDocLLM,
			"## consumers(stream := NULL)\n\nConsumers with delivery state and lag.",
			"JetStream consumers with delivery state, ack floor and lag", consumersColumns, consumersExamples),
		fetch: single(func(ctx context.Context, c *config, a consumersArgs) ([][]any, error) {
			js, err := c.jetStream()
			if err != nil {
				return nil, err
			}
			var names []string
			if a.Stream != nil {
				names = []string{*a.Stream}
			} else {
				infos, err := listStreams(ctx, js)
				if err != nil {
					return nil, err
				}
				for _, si := range infos {
					names = append(names, si.Config.Name)
				}
			}
			var rows [][]any
			for _, name := range names {
				s, err := js.Stream(ctx, name)
				if err != nil {
					return nil, streamErr(name, err)
				}
				l := s.ListConsumers(ctx)
				var infos []*jetstream.ConsumerInfo
				for ci := range l.Info() {
					infos = append(infos, ci)
				}
				if err := l.Err(); err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
					return nil, jsError(err)
				}
				sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
				for _, ci := range infos {
					rows = append(rows, consumerRow(ci))
				}
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- kv_buckets

var kvBucketsColumns = []arrow.Field{
	column("bucket", str, "Bucket name.", false),
	column("description", str, "Bucket description.", true),
	column("stream", str, "Backing stream (KV_<bucket>).", false),
	column("values", i64, "Stored entries, including history and delete markers.", false),
	column("keys", i64, "Distinct keys with stored entries (including deleted keys' markers).", false),
	column("history", i64, "Revisions kept per key.", false),
	column("ttl_seconds", f64, "Entry expiry in seconds; NULL when entries never expire.", true),
	column("max_value_size", i64, "Largest value accepted; NULL when unlimited.", true),
	column("max_bytes", i64, "Bucket size limit; NULL when unlimited.", true),
	column("bytes", i64, "Bytes stored.", false),
	column("storage", str, "'file' or 'memory'.", true),
	column("replicas", i64, "Replica count.", false),
	column("compressed", boolT, "Whether the backing stream is S2-compressed.", false),
	column("last_revision", i64, "Revision (stream sequence) of the newest entry.", false),
	column("last_update", tsT, "When the newest entry was written; NULL when empty.", true),
	column("metadata", strMap, "User metadata on the bucket.", false),
}

var kvBucketsExamples = []vgi.CatalogExample{
	{SQL: "SELECT bucket, keys, values, history, bytes FROM nats.main.kv_buckets() ORDER BY bucket",
		Description: "Every KV bucket with its size and history depth"},
	{SQL: "SELECT bucket, ttl_seconds FROM nats.main.kv_buckets() WHERE ttl_seconds IS NOT NULL ORDER BY ttl_seconds",
		Description: "Buckets whose entries expire"},
}

func newKVBuckets() vgi.TableFunction {
	return newSimple(&simpleFunc[jsArgs]{
		name: "kv_buckets", columns: kvBucketsColumns,
		meta: meta("Key-Value Buckets", "kv", "kv, key value, buckets, jetstream kv, history, ttl",
			"One row per JetStream KV bucket: description, backing stream, values (entries incl. history and delete markers), "+
				"keys, history depth, ttl_seconds, size limits, bytes, storage, replicas, compressed, last_revision, last_update, "+
				"metadata. Named arg: secret_name. Read entries with kv_entries(bucket)."+jsDocLLM,
			"## kv_buckets()\n\nJetStream key-value buckets.",
			"JetStream key-value buckets with size, history depth and TTL", kvBucketsColumns, kvBucketsExamples),
		fetch: single(func(ctx context.Context, c *config, _ jsArgs) ([][]any, error) {
			js, err := c.jetStream()
			if err != nil {
				return nil, err
			}
			infos, err := listStreams(ctx, js)
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, si := range infos {
				if streamKind(si.Config.Name) != "kv" {
					continue
				}
				cfg, st := si.Config, si.State
				md := cfg.Metadata
				if md == nil {
					md = map[string]string{}
				}
				var last any
				if st.Msgs > 0 {
					last = optTime(st.LastTime)
				}
				rows = append(rows, []any{strings.TrimPrefix(cfg.Name, "KV_"), optString(cfg.Description), cfg.Name,
					u64(st.Msgs), u64(st.NumSubjects), cfg.MaxMsgsPerSubject, durationLimit(cfg.MaxAge),
					limit(int64(cfg.MaxMsgSize)), limit(cfg.MaxBytes), u64(st.Bytes), wire(cfg.Storage), int64(cfg.Replicas),
					cfg.Compression == jetstream.S2Compression, u64(st.LastSeq), last, md})
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- object stores

var objBucketsColumns = []arrow.Field{
	column("bucket", str, "Object store name.", false),
	column("description", str, "Object store description.", true),
	column("stream", str, "Backing stream (OBJ_<bucket>).", false),
	column("objects", i64, "Live (not deleted) objects.", false),
	column("size_bytes", i64, "Bytes stored, including chunk and metadata overhead.", false),
	column("ttl_seconds", f64, "Object expiry in seconds; NULL when objects never expire.", true),
	column("max_bytes", i64, "Store size limit; NULL when unlimited.", true),
	column("storage", str, "'file' or 'memory'.", true),
	column("replicas", i64, "Replica count.", false),
	column("sealed", boolT, "Whether the store is sealed (read-only).", false),
	column("compressed", boolT, "Whether the backing stream is S2-compressed.", false),
	column("metadata", strMap, "User metadata on the store.", false),
}

var objBucketsExamples = []vgi.CatalogExample{
	{SQL: "SELECT bucket, objects, size_bytes FROM nats.main.object_stores() ORDER BY size_bytes DESC",
		Description: "Object stores by size"},
	{SQL: "SELECT count(*) AS stores, sum(objects) AS objects FROM nats.main.object_stores()",
		Description: "How many object stores and objects the account has"},
}

func newObjectStores() vgi.TableFunction {
	return newSimple(&simpleFunc[jsArgs]{
		name: "object_stores", columns: objBucketsColumns,
		meta: meta("Object Stores", "objects", "object store, buckets, files, blobs, jetstream object store",
			"One row per JetStream object store: description, backing stream, objects (live, from the object list), size_bytes, "+
				"ttl_seconds, max_bytes, storage, replicas, sealed, compressed, metadata. Named arg: secret_name. List objects "+
				"with objects(bucket)."+jsDocLLM,
			"## object_stores()\n\nJetStream object stores.",
			"JetStream object stores with object counts and size", objBucketsColumns, objBucketsExamples),
		fetch: single(func(ctx context.Context, c *config, _ jsArgs) ([][]any, error) {
			js, err := c.jetStream()
			if err != nil {
				return nil, err
			}
			infos, err := listStreams(ctx, js)
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, si := range infos {
				if streamKind(si.Config.Name) != "object_store" {
					continue
				}
				cfg := si.Config
				name := strings.TrimPrefix(cfg.Name, "OBJ_")
				objs, err := listObjects(ctx, js, name, false)
				if err != nil {
					return nil, err
				}
				md := cfg.Metadata
				if md == nil {
					md = map[string]string{}
				}
				rows = append(rows, []any{name, optString(cfg.Description), cfg.Name, int64(len(objs)), u64(si.State.Bytes),
					durationLimit(cfg.MaxAge), limit(cfg.MaxBytes), wire(cfg.Storage), int64(cfg.Replicas), cfg.Sealed,
					cfg.Compression == jetstream.S2Compression, md})
			}
			return rows, nil
		}),
	})
}

func listObjects(ctx context.Context, js jetstream.JetStream, bucket string, deleted bool) ([]*jetstream.ObjectInfo, error) {
	obs, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("object store %q not found in this account", bucket)
		}
		return nil, jsError(err)
	}
	var opts []jetstream.ListObjectsOpt
	if deleted {
		opts = append(opts, jetstream.ListObjectsShowDeleted())
	}
	list, err := obs.List(ctx, opts...)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil, nil
		}
		return nil, jsError(err)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list, nil
}

type objectsArgs struct {
	Bucket         string  `vgi:"pos=0,doc=Object store name."`
	IncludeDeleted bool    `vgi:"name=include_deleted,default=false,doc=Also list deleted objects (their delete markers)."`
	IncludeContent bool    `vgi:"name=include_content,default=false,doc=Download each object into the content column (default false: content is NULL)."`
	MaxContent     int64   `vgi:"name=max_content_bytes,default=16777216,ge=0,doc=With include_content: objects larger than this many bytes get NULL content (default 16 MiB)."`
	SecretName     *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a objectsArgs) secret() string { return deref(a.SecretName) }

var objectsColumns = []arrow.Field{
	column("bucket", str, "Object store name.", false),
	column("name", str, "Object name.", false),
	column("description", str, "Object description.", true),
	column("size", i64, "Object size in bytes.", false),
	column("chunks", i64, "Number of chunks the object is stored in.", false),
	column("digest", str, "Content digest, e.g. 'SHA-256=...'.", true),
	column("modified", tsT, "When the object was last written.", true),
	column("nuid", str, "Unique id of this object version.", false),
	column("deleted", boolT, "Whether this is a delete marker.", false),
	column("link", str, "For a link: '<bucket>/<name>' it points to; NULL otherwise.", true),
	column("headers", strMap, "Object headers (multiple values joined with ', ').", false),
	column("metadata", strMap, "Object metadata.", false),
	column("content", bin, "The object bytes when include_content := true and size <= max_content_bytes; otherwise NULL.", true),
	column("content_text", str, "content as text when it is valid UTF-8; otherwise NULL.", true),
}

var objectsExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, size, chunks, modified FROM nats.main.objects('files') ORDER BY size DESC",
		Description: "Objects in the files store by size"},
	{SQL: "SELECT name, content_text FROM nats.main.objects('files', include_content := true, max_content_bytes := 1024) WHERE content_text IS NOT NULL ORDER BY name",
		Description: "Read small text objects"},
}

func joinHeaders(h map[string][]string) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		out[k] = strings.Join(vs, ", ")
	}
	return out
}

func textOf(b []byte) any {
	if b == nil || !utf8.Valid(b) {
		return nil
	}
	return string(b)
}

func newObjects() vgi.TableFunction {
	return newSimple(&simpleFunc[objectsArgs]{
		name: "objects", columns: objectsColumns,
		meta: meta("Objects", "objects", "objects, files, object store list, object info, blobs, download",
			"One row per object in an object store (the store's metadata subjects, read with a short-lived ordered consumer): "+
				"name, description, size, chunks, digest, modified, nuid, deleted, link, headers and metadata MAPs. Args: "+
				"bucket (positional), include_deleted (default false), include_content (default false: content is NULL), "+
				"max_content_bytes (default 16 MiB; larger objects keep NULL content), secret_name. content is BLOB, "+
				"content_text the same bytes as VARCHAR when valid UTF-8."+jsDocLLM,
			"## objects(bucket, include_deleted := false, include_content := false, max_content_bytes := 16777216)\n\n"+
				"Objects in an object store; content only on request.",
			"Objects in a JetStream object store, with optional content download", objectsColumns, objectsExamples),
		fetch: single(func(ctx context.Context, c *config, a objectsArgs) ([][]any, error) {
			js, err := c.jetStream()
			if err != nil {
				return nil, err
			}
			list, err := listObjects(ctx, js, a.Bucket, a.IncludeDeleted)
			if err != nil {
				return nil, err
			}
			var obs jetstream.ObjectStore
			if a.IncludeContent {
				if obs, err = js.ObjectStore(ctx, a.Bucket); err != nil {
					return nil, jsError(err)
				}
			}
			rows := make([][]any, 0, len(list))
			for _, o := range list {
				var link any
				if o.Opts != nil && o.Opts.Link != nil {
					l := o.Opts.Link
					link = l.Bucket + "/" + l.Name
				}
				md := o.Metadata
				if md == nil {
					md = map[string]string{}
				}
				var content, text any
				if a.IncludeContent && !o.Deleted && link == nil && u64(o.Size) <= a.MaxContent {
					b, err := obs.GetBytes(ctx, o.Name)
					if err != nil {
						return nil, fmt.Errorf("reading object %q: %w", o.Name, jsError(err))
					}
					if b == nil {
						b = []byte{}
					}
					content, text = b, textOf(b)
				}
				rows = append(rows, []any{a.Bucket, o.Name, optString(o.Description), u64(o.Size), int64(o.Chunks),
					optString(o.Digest), optTime(o.ModTime), o.NUID, o.Deleted, link, joinHeaders(o.Headers), md, content, text})
			}
			return rows, nil
		}),
	})
}
