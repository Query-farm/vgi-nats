// Non-destructive stream reads: messages() and kv_entries().
//
// Each batch creates a short-lived EPHEMERAL consumer (AckPolicy none, memory
// storage, one replica, 30 s inactivity threshold), fetches up to one batch,
// and deletes that consumer again. Ephemeral consumers with AckNone never
// touch durable consumers, acknowledgement state or the stored messages, and
// a work-queue or interest stream does not remove messages for them.
//
// The scan is bounded by the stream's last sequence when the scan starts, so
// it terminates on a busy stream. The cursor (next sequence) lives in the
// exported scan state, so the scan resumes on another request or process over
// the HTTP transport; every batch after the first starts by sequence.

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// BatchMsgs caps the messages fetched per batch (a variable so tests can page small).
var BatchMsgs = 2000

const (
	// BatchBytes caps the payload bytes fetched per batch.
	BatchBytes = 32 << 20
	// ephemeralIdle is the inactivity threshold of the scan's consumers.
	ephemeralIdle = 30 * time.Second
)

// ScanState is the serializable cursor of a stream scan.
type ScanState struct {
	Stream    string
	Filters   []string
	NextSeq   uint64     // next stream sequence to read; 0 = use StartTime / the start policy
	StartTime *time.Time // first batch only
	LastPer   bool       // deliver only the last message per subject (KV latest values)
	StopSeq   uint64     // the stream's last sequence when the scan started
	Remaining int64      // rows still allowed; -1 = unlimited
	Done      bool
	// KV
	Bucket         string
	IncludeDeleted bool
	SecretName     string
	// FiltersJSON holds the pushed predicates, evaluated exactly on every row.
	FiltersJSON string
}

// fetchBatch reads the next batch for st. It returns the messages in
// sequence order (at most StopSeq) and updates the cursor.
func fetchBatch(ctx context.Context, c *config, st *ScanState) ([]jetstream.Msg, error) {
	js, err := c.jetStream()
	if err != nil {
		return nil, err
	}
	cfg := jetstream.ConsumerConfig{
		Name:              "vgi_scan_" + nuid.Next(),
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: ephemeralIdle,
		MemoryStorage:     true,
		Replicas:          1,
		Description:       "vgi-nats read-only scan (ephemeral)",
	}
	if len(st.Filters) == 1 {
		cfg.FilterSubject = st.Filters[0]
	} else if len(st.Filters) > 1 {
		cfg.FilterSubjects = st.Filters
	}
	switch {
	case st.LastPer:
		cfg.DeliverPolicy = jetstream.DeliverLastPerSubjectPolicy
	case st.NextSeq > 0:
		cfg.DeliverPolicy, cfg.OptStartSeq = jetstream.DeliverByStartSequencePolicy, st.NextSeq
	case st.StartTime != nil:
		t := *st.StartTime
		cfg.DeliverPolicy, cfg.OptStartTime = jetstream.DeliverByStartTimePolicy, &t
	default:
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}
	cons, err := js.CreateConsumer(ctx, st.Stream, cfg)
	if err != nil {
		return nil, streamErr(st.Stream, err)
	}
	defer func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = js.DeleteConsumer(dctx, st.Stream, cfg.Name)
	}()
	pending := int64(cons.CachedInfo().NumPending)
	want := int64(BatchMsgs)
	if st.LastPer {
		// The last-per-subject set is re-delivered from its start on every
		// batch; entries before the cursor are skipped below.
		want = pending
	}
	want = min(want, pending)
	if st.Remaining >= 0 && !st.LastPer {
		want = min(want, st.Remaining)
	}
	if !st.LastPer && st.NextSeq > 0 && len(st.Filters) == 0 && st.StopSeq >= st.NextSeq {
		want = min(want, int64(st.StopSeq-st.NextSeq+1))
	}
	if want <= 0 {
		st.Done = true
		return nil, nil
	}
	var out []jetstream.Msg
	for int64(len(out)) < want {
		n := int(min(want-int64(len(out)), int64(BatchMsgs)))
		b, err := cons.Fetch(n, jetstream.FetchMaxBytes(BatchBytes), jetstream.FetchMaxWait(RequestTimeout))
		if err != nil {
			return nil, jsError(err)
		}
		got := 0
		for m := range b.Messages() {
			got++
			out = append(out, m)
		}
		if err := b.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
			return nil, jsError(err)
		}
		if got == 0 || !st.LastPer {
			break // one pull per batch for sequential scans; LastPer drains its whole set
		}
	}
	// Apply the stop sequence, the skip cursor (LastPer) and the row budget.
	kept := out[:0]
	maxSeq := st.NextSeq
	for _, m := range out {
		md, err := m.Metadata()
		if err != nil {
			return nil, err
		}
		seq := md.Sequence.Stream
		if seq > st.StopSeq {
			st.Done = true
			continue
		}
		if st.LastPer && seq < st.NextSeq {
			continue
		}
		if st.Remaining >= 0 && int64(len(kept)) >= st.Remaining {
			st.Done = true
			break
		}
		kept = append(kept, m)
		if seq+1 > maxSeq {
			maxSeq = seq + 1
		}
	}
	if len(kept) == 0 && !st.Done {
		st.Done = true
	}
	if st.LastPer {
		// One pass delivers the whole last-per-subject set.
		st.Done = true
	}
	st.NextSeq = maxSeq
	if st.Remaining >= 0 {
		st.Remaining -= int64(len(kept))
		if st.Remaining <= 0 {
			st.Done = true
		}
	}
	if maxSeq > st.StopSeq {
		st.Done = true
	}
	return kept, nil
}

// msgTime returns a message's stream timestamp.
func msgTime(m jetstream.Msg) (time.Time, uint64, error) {
	md, err := m.Metadata()
	if err != nil {
		return time.Time{}, 0, err
	}
	return md.Timestamp, md.Sequence.Stream, nil
}

// headerMap flattens message headers (several values become a JSON array).
func headerMap(m jetstream.Msg) map[string]string {
	h := m.Headers()
	if len(h) == 0 {
		return map[string]string{}
	}
	return joinHeaders(h)
}

// payload returns a non-nil payload slice.
func payload(m jetstream.Msg) []byte {
	if d := m.Data(); d != nil {
		return d
	}
	return []byte{}
}

// stopSeqOf reads the stream's current last sequence.
func stopSeqOf(ctx context.Context, c *config, stream string) (uint64, error) {
	js, err := c.jetStream()
	if err != nil {
		return 0, err
	}
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return 0, streamErr(stream, err)
	}
	return s.CachedInfo().State.LastSeq, nil
}

// ---------------------------------------------------------------- messages

type messagesArgs struct {
	Stream     string     `vgi:"pos=0,doc=Stream to read."`
	Subject    *string    `vgi:"name=subject,doc=Only messages on this subject; NATS wildcards * and > allowed (pushed down as the consumer's filter subject)."`
	StartSeq   *int64     `vgi:"name=start_seq,ge=1,doc=First stream sequence to read (default: the oldest message)."`
	StartTime  *time.Time `vgi:"name=start_time,doc=Read messages stored at or after this time (ignored when start_seq is given)."`
	MaxMsgs    *int64     `vgi:"name=max_msgs,ge=1,doc=Stop after this many messages."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a messagesArgs) secret() string { return deref(a.SecretName) }

var messagesColumns = []arrow.Field{
	column("stream", str, "Stream the message is stored in.", false),
	column("subject", str, "Subject the message was published on.", false),
	column("sequence", i64, "Stream sequence number (unique and increasing within the stream).", false),
	column("timestamp", tsT, "When the stream stored the message.", false),
	column("key", str, "The message key in the shared messaging vocabulary: for NATS, the subject.", false),
	column("payload", bin, "Message payload bytes.", false),
	column("payload_text", str, "The payload as text when it is valid UTF-8; otherwise NULL.", true),
	column("payload_bytes", i64, "Payload size in bytes.", false),
	column("headers", strMap, "Message headers; a header with several values is a JSON array of strings.", false),
	column("redelivered", boolT, "Always NULL: a stream read has no delivery history (kept for the shared messaging vocabulary).", true),
}

var messagesExamples = []vgi.CatalogExample{
	{SQL: "SELECT sequence, subject, timestamp, payload_text FROM nats.main.messages('ORDERS', subject := 'orders.eu.*') ORDER BY sequence",
		Description: "European order events, oldest first"},
	{SQL: "SELECT subject, count(*) AS msgs, sum(payload_bytes) AS bytes FROM nats.main.messages('ORDERS') WHERE sequence > 5 GROUP BY subject ORDER BY subject",
		Description: "Message count and bytes per subject after sequence 5 (the sequence filter is pushed down)"},
	{SQL: "SELECT json_extract_string(payload_text, '$.region') AS region, count(*) AS orders FROM nats.main.messages('ORDERS', subject := 'orders.*.created', max_msgs := 1000) GROUP BY region ORDER BY region",
		Description: "Decode JSON payloads and count created orders per region"},
}

// MessagesFunc implements messages().
type MessagesFunc struct{}

var _ vgi.TypedTableFunc[ScanState] = (*MessagesFunc)(nil)

// Name is the SQL name.
func (*MessagesFunc) Name() string { return "messages" }

// Metadata describes messages().
func (*MessagesFunc) Metadata() vgi.FunctionMetadata {
	m := meta("Stream Messages", "jetstream", "messages, read stream, browse, peek, replay, payload, headers, subject, sequence",
		"Read a JetStream stream's stored messages without consuming them: one row per message with stream, subject, "+
			"sequence, timestamp `TIMESTAMPTZ`, key (the subject), payload `BLOB`, payload_text (`VARCHAR` when valid UTF-8, "+
			"else NULL), payload_bytes, headers `MAP` (several values become a JSON array) and redelivered (always NULL), "+
			"the shared Query.Farm messaging vocabulary. Args: stream (positional), subject (wildcards allowed), start_seq, start_time, max_msgs, secret_name. "+
			"Pushed down: equality and IN-list filters on subject (or key), plus the subject argument, become the consumer's filter "+
			"subjects; comparisons and ranges on sequence choose the start and stop sequence; a lower bound on timestamp "+
			"chooses the start time. Every pushed filter is re-checked exactly on each row. Reads use a short-"+
			"lived ephemeral AckNone consumer per batch (deleted afterwards), so durable consumers, acknowledgements and "+
			"work-queue/interest retention are unaffected. The scan stops at the stream's last sequence when it started. "+
			"Decode JSON payloads with the json_extract function over payload_text."+jsDocLLM,
		"## messages(stream, subject := NULL, start_seq := NULL, start_time := NULL, max_msgs := NULL)\n\n"+
			"Browse a stream's messages without acknowledging or removing anything. `WHERE subject = ...`, `sequence` ranges and "+
			"`timestamp >= ...` are pushed down into the read.",
		"Read a stream's messages without consuming them; subject, sequence and time filters pushed down",
		messagesColumns, messagesExamples)()
	m.FilterPushdown = true
	return m
}

// ArgumentSpecs derives the signature.
func (*MessagesFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(messagesArgs{}) }

// OnBind declares the schema (resolving a named secret first).
func (*MessagesFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var a messagesArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if a.Subject != nil {
		if err := validFilterSubject(*a.Subject); err != nil {
			return nil, err
		}
	}
	return bindNamedSecret(params, a.secret(), arrow.NewSchema(messagesColumns, nil))
}

// validFilterSubject rejects subjects NATS would refuse as a filter.
func validFilterSubject(s string) error {
	if s == "" || strings.ContainsAny(s, " \t\r\n") {
		return fmt.Errorf("invalid subject %q: subjects are dot-separated tokens without spaces", s)
	}
	toks := strings.Split(s, ".")
	for i, t := range toks {
		if t == "" {
			return fmt.Errorf("invalid subject %q: empty token", s)
		}
		if t == ">" && i != len(toks)-1 {
			return fmt.Errorf("invalid subject %q: '>' must be the last token", s)
		}
	}
	return nil
}

// NewState plans the scan: filters from arguments and pushed predicates, the stop sequence.
func (*MessagesFunc) NewState(params *vgi.ProcessParams) (*ScanState, error) {
	var a messagesArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	c, err := resolveConfig(params.Secrets, a.secret())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()
	stop, err := stopSeqOf(ctx, c, a.Stream)
	if err != nil {
		return nil, err
	}
	st := &ScanState{Stream: a.Stream, StopSeq: stop, Remaining: -1, SecretName: a.secret()}
	if a.MaxMsgs != nil {
		st.Remaining = *a.MaxMsgs
	}
	if a.Subject != nil {
		st.Filters = []string{*a.Subject}
	}
	if a.StartSeq != nil {
		st.NextSeq = uint64(*a.StartSeq)
	} else if a.StartTime != nil {
		t := a.StartTime.UTC()
		st.StartTime = &t
	}
	preds, err := compileFilters(params.CurrentPushdownFilters, arrow.NewSchema(messagesColumns, nil))
	if err != nil {
		return nil, err
	}
	if len(preds) > 0 {
		b, err := json.Marshal(preds)
		if err != nil {
			return nil, err
		}
		st.FiltersJSON = string(b)
	}
	plan := planPushdown(params.CurrentPushdownFilters)
	plan.apply(st)
	if st.NextSeq > st.StopSeq || stop == 0 {
		st.Done = true
	}
	return st, nil
}

// Process emits one batch.
func (*MessagesFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *ScanState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	c, err := resolveConfig(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	msgs, err := fetchBatch(ctx, c, st)
	if err != nil {
		return err
	}
	var preds []*expr
	if st.FiltersJSON != "" {
		if err := json.Unmarshal([]byte(st.FiltersJSON), &preds); err != nil {
			return err
		}
	}
	env := newRowEnv(messagesColumns)
	rows := make([][]any, 0, len(msgs))
	for _, m := range msgs {
		ts, seq, err := msgTime(m)
		if err != nil {
			return err
		}
		p := payload(m)
		row := []any{st.Stream, m.Subject(), int64(seq), ts, m.Subject(), p, textOf(p), int64(len(p)), headerMap(m), nil}
		if len(preds) > 0 {
			env.r = row
			ok, err := matchAll(preds, env)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		rows = append(rows, row)
	}
	return emitRows(params, out, messagesColumns, rows)
}

// emitRows builds and emits one batch (an empty batch is fine).
func emitRows(params *vgi.ProcessParams, out *vgirpc.OutputCollector, fields []arrow.Field, rows [][]any) error {
	cols, err := buildGeneric(fields, rows)
	if err != nil {
		return err
	}
	for _, col := range cols {
		defer col.Release()
	}
	return out.EmitArrays(cols, int64(len(rows)))
}

// ---------------------------------------------------------------- kv_entries

type kvEntriesArgs struct {
	Bucket         string  `vgi:"pos=0,doc=KV bucket name."`
	Key            string  `vgi:"name=key,default=>,doc=Key or key pattern; NATS wildcards * and > allowed (default '>': every key). Pushed down as the filter subject."`
	IncludeHistory bool    `vgi:"name=include_history,default=false,doc=Return every stored revision of each key (up to the bucket's history depth), not just the latest."`
	IncludeDeleted bool    `vgi:"name=include_deleted,default=false,doc=Include delete and purge markers (operation DEL / PURGE)."`
	SecretName     *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a kvEntriesArgs) secret() string { return deref(a.SecretName) }

var kvEntriesColumns = []arrow.Field{
	column("bucket", str, "KV bucket name.", false),
	column("key", str, "Entry key.", false),
	column("revision", i64, "Revision: the entry's sequence in the bucket's stream.", false),
	column("operation", str, "'PUT', 'DEL' (deleted) or 'PURGE' (purged).", false),
	column("created", tsT, "When this revision was written.", false),
	column("value", bin, "Value bytes; NULL for DEL and PURGE markers.", true),
	column("value_text", str, "The value as text when it is valid UTF-8; otherwise NULL.", true),
}

var kvEntriesExamples = []vgi.CatalogExample{
	{SQL: "SELECT key, value_text, revision FROM nats.main.kv_entries('config') ORDER BY key",
		Description: "Current value of every key in the config bucket"},
	{SQL: "SELECT revision, operation, value_text, created FROM nats.main.kv_entries('config', key := 'app.color', include_history := true) ORDER BY revision",
		Description: "Change history of one key"},
}

// KVEntriesFunc implements kv_entries().
type KVEntriesFunc struct{}

var _ vgi.TypedTableFunc[ScanState] = (*KVEntriesFunc)(nil)

// Name is the SQL name.
func (*KVEntriesFunc) Name() string { return "kv_entries" }

// Metadata describes kv_entries().
func (*KVEntriesFunc) Metadata() vgi.FunctionMetadata {
	return meta("Key-Value Entries", "kv", "kv, key value, get, history, revisions, watch, config, entries",
		"Read a KV bucket: by default the latest revision of every live key; with include_history true, every stored "+
			"revision (up to the bucket's history depth); include_deleted true adds DEL/PURGE markers. Columns: bucket, "+
			"key, revision (stream sequence), operation ('PUT', 'DEL', 'PURGE'), created `TIMESTAMPTZ`, value `BLOB`, value_text. "+
			"Args: bucket (positional), key (named; exact key or NATS wildcard pattern, pushed down), include_history, "+
			"include_deleted, secret_name. Reads with an ephemeral AckNone consumer; nothing is modified."+jsDocLLM,
		"## kv_entries(bucket, key := '>', include_history := false, include_deleted := false)\n\nKV values and their history.",
		"Latest values or full revision history of a JetStream KV bucket", kvEntriesColumns, kvEntriesExamples)()
}

// ArgumentSpecs derives the signature.
func (*KVEntriesFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(kvEntriesArgs{}) }

// OnBind declares the schema.
func (*KVEntriesFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var a kvEntriesArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if err := validFilterSubject(a.Key); err != nil {
		return nil, err
	}
	return bindNamedSecret(params, a.secret(), arrow.NewSchema(kvEntriesColumns, nil))
}

// NewState plans the bucket scan.
func (*KVEntriesFunc) NewState(params *vgi.ProcessParams) (*ScanState, error) {
	var a kvEntriesArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	c, err := resolveConfig(params.Secrets, a.secret())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()
	stream := "KV_" + a.Bucket
	stop, err := stopSeqOf(ctx, c, stream)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, fmt.Errorf("KV bucket %q not found in this account", a.Bucket)
		}
		return nil, err
	}
	st := &ScanState{Stream: stream, Bucket: a.Bucket, Filters: []string{"$KV." + a.Bucket + "." + a.Key},
		StopSeq: stop, Remaining: -1, LastPer: !a.IncludeHistory, IncludeDeleted: a.IncludeDeleted, SecretName: a.secret()}
	if stop == 0 {
		st.Done = true
	}
	return st, nil
}

// Process emits one batch of entries.
func (*KVEntriesFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *ScanState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	c, err := resolveConfig(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	msgs, err := fetchBatch(ctx, c, st)
	if err != nil {
		return err
	}
	prefix := "$KV." + st.Bucket + "."
	rows := make([][]any, 0, len(msgs))
	for _, m := range msgs {
		ts, seq, err := msgTime(m)
		if err != nil {
			return err
		}
		op := "PUT"
		if h := m.Headers(); h != nil {
			if o := h.Get("KV-Operation"); o != "" {
				op = o
			}
		}
		if op != "PUT" && !st.IncludeDeleted {
			continue
		}
		var val, text any
		if op == "PUT" {
			p := payload(m)
			val, text = p, textOf(p)
		}
		rows = append(rows, []any{st.Bucket, strings.TrimPrefix(m.Subject(), prefix), int64(seq), op, ts, val, text})
	}
	return emitRows(params, out, kvEntriesColumns, rows)
}
