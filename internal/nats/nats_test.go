package nats

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	server "github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

func TestSubjectMatches(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"orders.>", "orders.eu.created", true},
		{"orders.>", "orders", false},
		{"orders.*", "orders.eu", true},
		{"orders.*", "orders.eu.created", false},
		{"orders.*.created", "orders.eu.created", true},
		{"orders.*.created", "orders.eu.shipped", false},
		{">", "a", true},
		{"a.b", "a.b", true},
		{"a.b", "a.b.c", false},
		{"*", "orders.>", false},
		{"orders.*", "orders.>", false},
		{"orders.>", "orders.*", true},
	}
	for _, c := range cases {
		if got := subjectMatches(c.pattern, c.subject); got != c.want {
			t.Errorf("subjectMatches(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

func TestValidFilterSubject(t *testing.T) {
	for _, ok := range []string{"a", "a.b", "a.*", "a.>", ">", "$KV.config.>"} {
		if err := validFilterSubject(ok); err != nil {
			t.Errorf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "a..b", "a.>.b", "a b", ".a", "a."} {
		if validFilterSubject(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func secretsOf(fields map[string]any) vgi.Secrets {
	fields["type"] = SecretType
	return vgi.Secrets{"__default_nats": fields}
}

func TestResolveConfig(t *testing.T) {
	c, err := resolveConfig(secretsOf(map[string]any{"monitoring_url": "http://a:8222/, http://b:8222"}), "")
	if err != nil {
		t.Fatal(err)
	}
	if c.Servers != natsio.DefaultURL || len(c.MonitoringURL) != 2 || c.MonitoringURL[0] != "http://a:8222" {
		t.Fatalf("unexpected config %+v", c)
	}
	bad := []struct {
		fields map[string]any
		want   string
	}{
		{map[string]any{"servers": "http://x:4222"}, "SERVERS must be"},
		{map[string]any{"monitoring_url": "nats://x:8222"}, "MONITORING_URL must be"},
		{map[string]any{"user": "u", "token": "t"}, "exactly one authentication method"},
		{map[string]any{"tls_cert": "x"}, "both TLS_CERT and TLS_KEY"},
	}
	for _, b := range bad {
		if _, err := resolveConfig(secretsOf(b.fields), ""); err == nil || !strings.Contains(err.Error(), b.want) {
			t.Errorf("%v: got %v, want %q", b.fields, err, b.want)
		}
	}
	if _, err := resolveConfig(vgi.Secrets{}, ""); err == nil || !strings.Contains(err.Error(), "CREATE SECRET") {
		t.Errorf("missing secret: %v", err)
	}
	if _, err := resolveConfig(vgi.Secrets{}, "other"); err == nil || !strings.Contains(err.Error(), `"other"`) {
		t.Errorf("missing named secret: %v", err)
	}
}

func TestConfigKeyIgnoresMonitoringAndHidesSecrets(t *testing.T) {
	a := &config{Servers: "nats://x:4222", Password: "s3cret"}
	b := &config{Servers: "nats://x:4222", Password: "s3cret", MonitoringURL: []string{"http://x:8222"}}
	if a.key() != b.key() {
		t.Error("monitoring URL changed the connection key")
	}
	if strings.Contains(a.key(), "s3cret") {
		t.Error("key contains the password")
	}
	if got := redactURLs("nats://u:p@h:4222,nats://h2:4222"); strings.Contains(got, "u:p") {
		t.Errorf("redactURLs kept credentials: %s", got)
	}
}

func TestOptionsAuthMethods(t *testing.T) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := kp.Seed()
	creds := "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.e30.c2ln\n------END NATS USER JWT------\n\n" +
		"-----BEGIN USER NKEY SEED-----\n" + string(seed) + "\n------END USER NKEY SEED------\n"
	for _, c := range []*config{
		{Token: "t"}, {User: "u", Password: "p"}, {NKeySeed: string(seed)}, {Creds: creds},
	} {
		if _, err := c.options(); err != nil {
			t.Errorf("%+v: %v", c, err)
		}
	}
	if _, err := (&config{NKeySeed: "nope"}).options(); err == nil {
		t.Error("bad seed accepted")
	}
	if _, err := (&config{Creds: "garbage"}).options(); err == nil {
		t.Error("bad creds accepted")
	}
	if _, err := (&config{TLSCA: "not pem"}).options(); err == nil {
		t.Error("bad CA accepted")
	}
}

func TestHTTPMonitor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/varz":
			_, _ = w.Write([]byte(`{"server_id":"NID","server_name":"n1","version":"2.11.0"}`))
		case "/connz":
			if r.URL.Query().Get("acc") != "APP" || r.URL.Query().Get("state") != "open" {
				http.Error(w, "bad params "+r.URL.RawQuery, 400)
				return
			}
			_, _ = w.Write([]byte(`{"server_id":"NID","now":"2026-01-01T00:00:10Z","total":1,"connections":[{"cid":7,"name":"c","start":"2026-01-01T00:00:00Z","last_activity":"2026-01-01T00:00:05Z","rtt":"1.5ms","subscriptions_list":["a.>"]}]}`))
		case "/healthz":
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"status":"unavailable","status_code":503,"error":"JetStream not current"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := &config{MonitoringURL: []string{srv.URL}}
	ctx := context.Background()
	reps, err := monitor(ctx, c, monRequest{Path: "connz", Query: map[string][]string{"acc": {"APP"}, "state": {"open"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reps) != 1 || reps[0].ServerID != "NID" || reps[0].ServerName != "n1" {
		t.Fatalf("server identity not filled from varz: %+v", reps)
	}
	var z connz
	if err := json.Unmarshal(reps[0].Data, &z); err != nil || len(z.Conns) != 1 || z.Conns[0].Cid != 7 {
		t.Fatalf("connz decode: %v %+v", err, z)
	}
	if rtt := parseRTT(z.Conns[0].RTT); rtt != 0.0015 {
		t.Errorf("rtt = %v", rtt)
	}
	// An unhealthy /healthz (HTTP 503 with a JSON body) is data, not an error.
	reps, err = monitor(ctx, c, monRequest{Path: "healthz"})
	if err != nil || !strings.Contains(string(reps[0].Data), "unavailable") {
		t.Fatalf("healthz: %v %v", err, reps)
	}
	if _, err := monitor(ctx, c, monRequest{Path: "leafz"}); err == nil || !strings.Contains(err.Error(), "HTTP 404") {
		t.Fatalf("404: %v", err)
	}
}

// startServer runs an in-process nats-server with JetStream.
func startServer(t *testing.T, conf string) *server.Server {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true}
	if conf != "" {
		path := t.TempDir() + "/s.conf"
		if err := writeFile(path, strings.ReplaceAll(conf, "STORE", opts.StoreDir)); err != nil {
			t.Fatal(err)
		}
		o, err := server.ProcessConfigFile(path)
		if err != nil {
			t.Fatal(err)
		}
		o.NoLog, o.NoSigs = true, true
		opts = o
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("server did not start")
	}
	t.Cleanup(s.Shutdown)
	return s
}

func TestScanPagesExactlyOnceAndLeavesNoConsumer(t *testing.T) {
	s := startServer(t, "")
	c := &config{Servers: s.ClientURL()}
	js, err := c.jetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"s.*"}}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 250; i++ {
		subj := "s.a"
		if i%3 == 0 {
			subj = "s.b"
		}
		if _, err := js.Publish(ctx, subj, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	old := BatchMsgs
	BatchMsgs = 40
	defer func() { BatchMsgs = old }()

	scan := func(st *ScanState) []uint64 {
		var seqs []uint64
		for i := 0; !st.Done && i < 100; i++ {
			// Round-trip the state through JSON, as the HTTP transport would (gob).
			b, _ := json.Marshal(st)
			var copyState ScanState
			_ = json.Unmarshal(b, &copyState)
			st = &copyState
			msgs, err := fetchBatch(ctx, c, st)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range msgs {
				seqs = append(seqs, m.Seq)
			}
		}
		return seqs
	}
	all := scan(&ScanState{Stream: "S", StopSeq: 250, Remaining: -1})
	if len(all) != 250 || all[0] != 1 || all[249] != 250 {
		t.Fatalf("full scan read %d messages", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i] != all[i-1]+1 {
			t.Fatalf("gap or duplicate at %d: %d after %d", i, all[i], all[i-1])
		}
	}
	b := scan(&ScanState{Stream: "S", Filters: []string{"s.b"}, StopSeq: 250, Remaining: -1})
	if len(b) != 83 {
		t.Fatalf("filtered scan read %d, want 83", len(b))
	}
	part := scan(&ScanState{Stream: "S", NextSeq: 100, StopSeq: 120, Remaining: 15})
	if len(part) != 15 || part[0] != 100 || part[14] != 114 {
		t.Fatalf("bounded scan: %v", part)
	}
	// The stop sequence holds even when the stream grows during the scan.
	st := &ScanState{Stream: "S", StopSeq: 250, Remaining: -1, NextSeq: 240}
	if _, err := js.Publish(ctx, "s.a", []byte("late")); err != nil {
		t.Fatal(err)
	}
	if got := scan(st); len(got) != 11 {
		t.Fatalf("scan past the stop sequence: %v", got)
	}
	info, err := js.Stream(ctx, "S")
	if err != nil {
		t.Fatal(err)
	}
	if n := info.CachedInfo().State.Consumers; n != 0 {
		t.Fatalf("%d consumers left behind", n)
	}
}

func TestKVLatestPerKey(t *testing.T) {
	s := startServer(t, "")
	c := &config{Servers: s.ClientURL()}
	js, err := c.jetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "b", History: 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range [][2]string{{"k1", "a"}, {"k2", "b"}, {"k1", "c"}} {
		if _, err := kv.Put(ctx, p[0], []byte(p[1])); err != nil {
			t.Fatal(err)
		}
	}
	st := &ScanState{Stream: "KV_b", Bucket: "b", Filters: []string{"$KV.b.>"}, StopSeq: 3, Remaining: -1, LastPer: true}
	msgs, err := fetchBatch(ctx, c, st)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || !st.Done {
		t.Fatalf("latest-per-key read %d messages (done=%v)", len(msgs), st.Done)
	}
	got := map[string]string{}
	for _, m := range msgs {
		got[m.Subject] = string(m.Data)
	}
	if got["$KV.b.k1"] != "c" || got["$KV.b.k2"] != "b" {
		t.Fatalf("latest values: %v", got)
	}
}

func TestSysPingNeedsSystemAccount(t *testing.T) {
	s := startServer(t, `
listen: 127.0.0.1:-1
jetstream { store_dir: "STORE" }
accounts {
  SYS { users: [ { user: sys, password: s } ] }
  APP { jetstream: enabled, users: [ { user: app, password: a } ] }
}
system_account: SYS
`)
	old := SysGather
	SysGather = time.Second
	defer func() { SysGather = old }()
	ctx := context.Background()
	sys := &config{Servers: s.ClientURL(), User: "sys", Password: "s"}
	reps, err := monitor(ctx, sys, monRequest{SysSubject: ping("VARZ")})
	if err != nil || len(reps) != 1 || reps[0].ServerID != s.ID() {
		t.Fatalf("$SYS varz: %v %+v", err, reps)
	}
	app := &config{Servers: s.ClientURL(), User: "app", Password: "a"}
	if _, err := monitor(ctx, app, monRequest{SysSubject: ping("VARZ")}); !errors.Is(err, errNoSys) {
		t.Fatalf("app user: %v", err)
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

func TestWorkQueueStreamIsReadDirectlyAndKeepsItsMessages(t *testing.T) {
	s := startServer(t, "")
	c := &config{Servers: s.ClientURL()}
	js, err := c.jetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "WQ", Subjects: []string{"wq.*"}, Retention: jetstream.WorkQueuePolicy}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		subj := "wq.a"
		if i%2 == 1 {
			subj = "wq.b"
		}
		if _, err := js.Publish(ctx, subj, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := js.CreateConsumer(ctx, "WQ", jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	stop, direct, err := stopSeqOf(ctx, c, "WQ")
	if err != nil || !direct || stop != 30 {
		t.Fatalf("stopSeqOf: %d %v %v", stop, direct, err)
	}
	old := BatchMsgs
	BatchMsgs = 7
	defer func() { BatchMsgs = old }()
	read := func(st *ScanState) int {
		n := 0
		for i := 0; !st.Done && i < 50; i++ {
			msgs, err := fetchBatch(ctx, c, st)
			if err != nil {
				t.Fatal(err)
			}
			n += len(msgs)
		}
		return n
	}
	if n := read(&ScanState{Stream: "WQ", StopSeq: stop, Remaining: -1, Direct: true}); n != 30 {
		t.Fatalf("read %d of 30", n)
	}
	if n := read(&ScanState{Stream: "WQ", Filters: []string{"wq.b"}, StopSeq: stop, Remaining: -1, Direct: true}); n != 15 {
		t.Fatalf("filtered read %d of 15", n)
	}
	if n := read(&ScanState{Stream: "WQ", NextSeq: 25, StopSeq: stop, Remaining: 3, Direct: true}); n != 3 {
		t.Fatalf("bounded read %d of 3", n)
	}
	st, err := js.Stream(ctx, "WQ")
	if err != nil {
		t.Fatal(err)
	}
	info, _ := st.Info(ctx)
	if info.State.Msgs != 30 || info.State.Consumers != 1 {
		t.Fatalf("work queue changed: %d msgs, %d consumers", info.State.Msgs, info.State.Consumers)
	}
	cons, _ := js.Consumer(ctx, "WQ", "worker")
	if ci, _ := cons.Info(ctx); ci.NumPending != 30 || ci.Delivered.Stream != 0 {
		t.Fatalf("durable consumer moved: pending %d delivered %d", ci.NumPending, ci.Delivered.Stream)
	}
}
