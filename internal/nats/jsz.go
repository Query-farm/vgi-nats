// JetStream monitoring (/jsz): server-level JetStream usage and, for
// operators with system or monitoring access, every account's streams and
// consumers in one request.

package nats

import (
	"context"
	"math"
	"net/url"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
)

type jszAPI struct {
	Level  int64 `json:"level"`
	Total  int64 `json:"total"`
	Errors int64 `json:"errors"`
}

type jszSeq struct {
	ConsumerSeq int64      `json:"consumer_seq"`
	StreamSeq   int64      `json:"stream_seq"`
	LastActive  *time.Time `json:"last_active"`
}

type jszConsumer struct {
	Stream       string    `json:"stream_name"`
	Name         string    `json:"name"`
	Created      time.Time `json:"created"`
	Delivered    jszSeq    `json:"delivered"`
	AckFloor     jszSeq    `json:"ack_floor"`
	NumAckPend   int64     `json:"num_ack_pending"`
	NumRedeliver int64     `json:"num_redelivered"`
	NumWaiting   int64     `json:"num_waiting"`
	NumPending   int64     `json:"num_pending"`
	Cluster      *struct {
		Leader string `json:"leader"`
	} `json:"cluster"`
}

type jszStream struct {
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
	Cluster *struct {
		Name   string `json:"name"`
		Leader string `json:"leader"`
	} `json:"cluster"`
	State struct {
		Msgs      int64     `json:"messages"`
		Bytes     int64     `json:"bytes"`
		FirstSeq  int64     `json:"first_seq"`
		FirstTS   time.Time `json:"first_ts"`
		LastSeq   int64     `json:"last_seq"`
		LastTS    time.Time `json:"last_ts"`
		NumSubj   int64     `json:"num_subjects"`
		NumDelete int64     `json:"num_deleted"`
		Consumers int64     `json:"consumer_count"`
	} `json:"state"`
	Consumers []jszConsumer `json:"consumer_detail"`
}

type jsz struct {
	Disabled bool   `json:"disabled"`
	Memory   int64  `json:"memory"`
	Storage  int64  `json:"storage"`
	ResMem   uint64 `json:"reserved_memory"`
	ResStore uint64 `json:"reserved_storage"`
	Accounts int64  `json:"accounts"`
	HAAssets int64  `json:"ha_assets"`
	API      jszAPI
	Config   *struct {
		MaxMemory  int64  `json:"max_memory"`
		MaxStorage int64  `json:"max_storage"`
		StoreDir   string `json:"store_dir"`
		Domain     string `json:"domain"`
	} `json:"config"`
	Streams   int64 `json:"streams"`
	Consumers int64 `json:"consumers"`
	Messages  int64 `json:"messages"`
	Bytes     int64 `json:"bytes"`
	Meta      *struct {
		Name    string `json:"name"`
		Leader  string `json:"leader"`
		Size    int64  `json:"cluster_size"`
		Pending int64  `json:"pending"`
	} `json:"meta_cluster"`
	AccountDetails []struct {
		Name    string      `json:"name"`
		Streams []jszStream `json:"stream_detail"`
	} `json:"account_details"`
}

var jsServersColumns = withServer(
	column("enabled", boolT, "Whether JetStream runs on this server.", false),
	column("domain", str, "JetStream domain; NULL when none.", true),
	column("memory_bytes", i64, "Memory-storage bytes in use.", true),
	column("storage_bytes", i64, "File-storage bytes in use.", true),
	column("reserved_memory_bytes", i64, "Memory reserved by account limits.", true),
	column("reserved_storage_bytes", i64, "Storage reserved by account limits.", true),
	column("max_memory_bytes", i64, "Configured memory-storage limit.", true),
	column("max_storage_bytes", i64, "Configured file-storage limit.", true),
	column("store_dir", str, "JetStream storage directory on the server.", true),
	column("accounts", i64, "Accounts with JetStream enabled.", true),
	column("ha_assets", i64, "Replicated (Raft) assets: streams and consumers with replicas > 1.", true),
	column("streams", i64, "Streams placed on this server.", true),
	column("consumers", i64, "Consumers placed on this server.", true),
	column("messages", i64, "Messages stored on this server.", true),
	column("bytes", i64, "Bytes stored on this server.", true),
	column("api_total", i64, "JetStream API requests served.", true),
	column("api_errors", i64, "JetStream API requests that failed.", true),
	column("api_level", i64, "JetStream API level the server supports.", true),
	column("meta_leader", str, "Current meta-cluster leader; NULL when not clustered.", true),
	column("meta_cluster_size", i64, "Number of peers in the meta cluster.", true),
)

type jszArgs struct {
	Account    *string `vgi:"name=account,doc=Only this account's streams (pushed down as /jsz?acc=)."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a jszArgs) secret() string { return deref(a.SecretName) }

func fetchJsz(ctx context.Context, c *config, detail bool, account *string) ([]monReply, error) {
	q := url.Values{}
	body := map[string]any{}
	if detail {
		q.Set("accounts", "true")
		q.Set("streams", "true")
		q.Set("consumers", "true")
		body["accounts"], body["streams"], body["consumer"] = true, true, true
	}
	if account != nil {
		q.Set("acc", *account)
		body["account"] = *account
	}
	return monitor(ctx, c, monRequest{Path: "jsz", Query: q, SysSubject: ping("JSZ"), SysBody: body})
}

var jsServersExamples = []vgi.CatalogExample{
	{SQL: "SELECT server_name, storage_bytes, max_storage_bytes, round(100.0 * storage_bytes / nullif(max_storage_bytes, 0), 1) AS pct_used FROM nats.main.jetstream_servers() ORDER BY pct_used DESC",
		Description: "JetStream file-storage use per server"},
	{SQL: "SELECT server_name, streams, consumers, api_errors FROM nats.main.jetstream_servers() WHERE enabled ORDER BY server_name",
		Description: "Streams, consumers and API errors on every JetStream server"},
}

func newJetStreamServers() vgi.TableFunction {
	return newSimple(&simpleFunc[monArgs]{
		name: "jetstream_servers", columns: jsServersColumns,
		meta: meta("JetStream Server Usage", "monitoring", "jsz, jetstream, storage, memory, meta leader, raft, api errors",
			"One row per server from /jsz: JetStream enabled, domain, memory and storage in use, reserved and configured "+
				"limits, store_dir, accounts, HA assets, stream/consumer/message/byte counts, API totals and errors, meta "+
				"leader and cluster size. Named arg: secret_name."+monitoringDocLLM,
			"## jetstream_servers()\n\nJetStream resource use per server (`/jsz`).",
			"JetStream storage, memory, API and meta-cluster state per server (/jsz)", jsServersColumns, jsServersExamples),
		fetch: single(func(ctx context.Context, c *config, _ monArgs) ([][]any, error) {
			reps, err := fetchJsz(ctx, c, false, nil)
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z jsz
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				var maxMem, maxStore, dir, domain any
				if z.Config != nil {
					maxMem, maxStore, dir, domain = z.Config.MaxMemory, z.Config.MaxStorage, optString(z.Config.StoreDir), optString(z.Config.Domain)
				}
				var leader, size any
				if z.Meta != nil {
					leader, size = optString(z.Meta.Leader), z.Meta.Size
				}
				rows = append(rows, append(srv(r), !z.Disabled && z.Config != nil, domain, z.Memory, z.Storage,
					clampU64(z.ResMem), clampU64(z.ResStore), maxMem, maxStore, dir, z.Accounts, z.HAAssets, z.Streams,
					z.Consumers, z.Messages, z.Bytes, z.API.Total, z.API.Errors, z.API.Level, leader, size))
			}
			return rows, nil
		}),
	})
}

// clampU64 maps NATS's "unlimited" uint64 max to NULL.
func clampU64(v uint64) any {
	if v > math.MaxInt64 {
		return nil
	}
	return int64(v)
}

var jszStreamsColumns = withServer(
	column("account", str, "Account owning the stream.", false),
	column("stream", str, "Stream name.", false),
	column("created", tsT, "When the stream was created.", true),
	column("cluster", str, "Cluster the stream is placed in.", true),
	column("leader", str, "Server currently leading the stream.", true),
	column("messages", i64, "Messages in the stream.", false),
	column("bytes", i64, "Bytes in the stream.", false),
	column("first_seq", i64, "Sequence of the oldest message.", false),
	column("first_ts", tsT, "Timestamp of the oldest message.", true),
	column("last_seq", i64, "Sequence of the newest message.", false),
	column("last_ts", tsT, "Timestamp of the newest message.", true),
	column("num_subjects", i64, "Distinct subjects in the stream.", true),
	column("num_deleted", i64, "Interior deleted messages.", true),
	column("consumer_count", i64, "Consumers on the stream.", false),
)

var jszStreamsExamples = []vgi.CatalogExample{
	{SQL: "SELECT account, stream, messages, bytes FROM nats.main.jsz_streams() ORDER BY bytes DESC LIMIT 10",
		Description: "The ten largest streams across all accounts"},
	{SQL: "SELECT account, count(*) AS streams, sum(bytes) AS bytes FROM nats.main.jsz_streams() GROUP BY account ORDER BY bytes DESC",
		Description: "Stream count and storage per account"},
}

// replicaRow reports whether this server's copy of a stream is the one to
// list: the leader's, or the only one when not clustered.
func replicaRow(r monReply, leader string) bool {
	return leader == "" || leader == r.ServerName
}

func newJszStreams() vgi.TableFunction {
	return newSimple(&simpleFunc[jszArgs]{
		name: "jsz_streams", columns: jszStreamsColumns,
		meta: meta("Streams Across Accounts", "monitoring", "jsz, streams, all accounts, operator, storage per account",
			"Every account's streams from /jsz?accounts=true&streams=true, as seen by monitoring (operator view; for the "+
				"current account's full stream config use streams()). One row per stream, taken from the stream leader's "+
				"server: account, stream, created, cluster, leader, messages, bytes, first/last seq and ts, num_subjects, "+
				"num_deleted, consumer_count. Named args: account (pushed down), secret_name."+monitoringDocLLM,
			"## jsz_streams(account := NULL)\n\nStreams of every account, from the monitoring endpoint (`/jsz`).",
			"Streams of every account with state, from JetStream monitoring (/jsz)", jszStreamsColumns, jszStreamsExamples),
		fetch: single(func(ctx context.Context, c *config, a jszArgs) ([][]any, error) {
			reps, err := fetchJsz(ctx, c, true, a.Account)
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z jsz
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				for _, acc := range z.AccountDetails {
					if a.Account != nil && acc.Name != *a.Account {
						continue // servers answer with every account when the named one has no JetStream
					}
					for _, s := range acc.Streams {
						var cl, leader string
						if s.Cluster != nil {
							cl, leader = s.Cluster.Name, s.Cluster.Leader
						}
						if !replicaRow(r, leader) {
							continue
						}
						st := s.State
						rows = append(rows, append(srv(r), acc.Name, s.Name, optTime(s.Created), optString(cl), optString(leader),
							st.Msgs, st.Bytes, st.FirstSeq, optTime(st.FirstTS), st.LastSeq, optTime(st.LastTS), st.NumSubj,
							st.NumDelete, st.Consumers))
					}
				}
			}
			return rows, nil
		}),
	})
}

var jszConsumersColumns = withServer(
	column("account", str, "Account owning the stream.", false),
	column("stream", str, "Stream the consumer reads.", false),
	column("consumer", str, "Consumer name.", false),
	column("created", tsT, "When the consumer was created.", true),
	column("leader", str, "Server currently leading the consumer.", true),
	column("delivered_stream_seq", i64, "Stream sequence of the last delivered message.", false),
	column("ack_floor_stream_seq", i64, "Stream sequence below which everything is acknowledged.", false),
	column("num_pending", i64, "Messages matching the consumer not yet delivered: the consumer lag.", false),
	column("lag", i64, "Consumer lag (same as num_pending), named as in the other messaging workers.", false),
	column("num_ack_pending", i64, "Delivered messages awaiting acknowledgement.", false),
	column("num_redelivered", i64, "Messages delivered more than once.", false),
	column("num_waiting", i64, "Outstanding pull requests.", false),
	column("last_active", tsT, "When the consumer last delivered a message.", true),
)

var jszConsumersExamples = []vgi.CatalogExample{
	{SQL: "SELECT account, stream, consumer, lag, num_ack_pending FROM nats.main.jsz_consumers() WHERE lag > 0 ORDER BY lag DESC",
		Description: "Lagging consumers across every account"},
	{SQL: "SELECT account, sum(lag) AS total_lag FROM nats.main.jsz_consumers() GROUP BY account ORDER BY total_lag DESC",
		Description: "Total consumer lag per account"},
}

func newJszConsumers() vgi.TableFunction {
	return newSimple(&simpleFunc[jszArgs]{
		name: "jsz_consumers", columns: jszConsumersColumns,
		meta: meta("Consumers Across Accounts", "monitoring", "jsz, consumers, lag, num_pending, ack pending, redelivered, all accounts",
			"Every account's consumers from /jsz?consumers=true (operator view; for the current account use consumers()). "+
				"One row per consumer, from its leader's server: account, stream, consumer, created, leader, delivered and "+
				"ack-floor stream sequences, num_pending (= lag), num_ack_pending, num_redelivered, num_waiting, last_active. "+
				"Named args: account (pushed down), secret_name."+monitoringDocLLM,
			"## jsz_consumers(account := NULL)\n\nConsumers of every account with lag, from the monitoring endpoint (`/jsz`).",
			"Consumers of every account with lag and ack state, from JetStream monitoring (/jsz)", jszConsumersColumns, jszConsumersExamples),
		fetch: single(func(ctx context.Context, c *config, a jszArgs) ([][]any, error) {
			reps, err := fetchJsz(ctx, c, true, a.Account)
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z jsz
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				for _, acc := range z.AccountDetails {
					if a.Account != nil && acc.Name != *a.Account {
						continue // servers answer with every account when the named one has no JetStream
					}
					for _, s := range acc.Streams {
						for _, cs := range s.Consumers {
							leader := ""
							if cs.Cluster != nil {
								leader = cs.Cluster.Leader
							}
							if !replicaRow(r, leader) {
								continue
							}
							rows = append(rows, append(srv(r), acc.Name, s.Name, cs.Name, optTime(cs.Created), optString(leader),
								cs.Delivered.StreamSeq, cs.AckFloor.StreamSeq, cs.NumPending, cs.NumPending, cs.NumAckPend,
								cs.NumRedeliver, cs.NumWaiting, optTimePtr(cs.Delivered.LastActive)))
						}
					}
				}
			}
			return rows, nil
		}),
	})
}
