// Monitoring functions: one row per server (or per connection, route,
// subscription, ... per server) from the HTTP monitoring endpoints or the
// $SYS request API.
//
//	server_info        /varz       $SYS.REQ.SERVER.PING.VARZ
//	connections        /connz      $SYS.REQ.SERVER.PING.CONNZ   (state, sort, account, user, cid, paging pushed down)
//	routes             /routez     $SYS.REQ.SERVER.PING.ROUTEZ
//	gateways           /gatewayz   $SYS.REQ.SERVER.PING.GATEWAYZ
//	leafnodes          /leafz      $SYS.REQ.SERVER.PING.LEAFZ
//	subscriptions      /subsz      $SYS.REQ.SERVER.PING.SUBSZ    (account, test subject, paging pushed down)
//	subscription_stats /subsz      $SYS.REQ.SERVER.PING.SUBSZ
//	accounts           /accountz   $SYS.REQ.SERVER.PING.ACCOUNTZ
//	account_stats      /accstatz   $SYS.REQ.ACCOUNT.PING.STATZ
//	jetstream_servers  /jsz        $SYS.REQ.SERVER.PING.JSZ
//	jsz_streams        /jsz        (streams=1, consumers=1)
//	jsz_consumers      /jsz
//	health             /healthz    $SYS.REQ.SERVER.PING.HEALTHZ

package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// serverCols are the leading columns of every monitoring function.
var serverCols = []arrow.Field{
	column("server_id", str, "Server ID (the server's public NKey).", true),
	column("server_name", str, "Server name (server_name in its config).", true),
}

func withServer(cols ...arrow.Field) []arrow.Field {
	return append(append([]arrow.Field{}, serverCols...), cols...)
}

func srv(r monReply) []any { return []any{optString(r.ServerID), optString(r.ServerName)} }

const monitoringDocLLM = " Monitoring source: the secret's MONITORING_URL (one HTTP base URL per server, port 8222) or, " +
	"without it, the $SYS request API, which answers once per server in the cluster and needs system-account credentials."

// monArgs are the arguments shared by argument-less monitoring functions.
type monArgs struct {
	SecretName *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist (default: the nats secret whose scope matches)."`
}

func (a monArgs) secret() string { return deref(a.SecretName) }

func decode(r monReply, v any) error {
	if err := json.Unmarshal(r.Data, v); err != nil {
		return fmt.Errorf("decoding monitoring reply from %s: %w", r.ServerName, err)
	}
	return nil
}

func ping(name string) string { return "$SYS.REQ.SERVER.PING." + name }

// ---------------------------------------------------------------- server_info

type varz struct {
	ServerID       string    `json:"server_id"`
	ServerName     string    `json:"server_name"`
	Version        string    `json:"version"`
	GitCommit      string    `json:"git_commit"`
	Go             string    `json:"go"`
	Host           string    `json:"host"`
	Port           int64     `json:"port"`
	AuthRequired   bool      `json:"auth_required"`
	TLSRequired    bool      `json:"tls_required"`
	MaxConnections int64     `json:"max_connections"`
	MaxPayload     int64     `json:"max_payload"`
	Start          time.Time `json:"start"`
	Now            time.Time `json:"now"`
	Mem            int64     `json:"mem"`
	Cores          int64     `json:"cores"`
	CPU            float64   `json:"cpu"`
	Connections    int64     `json:"connections"`
	TotalConns     int64     `json:"total_connections"`
	Routes         int64     `json:"routes"`
	Remotes        int64     `json:"remotes"`
	Leafnodes      int64     `json:"leafnodes"`
	InMsgs         int64     `json:"in_msgs"`
	OutMsgs        int64     `json:"out_msgs"`
	InBytes        int64     `json:"in_bytes"`
	OutBytes       int64     `json:"out_bytes"`
	SlowConsumers  int64     `json:"slow_consumers"`
	Subscriptions  int64     `json:"subscriptions"`
	ConfigLoadTime time.Time `json:"config_load_time"`
	SystemAccount  string    `json:"system_account"`
	Tags           []string  `json:"tags"`
	Cluster        struct {
		Name string `json:"name"`
	} `json:"cluster"`
	JetStream struct {
		Config *struct {
			Domain string `json:"domain"`
		} `json:"config"`
	} `json:"jetstream"`
}

var serverInfoColumns = withServer(
	column("version", str, "nats-server version, e.g. '2.11.4'.", true),
	column("git_commit", str, "Git commit the server was built from.", true),
	column("go_version", str, "Go toolchain the server was built with.", true),
	column("host", str, "Client listen host.", true),
	column("port", i64, "Client listen port.", true),
	column("cluster", str, "Cluster name; NULL when not clustered.", true),
	column("jetstream_enabled", boolT, "Whether JetStream runs on this server.", false),
	column("jetstream_domain", str, "JetStream domain; NULL when none is configured.", true),
	column("auth_required", boolT, "Whether clients must authenticate.", false),
	column("tls_required", boolT, "Whether clients must use TLS.", false),
	column("start_time", tsT, "When the server started.", true),
	column("now", tsT, "The server's clock when it answered.", true),
	column("uptime_seconds", f64, "now - start_time in seconds.", true),
	column("config_load_time", tsT, "When the configuration was last (re)loaded.", true),
	column("mem_bytes", i64, "Resident memory of the server process.", true),
	column("cores", i64, "CPU cores the server sees.", true),
	column("cpu_percent", f64, "CPU use in percent of one core.", true),
	column("connections", i64, "Current client connections.", true),
	column("total_connections", i64, "Client connections accepted since start.", true),
	column("routes", i64, "Current cluster routes.", true),
	column("gateways", i64, "Current outbound gateway (super-cluster) connections.", true),
	column("leafnodes", i64, "Current leaf-node connections.", true),
	column("subscriptions", i64, "Subscriptions in the server's interest graph.", true),
	column("in_msgs", i64, "Messages received since start.", true),
	column("out_msgs", i64, "Messages sent since start.", true),
	column("in_bytes", i64, "Bytes received since start.", true),
	column("out_bytes", i64, "Bytes sent since start.", true),
	column("slow_consumers", i64, "Slow-consumer events since start.", true),
	column("max_connections", i64, "Configured connection limit.", true),
	column("max_payload", i64, "Largest message payload accepted, in bytes.", true),
	column("system_account", str, "Name of the system account; NULL when none.", true),
	column("tags", strLst, "Server tags (placement tags).", false),
	column("varz", str, "The complete /varz document as JSON, for fields without a column.", false),
)

var serverInfoExamples = []vgi.CatalogExample{
	{SQL: "SELECT server_name, version, jetstream_enabled, connections, round(uptime_seconds / 3600, 1) AS uptime_hours FROM nats.main.server_info() ORDER BY server_name",
		Description: "Version, JetStream status and load of every server"},
	{SQL: "SELECT sum(in_msgs) AS msgs_in, sum(out_msgs) AS msgs_out, sum(slow_consumers) AS slow FROM nats.main.server_info()",
		Description: "Cluster-wide message totals and slow-consumer events"},
}

func newServerInfo() vgi.TableFunction {
	return newSimple(&simpleFunc[monArgs]{
		name: "server_info", columns: serverInfoColumns,
		meta: meta("Server Information", "monitoring", "varz, server, version, uptime, memory, cpu, connections, slow consumers, cluster",
			"One row per server from /varz: version, cluster, JetStream status, start time and uptime, memory, CPU, "+
				"connection/route/gateway/leaf counts, message and byte totals, slow consumers, limits; varz holds the whole "+
				"document as JSON. Named arg: secret_name."+monitoringDocLLM,
			"## server_info()\n\nServer identity, load and counters (`/varz`), one row per server.",
			"Server version, uptime, resources and traffic counters (/varz), one row per server", serverInfoColumns, serverInfoExamples),
		fetch: single(func(ctx context.Context, c *config, _ monArgs) ([][]any, error) {
			reps, err := monitor(ctx, c, monRequest{Path: "varz", SysSubject: ping("VARZ")})
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var v varz
				if err := decode(r, &v); err != nil {
					return nil, err
				}
				var up any
				if !v.Start.IsZero() && !v.Now.IsZero() {
					up = v.Now.Sub(v.Start).Seconds()
				}
				domain := ""
				if v.JetStream.Config != nil {
					domain = v.JetStream.Config.Domain
				}
				rows = append(rows, append(srv(r), optString(v.Version), optString(v.GitCommit), optString(v.Go),
					optString(v.Host), v.Port, optString(v.Cluster.Name), v.JetStream.Config != nil, optString(domain),
					v.AuthRequired, v.TLSRequired, optTime(v.Start), optTime(v.Now), up, optTime(v.ConfigLoadTime),
					v.Mem, v.Cores, v.CPU, v.Connections, v.TotalConns, v.Routes, v.Remotes, v.Leafnodes, v.Subscriptions,
					v.InMsgs, v.OutMsgs, v.InBytes, v.OutBytes, v.SlowConsumers, v.MaxConnections, v.MaxPayload,
					optString(v.SystemAccount), strList(v.Tags), string(r.Data)))
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- connections

type connInfo struct {
	Cid            int64      `json:"cid"`
	Kind           string     `json:"kind"`
	Type           string     `json:"type"`
	IP             string     `json:"ip"`
	Port           int64      `json:"port"`
	Start          time.Time  `json:"start"`
	LastActivity   time.Time  `json:"last_activity"`
	Stop           *time.Time `json:"stop"`
	Reason         string     `json:"reason"`
	RTT            string     `json:"rtt"`
	PendingBytes   int64      `json:"pending_bytes"`
	InMsgs         int64      `json:"in_msgs"`
	OutMsgs        int64      `json:"out_msgs"`
	InBytes        int64      `json:"in_bytes"`
	OutBytes       int64      `json:"out_bytes"`
	Subscriptions  int64      `json:"subscriptions"`
	Name           string     `json:"name"`
	Lang           string     `json:"lang"`
	Version        string     `json:"version"`
	TLSVersion     string     `json:"tls_version"`
	TLSCipher      string     `json:"tls_cipher_suite"`
	AuthorizedUser string     `json:"authorized_user"`
	Account        string     `json:"account"`
	SubsList       []string   `json:"subscriptions_list"`
	MQTTClient     string     `json:"mqtt_client"`
}

type connz struct {
	Now   time.Time  `json:"now"`
	Total int64      `json:"total"`
	Conns []connInfo `json:"connections"`
}

// ConnzPage is the page size requested from /connz.
var ConnzPage = 1024

type connectionsArgs struct {
	State      string  `vgi:"name=state,default=open,choices='open,closed,any',doc=Which connections: 'open' (default), 'closed' (recently closed, kept by the server) or 'any'."`
	Sort       *string `vgi:"name=sort,choices='cid,start,subs,pending,msgs_to,msgs_from,bytes_to,bytes_from,last,idle,uptime,stop,reason,rtt',doc=Server-side sort order before paging, e.g. 'bytes_to' for the heaviest receivers first (stop/reason need state 'closed')."`
	Account    *string `vgi:"name=account,doc=Only connections bound to this account."`
	User       *string `vgi:"name=user,doc=Only connections authenticated as this user."`
	Cid        *int64  `vgi:"name=cid,ge=1,doc=Only the connection with this connection id."`
	Subject    *string `vgi:"name=subject,doc=Only connections with a subscription that receives this subject (wildcard subscriptions are matched)."`
	Offset     int64   `vgi:"name=offset,default=0,ge=0,doc=Skip this many connections per server (server-side paging)."`
	MaxRows    *int64  `vgi:"name=max_rows,ge=1,doc=At most this many connections per server (server-side limit)."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a connectionsArgs) secret() string { return deref(a.SecretName) }

var connectionsColumns = withServer(
	column("cid", i64, "Connection id, unique per server.", false),
	column("kind", str, "Connection kind: 'Client', 'Leafnode', ...", true),
	column("type", str, "Client type: 'nats', 'mqtt', 'websocket'.", true),
	column("name", str, "Name the client gave on connect.", true),
	column("ip", str, "Remote IP address.", true),
	column("port", i64, "Remote port.", true),
	column("account", str, "Account the connection is bound to.", true),
	column("authorized_user", str, "Authenticated user name.", true),
	column("lang", str, "Client library language.", true),
	column("client_version", str, "Client library version.", true),
	column("start_time", tsT, "When the connection was established.", true),
	column("last_activity", tsT, "Time of the last message in either direction.", true),
	column("stop_time", tsT, "When a closed connection closed; NULL when open.", true),
	column("reason", str, "Why a closed connection closed; NULL when open.", true),
	column("uptime_seconds", f64, "Connection age in seconds (server clock).", true),
	column("idle_seconds", f64, "Seconds since last_activity (server clock).", true),
	column("rtt_seconds", f64, "Last measured round-trip time in seconds.", true),
	column("pending_bytes", i64, "Bytes queued to the client but not yet sent.", true),
	column("in_msgs", i64, "Messages received from the client.", true),
	column("out_msgs", i64, "Messages sent to the client.", true),
	column("in_bytes", i64, "Bytes received from the client.", true),
	column("out_bytes", i64, "Bytes sent to the client.", true),
	column("subscriptions", i64, "Number of subscriptions.", true),
	column("subscriptions_list", strLst, "The connection's subscription subjects.", false),
	column("tls_version", str, "TLS version; NULL for plaintext.", true),
	column("tls_cipher_suite", str, "TLS cipher suite; NULL for plaintext.", true),
	column("mqtt_client", str, "MQTT client id for MQTT connections.", true),
)

var connectionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT name, account, ip, in_msgs, out_msgs FROM nats.main.connections(sort := 'msgs_from', max_rows := 20) ORDER BY in_msgs DESC",
		Description: "The 20 connections that published the most messages"},
	{SQL: "SELECT account, count(*) AS conns, sum(pending_bytes) AS pending FROM nats.main.connections() GROUP BY account ORDER BY conns DESC",
		Description: "Open connections and pending bytes per account"},
}

func connState(s string) (http string, sys int) {
	switch s {
	case "closed":
		return "closed", 1
	case "any":
		return "any", 2
	}
	return "open", 0
}

func newConnections() vgi.TableFunction {
	return newSimple(&simpleFunc[connectionsArgs]{
		name: "connections", columns: connectionsColumns,
		meta: meta("Client Connections", "monitoring", "connz, connections, clients, slow consumers, pending bytes, rtt, closed connections",
			"One row per client connection per server from /connz. Pushed down to the server: state ('open', 'closed', 'any'), "+
				"sort (cid, start, subs, pending, msgs_to, msgs_from, bytes_to, bytes_from, last, idle, uptime, stop, reason, rtt), "+
				"account, user, cid, offset and max_rows (per server). subject keeps connections with a subscription that receives "+
				"that subject. Columns include name, account, authorized_user, ip, start_time, last_activity, rtt_seconds, "+
				"pending_bytes, in/out msgs and bytes, subscriptions_list. Pages of 1024 are fetched until done."+monitoringDocLLM,
			"## connections(state := 'open', sort, account, user, cid, subject, offset := 0, max_rows)\n\n"+
				"Client connections (`/connz`) with filters, sorting and paging done by the server.",
			"Client connections with server-side state, account, user, sort and paging filters (/connz)", connectionsColumns, connectionsExamples),
		fetch: func(ctx context.Context, c *config, a connectionsArgs, cursor string) (page, error) {
			offset := a.Offset
			if cursor != "" {
				offset, _ = strconv.ParseInt(cursor, 10, 64)
			}
			size := int64(ConnzPage)
			if a.MaxRows != nil {
				size = min(size, *a.MaxRows-(offset-a.Offset))
			}
			httpState, sysState := connState(a.State)
			q := url.Values{"subs": {"1"}, "auth": {"1"}, "state": {httpState},
				"offset": {strconv.FormatInt(offset, 10)}, "limit": {strconv.FormatInt(size, 10)}}
			body := map[string]any{"subscriptions": true, "auth": true, "state": sysState, "offset": offset, "limit": size}
			if a.Sort != nil {
				q.Set("sort", *a.Sort)
				body["sort"] = *a.Sort
			}
			if a.Account != nil {
				q.Set("acc", *a.Account)
				body["acc"] = *a.Account
				if a.Subject != nil {
					body["filter_subject"] = *a.Subject
				}
			}
			if a.User != nil {
				q.Set("user", *a.User)
				body["user"] = *a.User
			}
			if a.Cid != nil {
				q.Set("cid", strconv.FormatInt(*a.Cid, 10))
				body["cid"] = *a.Cid
			}
			reps, err := monitor(ctx, c, monRequest{Path: "connz", Query: q, SysSubject: ping("CONNZ"), SysBody: body})
			if err != nil {
				return page{}, err
			}
			var rows [][]any
			more := false
			for _, r := range reps {
				var z connz
				if err := decode(r, &z); err != nil {
					return page{}, err
				}
				if int64(len(z.Conns)) >= size && offset+int64(len(z.Conns)) < z.Total {
					more = true
				}
				for _, ci := range z.Conns {
					if a.Subject != nil && !anyMatch(ci.SubsList, *a.Subject) {
						continue
					}
					var up, idle any
					if !z.Now.IsZero() && !ci.Start.IsZero() {
						end := z.Now
						if ci.Stop != nil && !ci.Stop.IsZero() {
							end = *ci.Stop
						}
						up = end.Sub(ci.Start).Seconds()
					}
					if !z.Now.IsZero() && !ci.LastActivity.IsZero() {
						idle = z.Now.Sub(ci.LastActivity).Seconds()
					}
					rows = append(rows, append(srv(r), ci.Cid, optString(ci.Kind), optString(ci.Type), optString(ci.Name),
						optString(ci.IP), ci.Port, optString(ci.Account), optString(ci.AuthorizedUser), optString(ci.Lang),
						optString(ci.Version), optTime(ci.Start), optTime(ci.LastActivity), optTimePtr(ci.Stop),
						optString(ci.Reason), up, idle, parseRTT(ci.RTT), ci.PendingBytes, ci.InMsgs, ci.OutMsgs, ci.InBytes,
						ci.OutBytes, ci.Subscriptions, strList(ci.SubsList), optString(ci.TLSVersion), optString(ci.TLSCipher),
						optString(ci.MQTTClient)))
				}
			}
			next := ""
			if more && size > 0 {
				next = strconv.FormatInt(offset+size, 10)
			}
			return page{Rows: rows, Next: next}, nil
		},
	})
}

func anyMatch(subs []string, subject string) bool {
	for _, s := range subs {
		if subjectMatches(s, subject) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- routes

type routeInfo struct {
	Rid          int64     `json:"rid"`
	RemoteID     string    `json:"remote_id"`
	RemoteName   string    `json:"remote_name"`
	DidSolicit   bool      `json:"did_solicit"`
	IsConfigured bool      `json:"is_configured"`
	IP           string    `json:"ip"`
	Port         int64     `json:"port"`
	Start        time.Time `json:"start"`
	LastActivity time.Time `json:"last_activity"`
	RTT          string    `json:"rtt"`
	PendingSize  int64     `json:"pending_size"`
	InMsgs       int64     `json:"in_msgs"`
	OutMsgs      int64     `json:"out_msgs"`
	InBytes      int64     `json:"in_bytes"`
	OutBytes     int64     `json:"out_bytes"`
	NumSubs      int64     `json:"subscriptions"`
	Account      string    `json:"account"`
	Compression  string    `json:"compression"`
}

var routesColumns = withServer(
	column("rid", i64, "Route id on this server.", false),
	column("remote_id", str, "Server ID of the peer.", true),
	column("remote_name", str, "Server name of the peer.", true),
	column("did_solicit", boolT, "Whether this server initiated the route.", false),
	column("is_configured", boolT, "Whether the route is in the config (rather than gossiped).", false),
	column("ip", str, "Peer IP address.", true),
	column("port", i64, "Peer port.", true),
	column("start_time", tsT, "When the route was established.", true),
	column("last_activity", tsT, "Time of the last message on the route.", true),
	column("rtt_seconds", f64, "Round-trip time in seconds.", true),
	column("pending_bytes", i64, "Bytes queued to the peer.", true),
	column("in_msgs", i64, "Messages received over the route.", true),
	column("out_msgs", i64, "Messages sent over the route.", true),
	column("in_bytes", i64, "Bytes received over the route.", true),
	column("out_bytes", i64, "Bytes sent over the route.", true),
	column("subscriptions", i64, "Interest (subscriptions) propagated over the route.", true),
	column("account", str, "Pinned account for a dedicated per-account route; NULL for shared routes.", true),
	column("compression", str, "Route compression mode.", true),
)

var routesExamples = []vgi.CatalogExample{
	{SQL: "SELECT server_name, remote_name, rtt_seconds * 1000 AS rtt_ms, pending_bytes FROM nats.main.routes() ORDER BY rtt_ms DESC",
		Description: "Cluster routes by round-trip time"},
	{SQL: "SELECT server_name, count(*) AS routes FROM nats.main.routes() GROUP BY server_name ORDER BY server_name",
		Description: "How many routes each server has (a full mesh has n-1 or more)"},
}

func newRoutes() vgi.TableFunction {
	return newSimple(&simpleFunc[monArgs]{
		name: "routes", columns: routesColumns,
		meta: meta("Cluster Routes", "monitoring", "routez, routes, cluster, mesh, peers, rtt",
			"One row per cluster route per server from /routez: peer remote_id/remote_name, whether solicited or configured, "+
				"ip/port, start_time, last_activity, rtt_seconds, pending_bytes, in/out msgs and bytes, subscriptions, pinned "+
				"account, compression. Empty on a single server. Named arg: secret_name."+monitoringDocLLM,
			"## routes()\n\nCluster routes (`/routez`).",
			"Cluster routes between servers with RTT and traffic (/routez)", routesColumns, routesExamples),
		fetch: single(func(ctx context.Context, c *config, _ monArgs) ([][]any, error) {
			reps, err := monitor(ctx, c, monRequest{Path: "routez", SysSubject: ping("ROUTEZ")})
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z struct {
					Routes []routeInfo `json:"routes"`
				}
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				for _, ri := range z.Routes {
					rows = append(rows, append(srv(r), ri.Rid, optString(ri.RemoteID), optString(ri.RemoteName), ri.DidSolicit,
						ri.IsConfigured, optString(ri.IP), ri.Port, optTime(ri.Start), optTime(ri.LastActivity), parseRTT(ri.RTT),
						ri.PendingSize, ri.InMsgs, ri.OutMsgs, ri.InBytes, ri.OutBytes, ri.NumSubs, optString(ri.Account),
						optString(ri.Compression)))
				}
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- gateways

type remoteGateway struct {
	Configured bool      `json:"configured"`
	Connection *connInfo `json:"connection"`
}

var gatewaysColumns = withServer(
	column("gateway", str, "This server's gateway (cluster) name.", true),
	column("direction", str, "'outbound' or 'inbound'.", false),
	column("remote_gateway", str, "Name of the remote gateway (cluster).", false),
	column("configured", boolT, "Whether the remote gateway is in the config (rather than discovered).", false),
	column("cid", i64, "Connection id of the gateway connection.", true),
	column("remote_server", str, "Server name of the remote end.", true),
	column("ip", str, "Remote IP address.", true),
	column("port", i64, "Remote port.", true),
	column("start_time", tsT, "When the gateway connection was established.", true),
	column("last_activity", tsT, "Time of the last message on the connection.", true),
	column("rtt_seconds", f64, "Round-trip time in seconds.", true),
	column("in_msgs", i64, "Messages received.", true),
	column("out_msgs", i64, "Messages sent.", true),
	column("in_bytes", i64, "Bytes received.", true),
	column("out_bytes", i64, "Bytes sent.", true),
	column("subscriptions", i64, "Subscriptions on the connection.", true),
)

var gatewaysExamples = []vgi.CatalogExample{
	{SQL: "SELECT server_name, direction, remote_gateway, rtt_seconds FROM nats.main.gateways() ORDER BY server_name, direction, remote_gateway",
		Description: "Super-cluster gateway connections of every server"},
	{SQL: "SELECT remote_gateway, sum(out_bytes) AS bytes_sent FROM nats.main.gateways() WHERE direction = 'outbound' GROUP BY remote_gateway ORDER BY bytes_sent DESC",
		Description: "Bytes sent to each remote cluster"},
}

func newGateways() vgi.TableFunction {
	return newSimple(&simpleFunc[monArgs]{
		name: "gateways", columns: gatewaysColumns,
		meta: meta("Super-Cluster Gateways", "monitoring", "gatewayz, gateways, super-cluster, inbound, outbound",
			"One row per gateway connection per server from /gatewayz: direction ('outbound', 'inbound'), remote_gateway, "+
				"configured, the connection's cid, remote_server, ip/port, times, rtt_seconds and traffic. Empty without a "+
				"super-cluster. Named arg: secret_name."+monitoringDocLLM,
			"## gateways()\n\nSuper-cluster gateway connections (`/gatewayz`).",
			"Outbound and inbound super-cluster gateway connections (/gatewayz)", gatewaysColumns, gatewaysExamples),
		fetch: single(func(ctx context.Context, c *config, _ monArgs) ([][]any, error) {
			reps, err := monitor(ctx, c, monRequest{Path: "gatewayz", SysSubject: ping("GATEWAYZ")})
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z struct {
					Name     string                     `json:"name"`
					Outbound map[string]remoteGateway   `json:"outbound_gateways"`
					Inbound  map[string][]remoteGateway `json:"inbound_gateways"`
				}
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				add := func(dir, name string, g remoteGateway) {
					row := append(srv(r), optString(z.Name), dir, name, g.Configured)
					if ci := g.Connection; ci != nil {
						row = append(row, ci.Cid, optString(ci.Name), optString(ci.IP), ci.Port, optTime(ci.Start),
							optTime(ci.LastActivity), parseRTT(ci.RTT), ci.InMsgs, ci.OutMsgs, ci.InBytes, ci.OutBytes, ci.Subscriptions)
					} else {
						row = append(row, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					}
					rows = append(rows, row)
				}
				for _, name := range sortedKeys(z.Outbound) {
					add("outbound", name, z.Outbound[name])
				}
				for _, name := range sortedKeys(z.Inbound) {
					for _, g := range z.Inbound[name] {
						add("inbound", name, g)
					}
				}
			}
			return rows, nil
		}),
	})
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------- leafnodes

type leafArgs struct {
	Account    *string `vgi:"name=account,doc=Only leaf-node connections bound to this account."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a leafArgs) secret() string { return deref(a.SecretName) }

var leafColumns = withServer(
	column("leaf_id", i64, "Connection id of the leaf-node connection.", true),
	column("name", str, "Server name of the remote end.", true),
	column("is_spoke", boolT, "Whether the remote is a spoke (it connected to this hub).", false),
	column("account", str, "Account the leaf-node connection is bound to.", true),
	column("ip", str, "Remote IP address.", true),
	column("port", i64, "Remote port.", true),
	column("rtt_seconds", f64, "Round-trip time in seconds.", true),
	column("in_msgs", i64, "Messages received.", true),
	column("out_msgs", i64, "Messages sent.", true),
	column("in_bytes", i64, "Bytes received.", true),
	column("out_bytes", i64, "Bytes sent.", true),
	column("subscriptions", i64, "Subscriptions (interest) over the connection.", true),
	column("compression", str, "Compression mode.", true),
)

var leafExamples = []vgi.CatalogExample{
	{SQL: "SELECT server_name, name, account, rtt_seconds FROM nats.main.leafnodes() ORDER BY server_name, name",
		Description: "Leaf-node connections and their accounts"},
	{SQL: "SELECT account, count(*) AS leafs FROM nats.main.leafnodes() GROUP BY account ORDER BY leafs DESC",
		Description: "Leaf-node connections per account"},
}

func newLeafnodes() vgi.TableFunction {
	return newSimple(&simpleFunc[leafArgs]{
		name: "leafnodes", columns: leafColumns,
		meta: meta("Leaf-Node Connections", "monitoring", "leafz, leafnodes, leaf nodes, edge, hub, spoke",
			"One row per leaf-node connection per server from /leafz: remote name, is_spoke, account, ip/port, rtt_seconds, "+
				"traffic, subscriptions, compression. Named args: account (pushed down), secret_name."+monitoringDocLLM,
			"## leafnodes(account := NULL)\n\nLeaf-node connections (`/leafz`).",
			"Leaf-node connections with account, RTT and traffic (/leafz)", leafColumns, leafExamples),
		fetch: single(func(ctx context.Context, c *config, a leafArgs) ([][]any, error) {
			q := url.Values{}
			body := map[string]any{}
			if a.Account != nil {
				q.Set("acc", *a.Account)
				body["account"] = *a.Account
			}
			reps, err := monitor(ctx, c, monRequest{Path: "leafz", Query: q, SysSubject: ping("LEAFZ"), SysBody: body})
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z struct {
					Leafs []struct {
						ID          int64  `json:"id"`
						Name        string `json:"name"`
						IsSpoke     bool   `json:"is_spoke"`
						Account     string `json:"account"`
						IP          string `json:"ip"`
						Port        int64  `json:"port"`
						RTT         string `json:"rtt"`
						InMsgs      int64  `json:"in_msgs"`
						OutMsgs     int64  `json:"out_msgs"`
						InBytes     int64  `json:"in_bytes"`
						OutBytes    int64  `json:"out_bytes"`
						NumSubs     int64  `json:"subscriptions"`
						Compression string `json:"compression"`
					} `json:"leafs"`
				}
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				for _, l := range z.Leafs {
					rows = append(rows, append(srv(r), l.ID, optString(l.Name), l.IsSpoke, optString(l.Account), optString(l.IP),
						l.Port, parseRTT(l.RTT), l.InMsgs, l.OutMsgs, l.InBytes, l.OutBytes, l.NumSubs, optString(l.Compression)))
				}
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- subscriptions

// SubszPage is the page size requested from /subsz.
var SubszPage = 1024

type subsArgs struct {
	Account     *string `vgi:"name=account,doc=Only subscriptions in this account."`
	TestSubject *string `vgi:"name=test_subject,doc=Only subscriptions that would receive a message published on this literal subject (server-side match)."`
	SecretName  *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a subsArgs) secret() string { return deref(a.SecretName) }

var subsColumns = withServer(
	column("account", str, "Account the subscription belongs to.", true),
	column("subject", str, "Subscribed subject, possibly with * and > wildcards.", false),
	column("queue_group", str, "Queue group; NULL for a plain subscription.", true),
	column("sid", str, "Subscription id on its connection.", true),
	column("msgs", i64, "Messages delivered to the subscription.", true),
	column("max", i64, "Auto-unsubscribe limit; NULL when none.", true),
	column("cid", i64, "Connection id owning the subscription.", true),
)

var subsExamples = []vgi.CatalogExample{
	{SQL: "SELECT account, subject, queue_group, msgs FROM nats.main.subscriptions(test_subject := 'orders.eu.created') ORDER BY account, subject",
		Description: "Who receives a message published on orders.eu.created"},
	{SQL: "SELECT account, count(*) AS subs FROM nats.main.subscriptions() GROUP BY account ORDER BY subs DESC",
		Description: "Subscriptions per account"},
}

func newSubscriptions() vgi.TableFunction {
	return newSimple(&simpleFunc[subsArgs]{
		name: "subscriptions", columns: subsColumns,
		meta: meta("Subscriptions", "monitoring", "subsz, subscriptions, interest, queue groups, fanout, who receives",
			"One row per subscription per server from /subsz?subs=1: account, subject, queue_group, sid, msgs, max, cid. "+
				"Pushed down: account and test_subject (only subscriptions that would receive a message on that literal "+
				"subject: the server's own interest match). Pages of 1024 are fetched until done. Named arg: secret_name."+monitoringDocLLM,
			"## subscriptions(account := NULL, test_subject := NULL)\n\nThe subscription interest graph (`/subsz`).",
			"Subscriptions with account and server-side 'who receives this subject' matching (/subsz)", subsColumns, subsExamples),
		fetch: func(ctx context.Context, c *config, a subsArgs, cursor string) (page, error) {
			offset, _ := strconv.ParseInt(cursor, 10, 64)
			q := url.Values{"subs": {"1"}, "offset": {strconv.FormatInt(offset, 10)}, "limit": {strconv.Itoa(SubszPage)}}
			body := map[string]any{"subscriptions": true, "offset": offset, "limit": SubszPage}
			if a.Account != nil {
				q.Set("acc", *a.Account)
				body["account"] = *a.Account
			}
			if a.TestSubject != nil {
				q.Set("test", *a.TestSubject)
				body["test"] = *a.TestSubject
			}
			reps, err := monitor(ctx, c, monRequest{Path: "subsz", Query: q, SysSubject: ping("SUBSZ"), SysBody: body})
			if err != nil {
				return page{}, err
			}
			var rows [][]any
			more := false
			for _, r := range reps {
				var z struct {
					Total int64 `json:"total"`
					Subs  []struct {
						Account string `json:"account"`
						Subject string `json:"subject"`
						Queue   string `json:"qgroup"`
						Sid     string `json:"sid"`
						Msgs    int64  `json:"msgs"`
						Max     int64  `json:"max"`
						Cid     int64  `json:"cid"`
					} `json:"subscriptions_list"`
				}
				if err := decode(r, &z); err != nil {
					return page{}, err
				}
				if len(z.Subs) >= SubszPage && offset+int64(len(z.Subs)) < z.Total {
					more = true
				}
				for _, s := range z.Subs {
					rows = append(rows, append(srv(r), optString(s.Account), s.Subject, optString(s.Queue), optString(s.Sid),
						s.Msgs, optInt(s.Max), s.Cid))
				}
			}
			next := ""
			if more {
				next = strconv.FormatInt(offset+int64(SubszPage), 10)
			}
			return page{Rows: rows, Next: next}, nil
		},
	})
}

var subStatsColumns = withServer(
	column("num_subscriptions", i64, "Subscriptions in the interest graph.", false),
	column("num_cache", i64, "Entries in the subject match cache.", false),
	column("num_inserts", i64, "Subscriptions inserted since start.", false),
	column("num_removes", i64, "Subscriptions removed since start.", false),
	column("num_matches", i64, "Subject match operations since start.", false),
	column("cache_hit_rate", f64, "Fraction of matches served from the cache.", true),
	column("max_fanout", i64, "Largest number of subscribers one message matched.", true),
	column("avg_fanout", f64, "Average number of subscribers per message.", true),
)

var subStatsExamples = []vgi.CatalogExample{
	{SQL: "SELECT server_name, num_subscriptions, round(cache_hit_rate, 3) AS hit_rate, max_fanout FROM nats.main.subscription_stats() ORDER BY server_name",
		Description: "Interest-graph size and match-cache efficiency per server"},
	{SQL: "SELECT sum(num_subscriptions) AS subs FROM nats.main.subscription_stats()",
		Description: "Total subscriptions across the cluster"},
}

func newSubscriptionStats() vgi.TableFunction {
	return newSimple(&simpleFunc[monArgs]{
		name: "subscription_stats", columns: subStatsColumns,
		meta: meta("Subscription Statistics", "monitoring", "subsz, sublist, fanout, match cache, interest graph",
			"One row per server from /subsz: num_subscriptions, cache size and hit rate, inserts, removes, matches, max and "+
				"average fanout. Named arg: secret_name."+monitoringDocLLM,
			"## subscription_stats()\n\nInterest-graph statistics (`/subsz`).",
			"Subscription interest-graph and match-cache statistics per server (/subsz)", subStatsColumns, subStatsExamples),
		fetch: single(func(ctx context.Context, c *config, _ monArgs) ([][]any, error) {
			reps, err := monitor(ctx, c, monRequest{Path: "subsz", SysSubject: ping("SUBSZ")})
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z struct {
					NumSubs   int64   `json:"num_subscriptions"`
					NumCache  int64   `json:"num_cache"`
					Inserts   int64   `json:"num_inserts"`
					Removes   int64   `json:"num_removes"`
					Matches   int64   `json:"num_matches"`
					HitRate   float64 `json:"cache_hit_rate"`
					MaxFanout int64   `json:"max_fanout"`
					AvgFanout float64 `json:"avg_fanout"`
				}
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				rows = append(rows, append(srv(r), z.NumSubs, z.NumCache, z.Inserts, z.Removes, z.Matches, z.HitRate, z.MaxFanout, z.AvgFanout))
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- accounts

var accountsColumns = withServer(
	column("account", str, "Account name (or public key in decentralized auth).", false),
	column("name_tag", str, "Human-readable account name from its JWT.", true),
	column("is_system", boolT, "Whether this is the system account.", false),
	column("jetstream_enabled", boolT, "Whether JetStream is enabled for the account.", true),
	column("client_connections", i64, "Client connections bound to the account on this server.", true),
	column("leafnode_connections", i64, "Leaf-node connections bound to the account on this server.", true),
	column("subscriptions", i64, "Subscriptions in the account.", true),
	column("expired", boolT, "Whether the account JWT has expired.", true),
	column("complete", boolT, "Whether the account's configuration was fully resolved.", true),
	column("imports", i64, "Number of stream and service imports.", true),
	column("exports", i64, "Number of stream and service exports.", true),
	column("update_time", tsT, "When the account was last updated.", true),
	column("detail", str, "The account's complete /accountz detail document as JSON (imports, exports, JWT claims).", true),
)

var accountsExamples = []vgi.CatalogExample{
	{SQL: "SELECT account, is_system, jetstream_enabled, client_connections FROM nats.main.accounts() ORDER BY account",
		Description: "Every account, which one is the system account and where JetStream is on"},
	{SQL: "SELECT account, imports, exports FROM nats.main.accounts() WHERE imports > 0 OR exports > 0 ORDER BY account",
		Description: "Accounts that import or export streams or services"},
}

func newAccounts() vgi.TableFunction {
	return newSimple(&simpleFunc[monArgs]{
		name: "accounts", columns: accountsColumns,
		meta: meta("Accounts", "monitoring", "accountz, accounts, tenants, imports, exports, system account, multi-tenancy",
			"One row per account per server from /accountz plus one /accountz?acc=<name> detail request per account: is_system, "+
				"jetstream_enabled, client and leaf-node connections, subscriptions, expired, complete, import and export counts, "+
				"update_time, and the detail document as JSON. Named arg: secret_name."+monitoringDocLLM,
			"## accounts()\n\nAccounts known to each server (`/accountz`).",
			"Accounts with connections, JetStream status, imports and exports (/accountz)", accountsColumns, accountsExamples),
		fetch: single(func(ctx context.Context, c *config, _ monArgs) ([][]any, error) {
			reps, err := monitor(ctx, c, monRequest{Path: "accountz", SysSubject: ping("ACCOUNTZ"), SysBody: map[string]any{}})
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z struct {
					System   string   `json:"system_account"`
					Accounts []string `json:"accounts"`
				}
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				sort.Strings(z.Accounts)
				for _, acc := range z.Accounts {
					row := append(srv(r), acc)
					det, err := accountDetail(ctx, c, r, acc)
					if err != nil {
						return nil, err
					}
					if det == nil {
						row = append(row, nil, acc == z.System, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					} else {
						row = append(row, optString(det.NameTag), acc == z.System, det.JetStream, det.ClientConns, det.LeafConns,
							det.Subs, det.Expired, det.Complete, int64(len(det.Imports)), int64(len(det.Exports)),
							optTime(det.Updated), det.raw)
					}
					rows = append(rows, row)
				}
			}
			return rows, nil
		}),
	})
}

type accDetail struct {
	NameTag     string            `json:"name_tag"`
	JetStream   bool              `json:"jetstream_enabled"`
	ClientConns int64             `json:"client_connections"`
	LeafConns   int64             `json:"leafnode_connections"`
	Subs        int64             `json:"subscriptions"`
	Expired     bool              `json:"expired"`
	Complete    bool              `json:"complete"`
	Imports     []json.RawMessage `json:"imports"`
	Exports     []json.RawMessage `json:"exports"`
	Updated     time.Time         `json:"update_time"`
	raw         string
}

// accountDetail fetches one account's detail from the same server.
func accountDetail(ctx context.Context, c *config, from monReply, acc string) (*accDetail, error) {
	req := monRequest{Path: "accountz", Query: url.Values{"acc": {acc}},
		SysSubject: "$SYS.REQ.SERVER." + from.ServerID + ".ACCOUNTZ", SysBody: map[string]any{"account": acc}}
	var reps []monReply
	var err error
	if len(c.MonitoringURL) > 0 {
		// The detail must come from the server that listed the account.
		for _, base := range c.MonitoringURL {
			body, gerr := httpGet(ctx, base, "accountz", req.Query)
			if gerr != nil {
				return nil, gerr
			}
			var ids struct {
				ServerID string `json:"server_id"`
			}
			_ = json.Unmarshal(body, &ids)
			if ids.ServerID == from.ServerID || len(c.MonitoringURL) == 1 {
				reps = []monReply{{ServerID: ids.ServerID, Data: body}}
				break
			}
		}
	} else {
		reps, err = sysPing(ctx, c, req)
		if err != nil {
			return nil, err
		}
	}
	if len(reps) == 0 {
		return nil, nil
	}
	var z struct {
		Detail json.RawMessage `json:"account_detail"`
	}
	if err := decode(reps[0], &z); err != nil {
		return nil, err
	}
	if len(z.Detail) == 0 || string(z.Detail) == "null" {
		return nil, nil
	}
	var d accDetail
	if err := json.Unmarshal(z.Detail, &d); err != nil {
		return nil, err
	}
	d.raw = string(z.Detail)
	return &d, nil
}

// ---------------------------------------------------------------- account_stats

type accStatsArgs struct {
	IncludeUnused bool    `vgi:"name=include_unused,default=true,doc=Include accounts with no connections on a server (default true)."`
	SecretName    *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a accStatsArgs) secret() string { return deref(a.SecretName) }

var accStatsColumns = withServer(
	column("account", str, "Account name (or public key).", false),
	column("name", str, "Account display name.", true),
	column("connections", i64, "Current client connections on this server.", true),
	column("leafnodes", i64, "Current leaf-node connections on this server.", true),
	column("total_connections", i64, "Client plus leaf-node connections.", true),
	column("subscriptions", i64, "Subscriptions in the account on this server.", true),
	column("sent_msgs", i64, "Messages the server delivered to the account's clients.", true),
	column("sent_bytes", i64, "Bytes the server delivered to the account's clients.", true),
	column("received_msgs", i64, "Messages the account's clients published.", true),
	column("received_bytes", i64, "Bytes the account's clients published.", true),
	column("slow_consumers", i64, "Slow-consumer events in the account.", true),
)

var accStatsExamples = []vgi.CatalogExample{
	{SQL: "SELECT account, sum(received_msgs) AS published, sum(sent_msgs) AS delivered FROM nats.main.account_stats() GROUP BY account ORDER BY published DESC",
		Description: "Messages published and delivered per account across the cluster"},
	{SQL: "SELECT account, connections FROM nats.main.account_stats(include_unused := false) ORDER BY connections DESC",
		Description: "Accounts with at least one connection"},
}

func newAccountStats() vgi.TableFunction {
	return newSimple(&simpleFunc[accStatsArgs]{
		name: "account_stats", columns: accStatsColumns,
		meta: meta("Account Statistics", "monitoring", "accstatz, account stats, per-account traffic, tenants, usage",
			"One row per account per server from /accstatz: connections, leafnodes, subscriptions, sent and received msgs and "+
				"bytes, slow consumers. Named args: include_unused (default true; pushed down), secret_name. In $SYS mode this "+
				"uses $SYS.REQ.ACCOUNT.PING.STATZ."+monitoringDocLLM,
			"## account_stats(include_unused := true)\n\nPer-account traffic counters (`/accstatz`).",
			"Per-account connection and message counters (/accstatz)", accStatsColumns, accStatsExamples),
		fetch: single(func(ctx context.Context, c *config, a accStatsArgs) ([][]any, error) {
			q := url.Values{}
			if a.IncludeUnused {
				q.Set("unused", "1")
			}
			reps, err := monitor(ctx, c, monRequest{Path: "accstatz", Query: q, SysSubject: "$SYS.REQ.ACCOUNT.PING.STATZ",
				SysBody: map[string]any{"accounts": []string{}, "include_unused": a.IncludeUnused}})
			if err != nil {
				return nil, err
			}
			type counts struct {
				Msgs  int64 `json:"msgs"`
				Bytes int64 `json:"bytes"`
			}
			var rows [][]any
			for _, r := range reps {
				var z struct {
					Accounts []struct {
						Acc        string `json:"acc"`
						Name       string `json:"name"`
						Conns      int64  `json:"conns"`
						Leafs      int64  `json:"leafnodes"`
						TotalConns int64  `json:"total_conns"`
						NumSubs    int64  `json:"num_subscriptions"`
						Sent       counts `json:"sent"`
						Received   counts `json:"received"`
						Slow       int64  `json:"slow_consumers"`
					} `json:"account_statz"`
				}
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				sort.Slice(z.Accounts, func(i, j int) bool { return z.Accounts[i].Acc < z.Accounts[j].Acc })
				for _, s := range z.Accounts {
					rows = append(rows, append(srv(r), s.Acc, optString(s.Name), s.Conns, s.Leafs, s.TotalConns, s.NumSubs,
						s.Sent.Msgs, s.Sent.Bytes, s.Received.Msgs, s.Received.Bytes, s.Slow))
				}
			}
			return rows, nil
		}),
	})
}

// ---------------------------------------------------------------- health

type healthArgs struct {
	JSEnabledOnly bool    `vgi:"name=js_enabled_only,default=false,doc=Only check that JetStream is enabled (skip stream and consumer health)."`
	JSServerOnly  bool    `vgi:"name=js_server_only,default=false,doc=Check JetStream server health but not individual streams and consumers."`
	SecretName    *string `vgi:"name=secret_name,doc=Name of the nats secret to use when several exist."`
}

func (a healthArgs) secret() string { return deref(a.SecretName) }

var healthColumns = withServer(
	column("status", str, "'ok', 'unavailable' or 'error'.", false),
	column("healthy", boolT, "status = 'ok'.", false),
	column("status_code", i64, "HTTP-style status code when unhealthy (e.g. 503).", true),
	column("error", str, "Why the server is unhealthy; NULL when healthy.", true),
)

var healthExamples = []vgi.CatalogExample{
	{SQL: "SELECT server_name, status, error FROM nats.main.health() ORDER BY server_name",
		Description: "Health check of every server"},
	{SQL: "SELECT count(*) FILTER (WHERE NOT healthy) AS unhealthy FROM nats.main.health(js_server_only := true)",
		Description: "How many servers fail the JetStream server health check"},
}

func newHealth() vgi.TableFunction {
	return newSimple(&simpleFunc[healthArgs]{
		name: "health", columns: healthColumns,
		meta: meta("Health Check", "monitoring", "healthz, health, readiness, liveness, jetstream health",
			"One row per server from /healthz: status ('ok' when healthy), healthy BOOLEAN, status_code and error. By default "+
				"the check includes JetStream streams and consumers; js_enabled_only or js_server_only narrow it (pushed down). "+
				"Named arg: secret_name."+monitoringDocLLM,
			"## health(js_enabled_only := false, js_server_only := false)\n\nServer and JetStream health (`/healthz`).",
			"Server and JetStream health check per server (/healthz)", healthColumns, healthExamples),
		fetch: single(func(ctx context.Context, c *config, a healthArgs) ([][]any, error) {
			q := url.Values{}
			body := map[string]any{}
			if a.JSEnabledOnly {
				q.Set("js-enabled-only", "true")
				body["js-enabled-only"] = true
			}
			if a.JSServerOnly {
				q.Set("js-server-only", "true")
				body["js-server-only"] = true
			}
			reps, err := monitor(ctx, c, monRequest{Path: "healthz", Query: q, SysSubject: ping("HEALTHZ"), SysBody: body})
			if err != nil {
				return nil, err
			}
			var rows [][]any
			for _, r := range reps {
				var z struct {
					Status     string `json:"status"`
					StatusCode int64  `json:"status_code"`
					Error      string `json:"error"`
				}
				if err := decode(r, &z); err != nil {
					return nil, err
				}
				rows = append(rows, append(srv(r), z.Status, z.Status == "ok", optInt(z.StatusCode), optString(z.Error)))
			}
			return rows, nil
		}),
	})
}
