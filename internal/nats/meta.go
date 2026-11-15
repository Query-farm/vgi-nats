// Discovery metadata: the one file to edit when you rename or extend the worker.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.
// Keep every object documented and `make lint-explain` stays at 100.
//
//   - Catalog-level tags (title, license, author, links, long description)
//     live in CatalogTags below.
//   - Every function carries objectTags(...): a title, a category from
//     SchemaCategories, search keywords, and LLM- and human-facing docs.

package nats

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "nats"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-nats"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "monitoring", Title: "Server Monitoring", Description: "Servers, connections, routes, gateways, leaf nodes, subscriptions, accounts, JetStream usage and health, from the HTTP monitoring endpoints or the $SYS API."},
	{Name: "jetstream", Title: "JetStream Streams", Description: "Account usage, streams, subjects, consumers with lag, and non-destructive message reads."},
	{Name: "kv", Title: "Key-Value Stores", Description: "JetStream KV buckets, current values and revision history."},
	{Name: "objects", Title: "Object Stores", Description: "JetStream object stores and the objects in them."},
	{Name: "subjects", Title: "Subject Tools", Description: "Offline NATS subject wildcard matching."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// docs is the set of standard per-object discovery fields.
type docs struct {
	Title    string
	Category string
	Keywords string
	DocLLM   string
	DocMD    string
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d docs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// firstExamples keeps the first example of each list (the schema-level
// vgi.example_queries tag must fit an agent tool's context).
func firstExamples(lists ...[]vgi.CatalogExample) []vgi.CatalogExample {
	out := []vgi.CatalogExample{}
	for _, l := range lists {
		if len(l) > 0 {
			out = append(out, l[0])
		}
	}
	return out
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
// Overloads share one SQL name, so pass every overload's examples.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

const catalogDocLLM = "Query NATS from SQL, read-only: the servers' monitoring data and the JetStream streams, " +
	"consumers, messages, KV buckets and object stores of one account. Every server function needs a DuckDB secret: " +
	"CREATE SECRET (TYPE nats, SERVERS 'nats://host:4222' (comma-separated for a cluster), one of TOKEN, USER/PASSWORD, " +
	"NKEY_SEED or CREDS (the contents of a .creds file), optional TLS_CA/TLS_CERT/TLS_KEY as PEM text, optional " +
	"MONITORING_URL such as 'http://host:8222' (one per server), optional JETSTREAM_DOMAIN). With several secrets, pass " +
	"the secret_name named argument. Monitoring functions read the HTTP monitoring endpoints when MONITORING_URL is set " +
	"and otherwise the $SYS request API, which needs system-account credentials and answers once per server. To look " +
	"inside a stream use messages (payload as `BLOB` plus payload_text, headers as a `MAP`, subject/sequence/timestamp " +
	"filters pushed into the read); for consumer lag use consumers; for KV values use kv_entries; for server load use " +
	"server_info and connections. Reads use short-lived ephemeral consumers without acknowledgements, so nothing is " +
	"consumed, acknowledged, purged or deleted, and nothing is ever published."

// CatalogDocMD is the catalog's comment: one short paragraph for directories.
const CatalogDocMD = "NATS and JetStream as SQL tables in DuckDB: server, connection, account and JetStream " +
	"monitoring, plus read-only streams, consumers with lag, stream messages, KV buckets and object stores."

const catalogDocMDLong = "# NATS in DuckDB\n\n" +
	"Read a NATS deployment from SQL without changing it.\n\n" +
	"- **Monitoring**: servers, client connections, cluster routes, gateways, leaf nodes, subscriptions, accounts and " +
	"JetStream usage, from the HTTP monitoring port or the `$SYS` request API, one row per server.\n" +
	"- **JetStream**: stream configuration and state, per-subject counts, consumers with their lag, and a " +
	"non-destructive read of a stream's messages with subject, sequence and time filters pushed into the read.\n" +
	"- **KV and object stores**: buckets, current values or full revision history, objects and (on request) their content.\n\n" +
	"Credentials come from a `nats` DuckDB secret: token, user/password, NKey seed or `.creds` contents, with optional TLS."

const schemaDocLLM = "The main schema holds every function; the table functions take an optional secret_name named " +
	"argument. Monitoring rows start with server_id and server_name. Times are `TIMESTAMPTZ`, durations are *_seconds " +
	"`DOUBLE` columns, limits are NULL when unlimited, and open-ended documents (varz, config, detail) are JSON text. " +
	"messages reads stored messages oldest first: filter with WHERE on subject, sequence and timestamp (pushed down) and " +
	"decode JSON payloads with the json_extract function over payload_text. kv_entries reads KV values (include_history " +
	"for every revision); objects lists object-store objects (include_content to download). Streams named KV_<bucket> " +
	"and OBJ_<bucket> back KV buckets and object stores (the kind column of streams). subject_matches applies NATS " +
	"wildcard rules locally."

// SchemaComment is the main schema's one-line comment.
const SchemaComment = "Read-only SQL over one NATS deployment: server monitoring, JetStream streams and messages, KV and object stores."

// SchemaDocMD is the main schema's human-facing description.
const SchemaDocMD = "## main\n\n" +
	"A read-only window onto one NATS deployment. Monitoring data comes from the HTTP monitoring endpoints when the " +
	"`nats` secret sets `MONITORING_URL`, and otherwise from the `$SYS` request API (system-account credentials, one " +
	"answer per server). JetStream, KV and object-store data comes from the account the secret authenticates as; pass " +
	"`secret_name` to pick between several secrets.\n\n" +
	"Conventions: times are `TIMESTAMPTZ`, durations are `*_seconds` doubles, unlimited limits are NULL, and open-ended " +
	"server documents are JSON text. Message reads use short-lived ephemeral consumers without acknowledgements, so " +
	"querying never consumes, acknowledges or deletes anything, and subject, sequence and time filters in `WHERE` are " +
	"pushed into the read instead of scanning the whole stream.\n\n" +
	"Streams named `KV_<bucket>` and `OBJ_<bucket>` are the storage behind KV buckets and object stores."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml. They assume
// the default nats secret points at the test fixture (test/fixture/natsfixture)
// as user app with MONITORING_URL set.
var agentTestTasks = []agentTask{
	{Name: "stream_sizes", Prompt: "Which JetStream streams (not KV or object stores) hold the most messages? List stream and message count, largest first."},
	{Name: "consumer_lag", Prompt: "Which consumers on the ORDERS stream are behind, and by how many messages?"},
	{Name: "eu_orders", Prompt: "List the sequence numbers and subjects of the European (orders.eu.*) messages in the ORDERS stream, oldest first."},
	{Name: "tenant_messages", Prompt: "How many ORDERS messages carry the header X-Tenant = 'globex'?"},
	{Name: "orders_per_subject", Prompt: "How many messages does each subject in the ORDERS stream hold?"},
	{Name: "current_color", Prompt: "What is the current value of the key app.color in the config KV bucket?"},
	{Name: "color_history", Prompt: "List every value app.color has had in the config KV bucket, oldest first."},
	{Name: "largest_object", Prompt: "Which object in the files object store is the largest, and how many bytes is it?"},
	{Name: "server_version", Prompt: "Which nats-server version is the server running, and is JetStream enabled?"},
	{Name: "busy_connection", Prompt: "Which named client connection has sent the server the most messages?"},
	{Name: "system_account", Prompt: "Which NATS account is the system account?"},
	{Name: "js_usage", Prompt: "How many streams and consumers does the account have in JetStream, and how much file storage do they use?"},
	{Name: "account_traffic", Prompt: "Which NATS account has published the most messages?"},
	{Name: "cluster_links", Prompt: "Does the NATS server have any cluster routes, super-cluster gateways or leaf-node connections? Count each."},
	{Name: "server_health", Prompt: "Is every NATS server healthy?"},
	{Name: "js_server_streams", Prompt: "How many streams and consumers does the JetStream server host?"},
	{Name: "all_account_lag", Prompt: "Across all accounts, which consumers have pending messages, and how many?"},
	{Name: "all_account_streams", Prompt: "Across all accounts, which stream stores the most bytes?"},
	{Name: "kv_inventory", Prompt: "Which KV buckets exist and how many revisions per key do they keep?"},
	{Name: "object_inventory", Prompt: "How many objects does each object store hold?"},
	{Name: "order_receivers", Prompt: "Which subscriptions in account APP would receive a message published on orders.eu.created?"},
	{Name: "match_cache", Prompt: "How many subscriptions does the NATS server's interest graph hold?"},
	{Name: "created_subjects", Prompt: "Which subjects of the ORDERS stream would a subscription to orders.*.created receive?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "VGI NATS Worker",
	"vgi.subtitle":           "NATS server monitoring and read-only JetStream streams, consumers, messages, KV and object stores.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             catalogDocMDLong,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("nats, jetstream, messaging, streams, consumers, consumer lag, kv, key value, object store, monitoring, varz, connz, observability"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	// Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
	"vgi.agent_test_tasks": mustJSON(agentTestTasks),
}

// schemaTags builds the main schema's tags; allExamples feeds vgi.example_queries.
func schemaTags(allExamples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "NATS Functions",
		"domain":              "messaging",
		"vgi.keywords":        keywordsArray("streams, consumers, messages, kv_entries, objects, server_info, connections, health"),
		"vgi.doc_llm":         schemaDocLLM,
		"vgi.doc_md":          SchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(allExamples),
	}
}
