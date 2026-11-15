// Offline subject tools: NATS wildcard matching as a SQL scalar, usable on
// subjects from messages(), stream_subjects() or subscriptions() without a
// server.

package nats

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type subjectMatchesArgs struct {
	Pattern *array.String `vgi:"pos=0,const=false,doc=Subscription subject, possibly with * (one token) and > (one or more trailing tokens) wildcards; NULL in, NULL out."`
	Subject *array.String `vgi:"pos=1,const=false,doc=Subject to test, e.g. a message's subject; NULL in, NULL out."`
}

var subjectMatchesExamples = []vgi.CatalogExample{
	{SQL: "SELECT subject, messages FROM nats.main.stream_subjects('ORDERS') WHERE nats.main.subject_matches('orders.*.created', subject) ORDER BY subject",
		Description: "Subjects of a stream that a wildcard subscription would receive"},
}

// SubjectMatches implements subject_matches(pattern, subject).
type SubjectMatches struct{}

// Name is the SQL name.
func (*SubjectMatches) Name() string { return "subject_matches" }

// Metadata describes subject_matches.
func (*SubjectMatches) Metadata() vgi.FunctionMetadata {
	tags := objectTags(docs{
		Title:    "Subject Wildcard Match",
		Category: "subjects",
		Keywords: "subject, wildcard, match, star, greater than, subscription, filter subject",
		DocLLM: "Return true when a subscription subject pattern receives messages published on subject, using NATS " +
			"wildcard rules: '*' matches exactly one token, '>' matches one or more trailing tokens, tokens are separated " +
			"by '.'. Both arguments are `VARCHAR` columns or constants; NULL in, NULL out. Runs locally, needs no secret " +
			"or server. Use it to filter subjects from messages, stream_subjects or subscriptions with NATS semantics " +
			"(SQL LIKE does not know that '*' stops at a dot).",
		DocMD: "## subject_matches(pattern, subject)\n\n" +
			"NATS wildcard matching: `*` is one token, `>` the rest.\n\n" +
			"```sql\nSELECT nats.main.subject_matches('orders.*.created', 'orders.eu.created');  -- true\n```",
	})
	tags["vgi.executable_examples"] = mustJSON([]map[string]any{
		{
			"description": "Single-token and trailing wildcards",
			"sql": "SELECT p, nats.main.subject_matches(p, 'orders.eu.created') AS matches FROM (VALUES ('orders.*.created'), " +
				"('orders.>'), ('orders.*'), ('orders.us.created')) t(p) ORDER BY p",
			"expected_result": []map[string]any{
				{"p": "orders.*", "matches": "false"},
				{"p": "orders.*.created", "matches": "true"},
				{"p": "orders.>", "matches": "true"},
				{"p": "orders.us.created", "matches": "false"},
			},
		},
		{
			"description": "'>' needs at least one more token",
			"sql":         "SELECT nats.main.subject_matches('orders.>', 'orders') AS matches",
			"expected_result": []map[string]any{
				{"matches": "false"},
			},
		},
	})
	return vgi.FunctionMetadata{
		Description:  "True when a NATS subscription pattern (with * and > wildcards) receives a subject",
		Stability:    vgi.StabilityConsistent,
		NullHandling: vgi.NullHandlingDefault,
		ReturnType:   arrow.FixedWidthTypes.Boolean,
		Tags:         withExamples(tags, subjectMatchesExamples),
		Examples:     subjectMatchesExamples,
	}
}

// OnBindTyped declares the BOOLEAN result.
func (*SubjectMatches) OnBindTyped(_ *subjectMatchesArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.FixedWidthTypes.Boolean)
}

// ProcessTyped matches row by row; a NULL in either argument gives NULL.
func (*SubjectMatches) ProcessTyped(_ context.Context, _ *subjectMatchesArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewBooleanBuilder,
		func(cols []arrow.Array, i int) bool {
			return subjectMatches(vgi.GetStringValue(cols[0], i), vgi.GetStringValue(cols[1], i))
		})
}
