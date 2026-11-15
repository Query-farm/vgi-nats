// A small framework for the request/response table functions: declare the
// arguments, the columns and a fetch that returns rows of Go values (and, for
// paged APIs, the next cursor); the shared code handles state, secrets,
// continuation and Arrow building.

package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SimpleState carries the bound arguments (JSON) and the page cursor. Both
// are exported so the scan resumes correctly over the HTTP transport.
type SimpleState struct {
	ArgsJSON string
	Cursor   string
	Done     bool
}

// page is one fetch result. Next is "" when there are no more pages.
type page struct {
	Rows [][]any
	Next string
}

// simpleFunc is a table function with argument struct A.
type simpleFunc[A secretNamed] struct {
	name     string
	meta     func() vgi.FunctionMetadata
	columns  []arrow.Field
	validate func(A) error
	fetch    func(ctx context.Context, c *config, a A, cursor string) (page, error)
}

func (f *simpleFunc[A]) Name() string                   { return f.name }
func (f *simpleFunc[A]) Metadata() vgi.FunctionMetadata { return f.meta() }
func (f *simpleFunc[A]) ArgumentSpecs() []vgi.ArgSpec   { var a A; return vgi.DeriveArgSpecs(a) }
func (f *simpleFunc[A]) schema() *arrow.Schema          { return arrow.NewSchema(f.columns, nil) }
func (f *simpleFunc[A]) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if f.validate != nil {
		if err := f.validate(a); err != nil {
			return nil, err
		}
	}
	return bindNamedSecret(params, a.secret(), f.schema())
}

func (f *simpleFunc[A]) NewState(params *vgi.ProcessParams) (*SimpleState, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if _, err := resolveConfig(params.Secrets, a.secret()); err != nil {
		return nil, err
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return &SimpleState{ArgsJSON: string(b)}, nil
}

func (f *simpleFunc[A]) Process(ctx context.Context, params *vgi.ProcessParams, st *SimpleState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	var a A
	if err := json.Unmarshal([]byte(st.ArgsJSON), &a); err != nil {
		return err
	}
	c, err := resolveConfig(params.Secrets, a.secret())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*RequestTimeout)
	defer cancel()
	p, err := f.fetch(ctx, c, a, st.Cursor)
	if err != nil {
		return err
	}
	st.Cursor = p.Next
	st.Done = p.Next == ""
	cols, err := buildGeneric(f.columns, p.Rows)
	if err != nil {
		return err
	}
	for _, col := range cols {
		defer col.Release()
	}
	return out.EmitArrays(cols, int64(len(p.Rows)))
}

func newSimple[A secretNamed](f *simpleFunc[A]) vgi.TableFunction {
	return vgi.AsTableFunction[SimpleState](f)
}

// single wraps a one-request fetch.
func single[A any](fn func(ctx context.Context, c *config, a A) ([][]any, error)) func(context.Context, *config, A, string) (page, error) {
	return func(ctx context.Context, c *config, a A, _ string) (page, error) {
		rows, err := fn(ctx, c, a)
		return page{Rows: rows}, err
	}
}

// ---------------------------------------------------------------- value helpers

// optString is NULL for "".
func optString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// optTime is NULL for the zero time.
func optTime(t time.Time) any {
	if t.IsZero() || t.Year() <= 1 {
		return nil
	}
	return t
}

// optTimePtr is NULL for nil or the zero time.
func optTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return optTime(*t)
}

// optInt is NULL for 0 (used for "unset" limits that NATS reports as 0).
func optInt(i int64) any {
	if i == 0 {
		return nil
	}
	return i
}

// limit is NULL for NATS's "unlimited" (-1 or 0).
func limit(i int64) any {
	if i <= 0 {
		return nil
	}
	return i
}

// seconds renders a duration in seconds; NULL when zero.
func seconds(d time.Duration) any {
	if d == 0 {
		return nil
	}
	return d.Seconds()
}

// strList is a non-nil []string (an empty LIST rather than NULL).
func strList(l []string) []string {
	if l == nil {
		return []string{}
	}
	return l
}

// rawJSON renders a value as JSON text; NULL for nil.
func rawJSON(v any) any {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil || string(b) == "null" {
		return nil
	}
	return string(b)
}

// ---------------------------------------------------------------- Arrow building

// buildGeneric builds columns from rows of Go values. Supported values per
// column type: string; int64; float64; bool; time.Time; []byte;
// map[string]string; []string; nil for NULL.
func buildGeneric(fields []arrow.Field, rows [][]any) ([]arrow.Array, error) {
	mem := memory.NewGoAllocator()
	out := make([]arrow.Array, 0, len(fields))
	release := func() {
		for _, a := range out {
			a.Release()
		}
	}
	for ci, fd := range fields {
		b := array.NewBuilder(mem, fd.Type)
		for ri, r := range rows {
			v := r[ci]
			if v == nil {
				if !fd.Nullable {
					b.Release()
					release()
					return nil, fmt.Errorf("internal: NULL in non-null column %s (row %d)", fd.Name, ri)
				}
				b.AppendNull()
				continue
			}
			if err := appendGeneric(b, v); err != nil {
				b.Release()
				release()
				return nil, fmt.Errorf("column %s: %w", fd.Name, err)
			}
		}
		out = append(out, b.NewArray())
		b.Release()
	}
	return out, nil
}

func appendGeneric(b array.Builder, v any) error {
	switch bb := b.(type) {
	case *array.StringBuilder:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("want string, got %T", v)
		}
		bb.Append(s)
	case *array.BinaryBuilder:
		s, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("want []byte, got %T", v)
		}
		bb.Append(s)
	case *array.Int64Builder:
		i, ok := v.(int64)
		if !ok {
			return fmt.Errorf("want int64, got %T", v)
		}
		bb.Append(i)
	case *array.Float64Builder:
		f, ok := v.(float64)
		if !ok {
			return fmt.Errorf("want float64, got %T", v)
		}
		bb.Append(f)
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
		bb.Append(x)
	case *array.TimestampBuilder:
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("want time, got %T", v)
		}
		bb.Append(arrow.Timestamp(t.UnixMicro()))
	case *array.ListBuilder:
		l, ok := v.([]string)
		if !ok {
			return fmt.Errorf("want []string, got %T", v)
		}
		bb.Append(true)
		vb := bb.ValueBuilder().(*array.StringBuilder)
		for _, x := range l {
			vb.Append(x)
		}
	case *array.MapBuilder:
		m, ok := v.(map[string]string)
		if !ok {
			return fmt.Errorf("want map, got %T", v)
		}
		bb.Append(true)
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		kb := bb.KeyBuilder().(*array.StringBuilder)
		ib := bb.ItemBuilder().(*array.StringBuilder)
		for _, k := range keys {
			kb.Append(k)
			ib.Append(m[k])
		}
	default:
		return fmt.Errorf("unsupported builder %T", b)
	}
	return nil
}

// Column types.
var (
	str    = arrow.BinaryTypes.String
	bin    = arrow.BinaryTypes.Binary
	i64    = arrow.PrimitiveTypes.Int64
	f64    = arrow.PrimitiveTypes.Float64
	boolT  = arrow.FixedWidthTypes.Boolean
	tsT    = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	strMap = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	strLst = arrow.ListOf(arrow.BinaryTypes.String)
)

func duckType(t arrow.DataType) string {
	switch t.ID() {
	case arrow.STRING:
		return "VARCHAR"
	case arrow.BINARY:
		return "BLOB"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.TIMESTAMP:
		return "TIMESTAMP WITH TIME ZONE"
	case arrow.MAP:
		return "MAP(VARCHAR, VARCHAR)"
	case arrow.LIST:
		return "VARCHAR[]"
	}
	return t.String()
}

// meta builds a function's metadata from its docs, columns and examples.
func meta(title, category, keywords, docLLM, docMD, desc string, cols []arrow.Field, examples []vgi.CatalogExample) func() vgi.FunctionMetadata {
	return func() vgi.FunctionMetadata {
		tags := objectTags(docs{Title: title, Category: category, Keywords: keywords, DocLLM: docLLM, DocMD: docMD})
		rc := make([]resultColumn, len(cols))
		for i, f := range cols {
			d, _ := f.Metadata.GetValue("comment")
			rc[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: d}
		}
		tags["vgi.result_columns_schema"] = mustJSON(rc)
		return vgi.FunctionMetadata{
			Description:     desc,
			Stability:       vgi.StabilityVolatile,
			Tags:            withExamples(tags, examples),
			RequiredSecrets: secretRequirements,
			Examples:        examples,
		}
	}
}
