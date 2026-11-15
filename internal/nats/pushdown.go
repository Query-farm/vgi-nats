// Pushdown planning for messages(): turn the SQL predicates DuckDB pushes on
// subject, seq and timestamp into the read's filter subjects, start position
// and stop sequence. The plan only narrows the read to a superset of the
// matching rows; see exact.go for how the remaining rows are filtered.

package nats

import (
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// pushPlan is what the pushed predicates allow the read to skip.
type pushPlan struct {
	Subjects []string // literal subjects (=, IN); nil = no subject restriction
	MinSeq   uint64   // first sequence that can match; 0 = none
	MaxSeq   uint64   // last sequence that can match; 0 = none
	Empty    bool     // the predicates can match nothing
	MinTime  *time.Time
}

func planPushdown(pf *vgi.PushdownFilters) pushPlan {
	var p pushPlan
	if pf == nil {
		return p
	}
	if vals := pf.GetColumnValues("subject"); vals != nil {
		if sa, ok := vals.(*array.String); ok {
			p.Subjects = []string{}
			for i := 0; i < sa.Len(); i++ {
				if sa.IsValid(i) && validFilterSubject(sa.Value(i)) == nil && !hasWildcard(sa.Value(i)) {
					p.Subjects = append(p.Subjects, sa.Value(i))
				} else if sa.IsValid(i) {
					// A literal that is not a usable filter (e.g. contains '*'): no subject pushdown.
					p.Subjects = nil
					break
				}
			}
			if p.Subjects != nil && len(p.Subjects) == 0 {
				p.Empty = true
			}
		}
	}
	if b := pf.GetColumnBounds("seq"); b != nil {
		if v, ok := scalarInt(b.MinValue); ok {
			if !b.MinInclusive {
				v++
			}
			if v < 1 {
				v = 1
			}
			p.MinSeq = uint64(v)
		}
		if v, ok := scalarInt(b.MaxValue); ok {
			if !b.MaxInclusive {
				v--
			}
			if v < 1 {
				p.Empty = true
			} else {
				p.MaxSeq = uint64(v)
			}
		}
	}
	if b := pf.GetColumnBounds("timestamp"); b != nil {
		if t, ok := scalarTime(b.MinValue); ok {
			p.MinTime = &t
		}
	}
	return p
}

func hasWildcard(s string) bool {
	for _, t := range splitTokens(s) {
		if t == "*" || t == ">" {
			return true
		}
	}
	return false
}

func splitTokens(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// apply narrows the scan state.
func (p pushPlan) apply(st *ScanState) {
	if p.Empty {
		st.Done = true
		return
	}
	if p.Subjects != nil {
		if len(st.Filters) == 0 {
			st.Filters = p.Subjects
		} else {
			// Keep only the literal subjects the argument's pattern also covers.
			var keep []string
			for _, s := range p.Subjects {
				if subjectMatches(st.Filters[0], s) {
					keep = append(keep, s)
				}
			}
			if len(keep) == 0 {
				st.Done = true
				return
			}
			st.Filters = keep
		}
	}
	if p.MinSeq > st.NextSeq {
		st.NextSeq = p.MinSeq
	}
	if p.MaxSeq > 0 && p.MaxSeq < st.StopSeq {
		st.StopSeq = p.MaxSeq
	}
	if p.MinTime != nil && st.NextSeq == 0 && (st.StartTime == nil || p.MinTime.After(*st.StartTime)) {
		t := p.MinTime.UTC()
		st.StartTime = &t
	}
}

func scalarInt(s scalar.Scalar) (int64, bool) {
	if s == nil || !s.IsValid() {
		return 0, false
	}
	switch v := s.(type) {
	case *scalar.Int64:
		return v.Value, true
	case *scalar.Int32:
		return int64(v.Value), true
	case *scalar.Int16:
		return int64(v.Value), true
	case *scalar.Int8:
		return int64(v.Value), true
	case *scalar.Uint64:
		return int64(v.Value), true
	case *scalar.Uint32:
		return int64(v.Value), true
	}
	return 0, false
}

func scalarTime(s scalar.Scalar) (time.Time, bool) {
	if s == nil || !s.IsValid() {
		return time.Time{}, false
	}
	ts, ok := s.(*scalar.Timestamp)
	if !ok {
		return time.Time{}, false
	}
	unit := ts.Type.(*arrow.TimestampType).Unit
	return ts.Value.ToTime(unit), true
}
