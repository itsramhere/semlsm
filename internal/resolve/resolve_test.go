package resolve

import (
	"testing"

	"semlsm/internal/base"
)

func TestResolveTruthTable(t *testing.T) {
	rawPut := Raw{Found: true, Ts: 10, Kind: base.KindPut}
	rawDel := Raw{Found: true, Ts: 10, Kind: base.KindDelete}
	none := Raw{}
	entry := Entry{Found: true, MinTs: 20, MaxTs: 30, Count: 5, LastKind: base.KindPut}
	entryDel := Entry{Found: true, MinTs: 20, MaxTs: 30, Count: 5, LastKind: base.KindDelete}

	tests := []struct {
		name string
		s    uint64
		r    Raw
		e    Entry
		want Outcome
	}{
		// Row 1
		{"no raw, no entry", 25, none, Entry{}, NotFound},
		// Rows 2-3
		{"raw only put", 25, rawPut, Entry{}, UseRaw},
		{"raw only delete", 25, rawDel, Entry{}, NotFound},
		// Rows 4-5
		{"raw newer than entry, put", 35, Raw{Found: true, Ts: 40, Kind: base.KindPut}, entry, UseRaw},
		{"raw newer than entry, delete", 35, Raw{Found: true, Ts: 40, Kind: base.KindDelete}, entry, NotFound},
		// Row 6: raw inside dropped range
		{"raw inside range", 25, Raw{Found: true, Ts: 25, Kind: base.KindPut}, entry, Invariant},
		{"raw at MinTs", 25, Raw{Found: true, Ts: 20, Kind: base.KindPut}, entry, Invariant},
		{"raw at MaxTs", 25, Raw{Found: true, Ts: 30, Kind: base.KindDelete}, entry, Invariant},
		// Row 7: entry ends with delete and nothing newer
		{"entry delete, no raw", 30, none, entryDel, NotFound},
		{"entry delete, stale raw", 30, Raw{Found: true, Ts: 10, Kind: base.KindPut}, entryDel, NotFound},
		// Row 7 does NOT fire when a newer raw exists (rows 4/5 handle that)
		{"entry delete, raw newer", 35, Raw{Found: true, Ts: 40, Kind: base.KindPut}, entryDel, UseRaw},
		// Row 8: entry covers, read inside or below
		{"entry covers, no raw", 25, none, entry, UseSummary},
		{"entry covers, stale raw", 25, Raw{Found: true, Ts: 10, Kind: base.KindPut}, entry, UseSummary},
		{"entry covers, s below MaxTs, delete end", 25, none, entryDel, UseSummary},
		{"entry covers, s below MinTs edge", 15, none, entry, UseSummary},
		// Boundary: s == MaxTs with Put end
		{"s equals MaxTs", 30, none, entry, UseSummary},
	}
	for _, tt := range tests {
		if got := Resolve(tt.s, tt.r, tt.e); got != tt.want {
			t.Errorf("%s: got %d want %d", tt.name, got, tt.want)
		}
	}
}

func TestResolveRows7Boundary(t *testing.T) {
	// Row 7 fires only when s >= e.MaxTs. At s < MaxTs the tail state is not
	// yet visible, so the summary covers (row 8).
	e := Entry{Found: true, MinTs: 20, MaxTs: 30, LastKind: base.KindDelete}
	if got := Resolve(29, Raw{}, e); got != UseSummary {
		t.Fatalf("s below MaxTs with delete tail: got %d, want UseSummary", got)
	}
	if got := Resolve(30, Raw{}, e); got != NotFound {
		t.Fatalf("s == MaxTs with delete tail: got %d, want NotFound", got)
	}
}
