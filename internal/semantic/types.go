package semantic

// B9 summary schema. The engine computes Stats; the LLM produces only the
// Narrative, which is validated (P8.5) before it enters a Summary.

// KeyCount is one entry of Stats.TopKeys.
type KeyCount struct {
	Key   string
	Count uint64
}

// NumericField aggregates one top-level numeric field of JSON-object values.
type NumericField struct {
	Name  string
	Count uint64
	Min   float64
	Max   float64
	Sum   float64
}

// Stats is the engine-computed half of a Summary (B9).
type Stats struct {
	RecordCount   uint64
	PutCount      uint64
	DeleteCount   uint64
	DistinctKeys  uint64
	FirstTs       uint64
	LastTs        uint64
	TopKeys       []KeyCount     // top 20 by version count, ties by key
	NumericFields []NumericField // top 20 fields by count, ties by name
	ValueBytes    uint64
}

// Fact is one LLM-produced statement citing record ids.
type Fact struct {
	Text string   `json:"text"`
	Refs []string `json:"refs"`
	// ResolvedRefs is filled by the engine (P8.6) and persisted under the
	// key resolved_refs in stored JSON via a separate struct (B9).
	ResolvedRefs []RecordRef `json:"-"`
}

// Narrative is the validated LLM half of a Summary (B9).
type Narrative struct {
	Summary    string `json:"summary"`
	KeyFacts   []Fact `json:"key_facts"`
	Anomalies  []Fact `json:"anomalies"`
	FinalState string `json:"final_state"`
}

// RecordRef is a record id resolved back to (key, ts) before storage.
type RecordRef struct {
	ID  string `json:"id"`
	Key []byte `json:"key"`
	Ts  uint64 `json:"ts"`
}

// Summary is the tuple's summary section (B9).
type Summary struct {
	SchemaVersion int
	Stats         Stats
	Narrative     Narrative
}
