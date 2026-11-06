package base

import "time"

// Mode is the summarization policy for a key prefix (B4). The root package
// re-exports it as an alias: internal packages (semantic) need it and may
// not import the root.
type Mode uint8

const (
	// ModeDisabled never summarizes.
	ModeDisabled Mode = iota
	// ModeState is for repeatedly updated keys: the newest version is never dropped.
	ModeState
	// ModeEvents is for append-only streams: the newest version may be dropped.
	ModeEvents
)

// String returns the mode name.
func (m Mode) String() string {
	switch m {
	case ModeDisabled:
		return "disabled"
	case ModeState:
		return "state"
	case ModeEvents:
		return "events"
	default:
		return "unknown"
	}
}

// Policy selects which keys Tier B may summarize and under what conditions.
type Policy struct {
	Prefix   string
	Mode     Mode
	MinAge   time.Duration // never summarize records younger than this; default 1h
	MinBatch int           // fewer eligible records -> stay raw; default 1000
	MaxBatch int           // records per tuple; default 10000
}
