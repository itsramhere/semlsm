package semantic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// Narrative length limits per B9 (runes, not bytes).
const (
	MaxSummaryRunes    = 2000
	MaxFinalStateRunes = 1000
	MaxFactRunes       = 300
	MaxKeyFacts        = 30
	MaxAnomalies       = 20
	MinRefsPerFact     = 1
	MaxRefsPerFact     = 5
)

// ValidationError is one rejected LLM narrative, tagged with the rule that
// fired (P8.5).
type ValidationError struct {
	Rule string // V1..V8
	Msg  string
}

// Error implements error.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("semantic: %s: %s", e.Rule, e.Msg)
}

func verr(rule, format string, args ...interface{}) error {
	return &ValidationError{Rule: rule, Msg: fmt.Sprintf(format, args...)}
}

// ParseAndValidate parses one LLM response into a Narrative, applying the
// rules V1..V8 in order (P8.5): V1 strip whitespace and one optional code
// fence, V2 strict JSON without unknown fields or trailing data, V3
// non-empty summary, V4 length limits, V5 list limits, V6 1-5 refs per
// fact, V7 every ref in validIDs, V8 no control characters except \n and
// \t. The narrative is returned only when every rule passes.
func ParseAndValidate(text string, validIDs map[string]bool) (Narrative, error) {
	// V1: strip whitespace and one optional code fence
	s := strings.TrimSpace(text)
	if strings.HasPrefix(s, "```") {
		i := strings.IndexByte(s, '\n')
		if i < 0 {
			return Narrative{}, verr("V1", "code fence without content")
		}
		s = strings.TrimSpace(s[i+1:])
		if strings.HasSuffix(s, "```") {
			s = strings.TrimSpace(s[:len(s)-3])
		}
	}

	// V2: strict JSON, unknown fields rejected, no trailing data
	dec := json.NewDecoder(strings.NewReader(s))
	dec.DisallowUnknownFields()
	var n Narrative
	if err := dec.Decode(&n); err != nil {
		return Narrative{}, verr("V2", "not valid narrative JSON: %v", err)
	}
	var extra interface{}
	if err := dec.Decode(&extra); err != io.EOF {
		return Narrative{}, verr("V2", "trailing data after the JSON object")
	}

	// V3: summary non-empty
	if strings.TrimSpace(n.Summary) == "" {
		return Narrative{}, verr("V3", "summary is empty")
	}

	// V4: length limits (runes)
	if utf8.RuneCountInString(n.Summary) > MaxSummaryRunes {
		return Narrative{}, verr("V4", "summary exceeds %d runes", MaxSummaryRunes)
	}
	if utf8.RuneCountInString(n.FinalState) > MaxFinalStateRunes {
		return Narrative{}, verr("V4", "final_state exceeds %d runes", MaxFinalStateRunes)
	}

	// V5: list limits
	if len(n.KeyFacts) > MaxKeyFacts {
		return Narrative{}, verr("V5", "key_facts has %d entries, max %d", len(n.KeyFacts), MaxKeyFacts)
	}
	if len(n.Anomalies) > MaxAnomalies {
		return Narrative{}, verr("V5", "anomalies has %d entries, max %d", len(n.Anomalies), MaxAnomalies)
	}

	// V6/V7/V8 per fact; V8 also covers the top-level text fields
	check := func(where string, facts []Fact) error {
		for i, f := range facts {
			if len(f.Refs) < MinRefsPerFact || len(f.Refs) > MaxRefsPerFact {
				return verr("V6", "%s[%d] has %d refs, want %d-%d", where, i, len(f.Refs), MinRefsPerFact, MaxRefsPerFact)
			}
			for _, r := range f.Refs {
				if !validIDs[r] {
					return verr("V7", "%s[%d] cites unknown record %q", where, i, r)
				}
			}
			if utf8.RuneCountInString(f.Text) > MaxFactRunes {
				return verr("V4", "%s[%d] text exceeds %d runes", where, i, MaxFactRunes)
			}
			if err := checkControl(where, f.Text); err != nil {
				return err
			}
		}
		return nil
	}
	if err := check("key_facts", n.KeyFacts); err != nil {
		return Narrative{}, err
	}
	if err := check("anomalies", n.Anomalies); err != nil {
		return Narrative{}, err
	}
	if err := checkControl("summary", n.Summary); err != nil {
		return Narrative{}, err
	}
	if err := checkControl("final_state", n.FinalState); err != nil {
		return Narrative{}, err
	}
	return n, nil
}

// checkControl rejects control characters other than \n and \t (V8).
func checkControl(where, s string) error {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 && c != '\n' && c != '\t' {
			return verr("V8", "%s contains control character 0x%02x at byte %d", where, c, i)
		}
		if c == 0x7f {
			return verr("V8", "%s contains control character 0x%02x at byte %d", where, c, i)
		}
	}
	return nil
}

// NarrativeJSON renders a Narrative as the compact JSON stored in a tuple's
// summary section (B9).
func NarrativeJSON(n Narrative) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(n); err != nil {
		return nil, fmt.Errorf("semantic: encode narrative: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
