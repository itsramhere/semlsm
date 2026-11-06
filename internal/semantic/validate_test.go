package semantic

import (
	"strings"
	"testing"
)

var validIDs = map[string]bool{"r0": true, "r1": true, "r2": true}

func validNarrativeJSON() string {
	return `{"summary":"all good","key_facts":[{"text":"fact","refs":["r0"]}],` +
		`"anomalies":[],"final_state":"done"}`
}

func TestValidateHappyPath(t *testing.T) {
	n, err := ParseAndValidate(validNarrativeJSON(), validIDs)
	if err != nil {
		t.Fatal(err)
	}
	if n.Summary != "all good" || n.FinalState != "done" ||
		len(n.KeyFacts) != 1 || n.KeyFacts[0].Refs[0] != "r0" {
		t.Fatalf("narrative: %+v", n)
	}
}

func TestValidateTable(t *testing.T) {
	longSummary := strings.Repeat("é", MaxSummaryRunes+1) // runes, not bytes
	longFact := strings.Repeat("x", MaxFactRunes+1)
	manyFacts := make([]string, MaxKeyFacts+1)
	for i := range manyFacts {
		manyFacts[i] = `{"text":"t","refs":["r0"]}`
	}
	manyAnomalies := make([]string, MaxAnomalies+1)
	for i := range manyAnomalies {
		manyAnomalies[i] = `{"text":"t","refs":["r0"]}`
	}

	cases := []struct {
		name string
		text string
		rule string
	}{
		{"v2 not json", "hello, I am not JSON", "V2"},
		{"v2 unknown field", `{"summary":"s","bogus":1}`, "V2"},
		{"v2 unknown nested field", `{"summary":"s","key_facts":[{"text":"t","refs":["r0"],"why":1}]}`, "V2"},
		{"v2 trailing data", validNarrativeJSON() + ` {"summary":"x"}`, "V2"},
		{"v2 trailing junk", validNarrativeJSON() + "\nthanks!", "V2"},
		{"v3 empty summary", `{"summary":"","key_facts":[]}`, "V3"},
		{"v3 whitespace summary", `{"summary":"   ","key_facts":[]}`, "V3"},
		{"v4 summary runes", `{"summary":"` + longSummary + `","key_facts":[]}`, "V4"},
		{"v4 final_state runes", `{"summary":"s","key_facts":[],"final_state":"` + strings.Repeat("f", MaxFinalStateRunes+1) + `"}`, "V4"},
		{"v4 fact runes", `{"summary":"s","key_facts":[{"text":"` + longFact + `","refs":["r0"]}]}`, "V4"},
		{"v5 key_facts cap", `{"summary":"s","key_facts":[` + strings.Join(manyFacts, ",") + `]}`, "V5"},
		{"v5 anomalies cap", `{"summary":"s","anomalies":[` + strings.Join(manyAnomalies, ",") + `]}`, "V5"},
		{"v6 no refs", `{"summary":"s","key_facts":[{"text":"t","refs":[]}]}`, "V6"},
		{"v6 six refs", `{"summary":"s","key_facts":[{"text":"t","refs":["r0","r1","r2","r0","r1","r2"]}]}`, "V6"},
		{"v7 hallucinated ref", `{"summary":"s","key_facts":[{"text":"t","refs":["r0","r99"]}]}`, "V7"},
		{"v8 control char in summary", "{\"summary\":\"bad\\u0001here\",\"key_facts\":[]}", "V8"},
		{"v8 control char in fact", "{\"summary\":\"s\",\"key_facts\":[{\"text\":\"bad\\u0002\",\"refs\":[\"r0\"]}]}", "V8"},
		{"v8 control char in final_state", "{\"summary\":\"s\",\"final_state\":\"bad\\u007f\"}", "V8"},
		{"v1 double fence", "```\n" + validNarrativeJSON() + "\n```\nextra ```", "V2"},
		{"v1 bare fence", "```", "V1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseAndValidate(tc.text, validIDs)
			if err == nil {
				t.Fatalf("accepted")
			}
			ve, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("not a ValidationError: %v", err)
			}
			if ve.Rule != tc.rule {
				t.Fatalf("rule = %s, want %s (%v)", ve.Rule, tc.rule, ve)
			}
		})
	}
}

func TestValidateAcceptsExactLimits(t *testing.T) {
	summary := strings.Repeat("é", MaxSummaryRunes) // exactly at the limit
	final := strings.Repeat("f", MaxFinalStateRunes)
	fact := strings.Repeat("x", MaxFactRunes)
	fiveRefs := `["r0","r1","r2","r0","r1"]`
	text := `{"summary":"` + summary + `","key_facts":[{"text":"` + fact + `","refs":` + fiveRefs + `}],"final_state":"` + final + `"}`
	if _, err := ParseAndValidate(text, validIDs); err != nil {
		t.Fatalf("exact limits rejected: %v", err)
	}
}

func TestValidateStripsFence(t *testing.T) {
	for _, text := range []string{
		"```json\n" + validNarrativeJSON() + "\n```",
		"```\n" + validNarrativeJSON() + "\n```",
		"  \n```JSON\n" + validNarrativeJSON() + "\n```  \n",
		validNarrativeJSON(),
	} {
		n, err := ParseAndValidate(text, validIDs)
		if err != nil {
			t.Fatalf("%q: %v", text, err)
		}
		if n.Summary != "all good" {
			t.Fatalf("%q: summary %q", text, n.Summary)
		}
	}
}

func TestValidateControlCharsAllowed(t *testing.T) {
	text := "{\"summary\":\"line1\\nline2\\ttabbed\",\"key_facts\":[]}"
	if _, err := ParseAndValidate(text, validIDs); err != nil {
		t.Fatalf("\\n and \\t must be allowed: %v", err)
	}
}

func TestNarrativeJSONRoundTrip(t *testing.T) {
	n := Narrative{
		Summary:    "s <not escaped>",
		KeyFacts:   []Fact{{Text: "t", Refs: []string{"r0"}}},
		FinalState: "f",
	}
	data, err := NarrativeJSON(n)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseAndValidate(string(data), validIDs)
	if err != nil {
		t.Fatalf("own JSON rejected: %v (%s)", err, data)
	}
	if back.Summary != n.Summary || back.KeyFacts[0].Text != "t" {
		t.Fatalf("round trip: %+v", back)
	}
}
