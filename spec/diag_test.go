package spec

import (
	"strings"
	"testing"
)

// Choices are recorded against the number of messages already sent rather than
// woven into the transcript, so Steps is where the two orders are reconciled.
// These cover the placements a conversation actually produces: a decision before
// the first message (a specification may branch before saying anything), several in
// a row with no message between them, and one after the last message.

func steps(t *testing.T, r *Result) []string {
	t.Helper()
	var out []string
	for _, st := range r.Steps() {
		switch {
		case st.Entry != nil:
			out = append(out, "msg")
		case st.Choice != nil:
			out = append(out, st.Choice.Label)
		default:
			t.Fatal("a Step has neither an Entry nor a Choice")
		}
	}
	return out
}

func msgs(n int) []Entry {
	es := make([]Entry, n)
	for i := range es {
		es[i] = Entry{Seq: i + 1}
	}
	return es
}

func TestStepsInterleavesChoicesAmongMessages(t *testing.T) {
	got := steps(t, &Result{
		Transcript: msgs(3),
		Choices: []Choice{
			{AfterSeq: 0, Label: "before-any"},
			{AfterSeq: 1, Label: "after-first"},
			{AfterSeq: 3, Label: "after-last"},
		},
	})
	want := []string{"before-any", "msg", "after-first", "msg", "msg", "after-last"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("steps came out as %v, want %v", got, want)
	}
}

// TestStepsKeepsSeveralDecisionsAtOnePointInOrder: two branches with no message
// between them share an AfterSeq, so only their order in Choices distinguishes
// them.
func TestStepsKeepsSeveralDecisionsAtOnePointInOrder(t *testing.T) {
	got := steps(t, &Result{
		Transcript: msgs(1),
		Choices: []Choice{
			{AfterSeq: 1, Label: "first"},
			{AfterSeq: 1, Label: "second"},
			{AfterSeq: 1, Label: "third"},
		},
	})
	want := []string{"msg", "first", "second", "third"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("steps came out as %v, want %v", got, want)
	}
}

func TestStepsHandlesRunsWithOnlyOneKind(t *testing.T) {
	if got := steps(t, &Result{Transcript: msgs(2)}); len(got) != 2 {
		t.Errorf("a run with no branches gave %v", got)
	}
	if got := steps(t, &Result{Choices: []Choice{{Label: "a"}}}); len(got) != 1 {
		t.Errorf("a run with no messages gave %v", got)
	}
	if got := steps(t, &Result{}); len(got) != 0 {
		t.Errorf("an empty run gave %v", got)
	}
}

// TestViolationListsTheBranchesItPassed: which way the conversation went is
// usually why the message that failed was the one expected, so the diagnostic has
// to show the branches alongside the messages.
func TestViolationListsTheBranchesItPassed(t *testing.T) {
	v := &Violation{
		Seq: 2, Constraint: "Eq(attempt)", Detail: "observed 2, want 1",
		Transcript: msgs(2),
		Choices: []Choice{{
			AfterSeq: 1, Label: "long-poll-timeout", Taken: false,
			FromLookahead: true, Describe: `next response has empty "task_token"`,
		}},
	}
	got := v.Error()

	if !strings.Contains(got, "long-poll-timeout") {
		t.Errorf("the diagnostic does not list the branch that was taken:\n%s", got)
	}
	if !strings.Contains(got, `lookahead: next response has empty "task_token"`) {
		t.Errorf("the diagnostic does not say what decided the branch:\n%s", got)
	}
}
