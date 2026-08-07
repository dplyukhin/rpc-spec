package spec

import (
	"strings"
	"testing"

	wsv1 "go.temporal.io/api/workflowservice/v1"
)

// count is how many times a rendered diagram contains something, for the "drawn
// once, not once per path" assertions that are the whole point of merging.
func count(diagram, what string) int { return strings.Count(diagram, what) }

// TestExplorationDrawsASharedPrefixOnce is the property that makes an exploration
// a diagram rather than a pile of them: paths that agree up to a branch share a
// trunk, so the messages before the branch appear once however many paths run
// through them.
func TestExplorationDrawsASharedPrefixOnce(t *testing.T) {
	// Two paths that differ only after the first message.
	body := func(s *Session[testCfg]) {
		Send[*wsv1.PollWorkflowTaskQueueRequest](s)
		if s.Maybe("failed", PeekIsType[*wsv1.RespondWorkflowTaskFailedRequest](
			&wsv1.RespondWorkflowTaskCompletedRequest{})) {
			Send[*wsv1.RespondWorkflowTaskFailedRequest](s)
			return
		}
		Send[*wsv1.RespondWorkflowTaskCompletedRequest](s)
	}
	ex := Explore(freshCfg, body, Bounds{})
	if len(ex.Paths) != 2 {
		t.Fatalf("expected two paths, got %d", len(ex.Paths))
	}
	got := ex.Mermaid()

	if n := count(got, "PollWorkflowTaskQueueRequest"); n != 1 {
		t.Errorf("the shared prefix was drawn %d times, want once:\n%s", n, got)
	}
	if n := count(got, "alt failed"); n != 1 {
		t.Errorf("the branch was drawn %d times, want once:\n%s", n, got)
	}
	for _, want := range []string{
		"RespondWorkflowTaskFailedRequest",
		"else not failed",
		"RespondWorkflowTaskCompletedRequest",
		"\n    end\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the diagram is missing %q:\n%s", want, got)
		}
	}
}

// TestExplorationIndentsNestedBranches: renderEntry takes an indent precisely so
// that arrows inside an alt sit inside its block.
func TestExplorationIndentsNestedBranches(t *testing.T) {
	got := Explore(freshCfg, loopSpec, Bounds{Repeats: 2}).Mermaid()

	if !strings.Contains(got, "\n        alt again") {
		t.Errorf("a nested branch was not indented inside its parent:\n%s", got)
	}
	if !strings.Contains(got, "\n            Worker->>Server:") {
		t.Errorf("a message inside a nested branch was not indented with it:\n%s", got)
	}
}

// TestExplorationNamesTheBranchesItDidNotFollow is the no-silent-caps rule. An
// unexplored arm must be visible in the drawing, because a diagram that quietly
// stopped looking reads as though it were the whole contract.
func TestExplorationNamesTheBranchesItDidNotFollow(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{RepeatsByLabel: map[string]int{"again": 0}})
	got := ex.Mermaid()

	if !strings.Contains(unwrap(got), "branch not followed: again = true") {
		t.Errorf("the unexplored branch is not named in the diagram:\n%s", got)
	}
	// With one arm missing there is nothing to contrast, so no alt block is drawn.
	if strings.Contains(got, "alt again") {
		t.Errorf("a branch with only one arm explored was drawn as an alt:\n%s", got)
	}
	if strings.Contains(got, "else") {
		t.Errorf("an else was drawn for a branch with no second arm:\n%s", got)
	}
}

func TestExplorationMarksWhereAPathWasCutShort(t *testing.T) {
	chatty := func(s *Session[testCfg]) {
		for {
			Send[*wsv1.PollWorkflowTaskQueueRequest](s)
			Send[*wsv1.PollWorkflowTaskQueueResponse](s)
		}
	}
	got := Explore(freshCfg, chatty, Bounds{MaxMessages: 4}).Mermaid()

	if !strings.Contains(unwrap(got), "bound reached: the conversation continues past this point") {
		t.Errorf("a truncated path is not marked in the diagram:\n%s", got)
	}
}

// TestExplorationPrefersTheLoopToTheTruncationMarker records a deliberate
// exchange. Where a loop is recognized, the unrolling below the back edge is not
// drawn, so a path that stopped at a bound down there loses its marker — but the
// loop box makes the stronger statement in its place: not "the search stopped
// looking here" but "this block repeats". The count of truncated paths is still
// reported by the exploration itself, so nothing is actually concealed.
func TestExplorationPrefersTheLoopToTheTruncationMarker(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{Repeats: 10, MaxChoices: 2})
	if ex.Truncated() == 0 {
		t.Fatal("expected this exploration to hit a bound")
	}
	got := ex.Mermaid()

	if !strings.Contains(got, `loop repeats from "again"`) {
		t.Errorf("the loop was not recognized:\n%s", got)
	}
	if strings.Contains(got, "bound reached") {
		t.Errorf("an unrolling below a recognized loop's back edge was still drawn:\n%s", got)
	}
}

// TestExplorationDoesNotCallEveryLifelineAMock: under the paper transport both
// directions are generated, so the label would be on every party and would say
// nothing. This diagram is of the contract, not of a run against a peer.
func TestExplorationDoesNotCallEveryLifelineAMock(t *testing.T) {
	got := Explore(freshCfg, loopSpec, Bounds{Repeats: 1}).Mermaid()

	if strings.Contains(got, "(mock)") {
		t.Errorf("a specification-level diagram labelled its lifelines as mocks:\n%s", got)
	}
	if !strings.Contains(got, "participant Worker as Worker\n") {
		t.Errorf("the Worker lifeline is missing or mislabelled:\n%s", got)
	}
}

// TestExplorationDoesNotNumberArrows: a number would imply an order across
// alternatives that never occur in the same conversation.
func TestExplorationDoesNotNumberArrows(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{Repeats: 1})
	if strings.Contains(MermaidOptions{Autonumber: true}.RenderExploration(ex), "autonumber") {
		t.Error("an exploration numbered its arrows across mutually exclusive branches")
	}
}

func TestExplorationRendersTitleAndDetail(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{Repeats: 1})
	got := MermaidOptions{
		Title:  "the loop",
		Detail: func(e Entry) string { return "[" + e.MsgType() + "]" },
	}.RenderExploration(ex)

	if !strings.Contains(got, "title the loop\n") {
		t.Errorf("the title is missing:\n%s", got)
	}
	if !strings.Contains(got, "[PollWorkflowTaskQueueRequest]") {
		t.Errorf("the detail hook was not applied inside the tree:\n%s", got)
	}
}

func TestExplorationHandlesNothingToDraw(t *testing.T) {
	if got := (MermaidOptions{}).RenderExploration(nil); !strings.HasPrefix(got, "sequenceDiagram\n") {
		t.Errorf("a nil exploration did not produce a well-formed empty diagram: %q", got)
	}
	empty := &Exploration{}
	if got := empty.Mermaid(); !strings.HasPrefix(got, "sequenceDiagram\n") {
		t.Errorf("an empty exploration did not produce a well-formed empty diagram: %q", got)
	}
}
