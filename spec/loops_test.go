package spec

import (
	"strings"
	"testing"

	wsv1 "go.temporal.io/api/workflowservice/v1"
)

// TestLoopIsDrawnOnceWithTheTailOutside is the whole point of the step. Unrolling
// a loop puts a copy of everything that follows it under every iteration; the loop
// box collapses that to one pass and one tail.
func TestLoopIsDrawnOnceWithTheTailOutside(t *testing.T) {
	// Poll until something arrives, then answer it. The answer is the tail.
	body := func(s *Session[testCfg]) {
		for s.Maybe("again", PeekEmpty[*wsv1.PollWorkflowTaskQueueResponse]("task_token")) {
			Send[*wsv1.PollWorkflowTaskQueueRequest](s)
			Send[*wsv1.PollWorkflowTaskQueueResponse](s)
		}
		Send[*wsv1.RespondWorkflowTaskCompletedRequest](s)
		Send[*wsv1.RespondWorkflowTaskCompletedResponse](s)
	}

	unrolled := Explore(freshCfg, body, Bounds{Repeats: 3})
	if len(unrolled.Paths) != 4 {
		t.Fatalf("expected four unrollings, got %d", len(unrolled.Paths))
	}
	got := unrolled.Mermaid()

	if n := count(got, `loop repeats from "again"`); n != 1 {
		t.Errorf("drew %d loop box(es), want 1:\n%s", n, got)
	}
	// Four unrollings would otherwise mean four copies of the tail.
	if n := count(got, "RespondWorkflowTaskCompletedRequest"); n != 1 {
		t.Errorf("the tail after the loop was drawn %d times, want once:\n%s", n, got)
	}
	// One pass through the body, not three.
	if n := count(got, "PollWorkflowTaskQueueRequest"); n != 1 {
		t.Errorf("the loop body was drawn %d times, want once:\n%s", n, got)
	}
	if !strings.Contains(unwrap(got), `goes round again from "again"`) {
		t.Errorf("the back edge is not named:\n%s", got)
	}
	// The tail must sit outside the box.
	if strings.Index(got, "\n    end\n") > strings.Index(got, "RespondWorkflowTaskCompletedRequest") {
		t.Errorf("the tail was left inside the loop box:\n%s", got)
	}
}

// TestLoopBoxIsIndependentOfHowFarTheSearchUnrolled: if the drawing changed with
// the repeat bound, it would be describing the search rather than the
// specification.
func TestLoopBoxIsIndependentOfHowFarTheSearchUnrolled(t *testing.T) {
	one := Explore(freshCfg, loopSpec, Bounds{Repeats: 1}).Mermaid()
	four := Explore(freshCfg, loopSpec, Bounds{Repeats: 4}).Mermaid()

	if one != four {
		t.Errorf("unrolling further changed the drawing:\n--- repeats=1 ---\n%s\n--- repeats=4 ---\n%s", one, four)
	}
}

// TestABranchThatDoesNotComeBackIsNotALoop guards the other direction: an
// ordinary fork must not be dressed up as a repetition.
func TestABranchThatDoesNotComeBackIsNotALoop(t *testing.T) {
	body := func(s *Session[testCfg]) {
		if s.Maybe("failed", PeekIsType[*wsv1.RespondWorkflowTaskFailedRequest](
			&wsv1.RespondWorkflowTaskCompletedRequest{})) {
			Send[*wsv1.RespondWorkflowTaskFailedRequest](s)
			return
		}
		Send[*wsv1.RespondWorkflowTaskCompletedRequest](s)
	}
	got := Explore(freshCfg, body, Bounds{}).Mermaid()

	if strings.Contains(got, "loop ") {
		t.Errorf("a plain fork was drawn as a loop:\n%s", got)
	}
	if !strings.Contains(got, "alt failed") {
		t.Errorf("the fork was not drawn as an alternative:\n%s", got)
	}
}

// TestTwoBranchesInOneLoopGiveOneBox is the shape the polling specification has:
// a loop whose body contains a second branch, either arm of which can come back
// round. Only the outer branch heads the loop — treating the inner one as a loop
// head as well would draw the tail twice, once hoisted out of each box.
func TestTwoBranchesInOneLoopGiveOneBox(t *testing.T) {
	body := func(s *Session[testCfg]) {
		for {
			if s.Maybe("sticky", PeekEq[*wsv1.PollWorkflowTaskQueueRequest]("task_queue.kind", 2)) {
				Send[*wsv1.PollWorkflowTaskQueueRequest](s)
				continue
			}
			Send[*wsv1.PollWorkflowTaskQueueRequest](s)
			if s.Maybe("empty", PeekEmpty[*wsv1.PollWorkflowTaskQueueResponse]("task_token")) {
				Send[*wsv1.PollWorkflowTaskQueueResponse](s)
				continue
			}
			break
		}
		Send[*wsv1.RespondWorkflowTaskCompletedRequest](s)
	}
	got := Explore(freshCfg, body, Bounds{Repeats: 1}).Mermaid()

	if n := count(got, "loop repeats from"); n != 1 {
		t.Errorf("drew %d loop box(es) for one loop with two exits back to its head:\n%s", n, got)
	}
	if n := count(got, "RespondWorkflowTaskCompletedRequest"); n != 1 {
		t.Errorf("the tail was drawn %d times, want once:\n%s", n, got)
	}
	if !strings.Contains(got, "alt empty") {
		t.Errorf("the inner branch was lost:\n%s", got)
	}
}

// TestLoopDetectionIsOffWithoutSourceSites: a hand-built Choice carries no Site,
// and guessing a loop from a repeated label alone would be wrong — one label is
// answered afresh on every pass through the loop it controls, but two unrelated
// branches may also share a name.
func TestLoopDetectionIsOffWithoutSourceSites(t *testing.T) {
	if got := branchID(Choice{Label: "again"}); got != "" {
		t.Errorf("a Choice with no Site produced the branch identity %q", got)
	}
	if got := branchID(Choice{Label: "again", Site: "spec.go:12"}); got == "" {
		t.Error("a Choice with a Site produced no branch identity")
	}
}

// TestEveryPassThroughOneLoopReportsTheSameSite is the invariant loop detection
// rests on. That the Site is the specification's own line is checked where a
// specification really lives, in specs/polling: these helpers are inside package
// spec, so callSite walks past them and they all share one site.
func TestEveryPassThroughOneLoopReportsTheSameSite(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{Repeats: 3})
	deepest := ex.Paths[len(ex.Paths)-1].Result.Choices
	if len(deepest) < 3 {
		t.Fatalf("expected the deepest path to branch at least three times, got %d", len(deepest))
	}

	for _, c := range deepest {
		if c.Site == "" {
			t.Fatal("a recorded branch carries no site, so loop detection would be off")
		}
		if c.Site != deepest[0].Site || c.Label != deepest[0].Label {
			t.Errorf("one loop reported two identities, %q and %q",
				branchID(deepest[0]), branchID(c))
		}
	}
}

// TestDistinctBranchesGetDistinctIdentities: conflating two branches would make an
// ordinary fork look like a loop coming back round.
func TestDistinctBranchesGetDistinctIdentities(t *testing.T) {
	body := func(s *Session[testCfg]) {
		if s.Maybe("first", PeekEmpty[*wsv1.PollWorkflowTaskQueueResponse]("task_token")) {
			Send[*wsv1.PollWorkflowTaskQueueRequest](s)
		}
		if s.Maybe("second", PeekEmpty[*wsv1.PollWorkflowTaskQueueResponse]("task_token")) {
			Send[*wsv1.PollWorkflowTaskQueueRequest](s)
		}
	}
	ex := Explore(freshCfg, body, Bounds{})

	ids := map[string]bool{}
	for _, p := range ex.Paths {
		for _, c := range p.Result.Choices {
			ids[branchID(c)] = true
		}
	}
	if len(ids) != 2 {
		t.Errorf("two branches produced %d identity/identities: %v", len(ids), ids)
	}
	if got := ex.Mermaid(); strings.Contains(got, "loop ") {
		t.Errorf("two separate branches were drawn as a loop:\n%s", got)
	}
}
