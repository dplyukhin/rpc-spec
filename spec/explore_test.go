package spec

import (
	"strings"
	"testing"

	wsv1 "go.temporal.io/api/workflowservice/v1"
)

// The specifications here are deliberately tiny. What is being tested is the
// search — that it reaches each path exactly once, re-executes from a clean start,
// and says what it left out — not anything about Temporal.

type testCfg struct{ rounds int }

// loopSpec is the shape that makes bounds necessary: a branch that keeps saying
// yes never reaches the end.
func loopSpec(s *Session[testCfg]) {
	for s.Maybe("again", PeekEmpty[*wsv1.PollWorkflowTaskQueueResponse]("task_token")) {
		Send[*wsv1.PollWorkflowTaskQueueRequest](s)
		Send[*wsv1.PollWorkflowTaskQueueResponse](s)
		s.C.rounds++
	}
	Send[*wsv1.RespondWorkflowTaskCompletedRequest](s)
	Send[*wsv1.RespondWorkflowTaskCompletedResponse](s)
}

func freshCfg() *testCfg { return &testCfg{} }

// pattern renders a path's choices as a string, so a test can name the set of
// paths it expects.
func pattern(p Path) string {
	var b strings.Builder
	for _, c := range p.Result.Choices {
		if c.Taken {
			b.WriteByte('T')
		} else {
			b.WriteByte('F')
		}
	}
	if p.Truncated {
		b.WriteByte('!')
	}
	return b.String()
}

func patterns(ex *Exploration) []string {
	out := make([]string, 0, len(ex.Paths))
	for _, p := range ex.Paths {
		out = append(out, pattern(p))
	}
	return out
}

func assertPaths(t *testing.T, ex *Exploration, want ...string) {
	t.Helper()
	got := patterns(ex)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("explored %v, want %v", got, want)
	}
	seen := map[string]bool{}
	for _, g := range got {
		if seen[g] {
			t.Errorf("path %q was explored more than once: %v", g, got)
		}
		seen[g] = true
	}
}

// TestExploreUnrollsALoopToTheRepeatBound: the repeat bound is what makes a
// looping specification finite, and it is expressed in unrollings rather than in
// raw choice count because that is the unit a reader of the diagram cares about.
func TestExploreUnrollsALoopToTheRepeatBound(t *testing.T) {
	one := Explore(freshCfg, loopSpec, Bounds{Repeats: 1})
	assertPaths(t, one, "F", "TF")
	if !one.Complete {
		t.Error("the search reported itself incomplete when nothing cut it short")
	}
	if one.Truncated() != 0 {
		t.Errorf("%d path(s) hit a bound when the repeat bound should have ended them all", one.Truncated())
	}
	if one.Pruned == 0 {
		t.Error("the repeat bound declined no branches, so nothing was actually bounded")
	}

	three := Explore(freshCfg, loopSpec, Bounds{Repeats: 3})
	assertPaths(t, three, "F", "TF", "TTF", "TTTF")
}

// TestExploreRunsEachPathFromACleanStart is why Explore takes a factory rather
// than a configuration. A specification mutates its own state as it goes, so
// replaying a prefix into state another path had already moved would explore
// something the specification does not permit.
func TestExploreRunsEachPathFromACleanStart(t *testing.T) {
	var made []*testCfg
	newCfg := func() *testCfg {
		c := &testCfg{}
		made = append(made, c)
		return c
	}

	ex := Explore(newCfg, loopSpec, Bounds{Repeats: 3})
	if len(made) != len(ex.Paths) {
		t.Fatalf("built %d config(s) for %d path(s)", len(made), len(ex.Paths))
	}
	// Each path's loop count must be its own. A shared configuration would make these
	// accumulate across paths instead.
	for i, p := range ex.Paths {
		want := strings.Count(pattern(p), "T")
		if made[i].rounds != want {
			t.Errorf("path %q ran %d round(s), want %d — state leaked between paths",
				pattern(p), made[i].rounds, want)
		}
	}
}

// TestExploreMarksPathsItCutShort: a path that stopped at a bound is not a path
// that ended, and the difference has to survive into the output.
func TestExploreMarksPathsItCutShort(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{Repeats: 10, MaxChoices: 3})
	assertPaths(t, ex, "F", "TF", "TTF", "TTT!")

	if ex.Truncated() != 1 {
		t.Errorf("%d path(s) marked truncated, want 1", ex.Truncated())
	}
	// A truncated path still has the conversation it got through.
	for _, p := range ex.Paths {
		if p.Truncated && len(p.Result.Transcript) == 0 {
			t.Error("a truncated path lost the messages it did produce")
		}
		if p.Err != nil {
			t.Errorf("a bound was reported as a specification failure: %v", p.Err)
		}
	}
}

// TestExploreBoundsRunawayMessages covers the specification that loops without
// branching, which no choice bound can stop.
func TestExploreBoundsRunawayMessages(t *testing.T) {
	chatty := func(s *Session[testCfg]) {
		for {
			Send[*wsv1.PollWorkflowTaskQueueRequest](s)
			Send[*wsv1.PollWorkflowTaskQueueResponse](s)
		}
	}
	ex := Explore(freshCfg, chatty, Bounds{MaxMessages: 7})

	if len(ex.Paths) != 1 || !ex.Paths[0].Truncated {
		t.Fatalf("a specification that never stops talking gave %d path(s), truncated=%v",
			len(ex.Paths), ex.Paths[0].Truncated)
	}
	if n := len(ex.Paths[0].Result.Transcript); n != 7 {
		t.Errorf("kept %d message(s), want the 7 the bound allowed", n)
	}
}

func TestExploreReportsWhenTheSearchItselfWasCutShort(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{Repeats: 10, MaxChoices: 20, MaxPaths: 3})
	if len(ex.Paths) != 3 {
		t.Errorf("explored %d path(s), want the 3 the bound allowed", len(ex.Paths))
	}
	if ex.Complete {
		t.Error("the search hit MaxPaths but reported itself complete")
	}
}

func TestExploreHandlesASpecificationWithNoBranches(t *testing.T) {
	straight := func(s *Session[testCfg]) {
		Send[*wsv1.PollWorkflowTaskQueueRequest](s)
		Send[*wsv1.PollWorkflowTaskQueueResponse](s)
	}
	ex := Explore(freshCfg, straight, Bounds{})

	if len(ex.Paths) != 1 || !ex.Complete || ex.Truncated() != 0 {
		t.Fatalf("a branchless specification gave %d path(s), complete=%v, truncated=%d",
			len(ex.Paths), ex.Complete, ex.Truncated())
	}
	if len(ex.Paths[0].Result.Transcript) != 2 {
		t.Errorf("got %d message(s), want 2", len(ex.Paths[0].Result.Transcript))
	}
}

// TestExploreRepeatsByLabelCanCloseABranchEntirely is how a loop is kept out of a
// diagram altogether: the branch is still reported as unfollowed rather than
// silently dropped.
func TestExploreRepeatsByLabelCanCloseABranchEntirely(t *testing.T) {
	ex := Explore(freshCfg, loopSpec, Bounds{RepeatsByLabel: map[string]int{"again": 0}})
	assertPaths(t, ex, "F")
	if ex.Pruned != 1 {
		t.Errorf("pruned %d branch(es), want the 1 that was closed off", ex.Pruned)
	}
}

// TestExploreSurfacesASpecificationThatCannotGenerate: under the paper transport
// nothing is being checked, so a failure means the specification could not produce
// its own side. That is a bug in the specification and must not be reported as a
// bound.
func TestExploreSurfacesASpecificationThatCannotGenerate(t *testing.T) {
	broken := func(s *Session[testCfg]) {
		Send[*wsv1.PollWorkflowTaskQueueRequest](s, Eq("no_such_field", 1))
	}
	ex := Explore(freshCfg, broken, Bounds{})

	failed := ex.Failed()
	if len(failed) != 1 {
		t.Fatalf("%d path(s) failed, want 1", len(failed))
	}
	if failed[0].Truncated {
		t.Error("a specification failure was reported as a bound")
	}
}
