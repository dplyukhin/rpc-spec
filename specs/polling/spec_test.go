package polling

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
)

// The supported-event-type set is the one piece of specification state that is not
// exercised end to end by a conversation: the reaction it forbids on a retried task
// is silence, which no transport here can observe. These tests cover it directly.

const (
	nameless = 9999 // no name in the API this specification was compiled against
	named    = int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED)
	alsoName = int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED)
)

func fresh() *Config {
	return New("default", "tq", "W", "wf", "run")
}

func TestFloorEventsRequireCompletion(t *testing.T) {
	c := fresh()
	if got, cands := c.reaction(); got != MustComplete || cands != nil {
		t.Fatalf("a history of nothing but floor events gave %v/%v, want MustComplete/none", got, cands)
	}
}

func TestIgnorableEventTakesNoPart(t *testing.T) {
	c := fresh()
	c.History = append(c.History, Event{Type: nameless, MayIgnore: true})
	if got, _ := c.reaction(); got != MustComplete {
		t.Fatalf("an ignorable event gave %v, want MustComplete: it may be skipped, so the reply is not in doubt", got)
	}
	if _, known := c.EventSupport[nameless]; known {
		t.Error("an ignorable event must teach nothing about the supported set")
	}
}

func TestUnknownSupportIsABranchThatThenBinds(t *testing.T) {
	c := fresh()
	c.History = append(c.History, Event{Type: nameless, MayIgnore: false})

	got, cands := c.reaction()
	if got != EitherReaction || len(cands) != 1 || cands[0] != nameless {
		t.Fatalf("first sight of an unsupported-or-not event gave %v/%v, want EitherReaction/[%d]",
			got, cands, nameless)
	}

	// The Worker refused it, so it does not support it — and may not accept the same
	// event later in the execution.
	c.learn(cands, false)
	if got, _ := c.reaction(); got != MustFail {
		t.Fatalf("after a refusal the same event gave %v, want MustFail", got)
	}
}

func TestSupportLearnedFromACompletionBinds(t *testing.T) {
	c := fresh()
	c.History = append(c.History, Event{Type: named, MayIgnore: false})

	_, cands := c.reaction()
	// A completion proves support: a Worker that did not support the event would have
	// been obliged to fail rather than complete.
	c.learn(cands, true)
	if !c.EventSupport[named] {
		t.Fatal("a completion must record the candidate as supported")
	}
	if got, _ := c.reaction(); got != MustComplete {
		t.Fatalf("after a completion the same event gave %v, want MustComplete", got)
	}
}

// TestARefusalDoesNotAttributeAcrossSeveralCandidates covers the asymmetry in what
// a reply proves. A completion speaks for every candidate; a refusal says only that
// at least one is unsupported, and guessing which would be unsound.
func TestARefusalDoesNotAttributeAcrossSeveralCandidates(t *testing.T) {
	c := fresh()
	c.History = append(c.History,
		Event{Type: named, MayIgnore: false},
		Event{Type: alsoName, MayIgnore: false})

	got, cands := c.reaction()
	if got != EitherReaction || len(cands) != 2 {
		t.Fatalf("two candidates gave %v/%v, want EitherReaction with both", got, cands)
	}

	c.learn(cands, false)
	if len(c.EventSupport) != len(requiredEventTypes) {
		t.Errorf("a refusal with two candidates recorded something: %v", c.EventSupport)
	}
	if got, _ := c.reaction(); got != EitherReaction {
		t.Fatalf("with nothing attributable the reaction became %v, want EitherReaction still", got)
	}
}

// TestDeclaredSupportRemovesTheBranch is the parameterized mode: a run that knows
// which Worker it launched can say so, and then every reaction is required rather
// than discovered.
func TestDeclaredSupportRemovesTheBranch(t *testing.T) {
	c := fresh()
	c.History = append(c.History, Event{Type: named, MayIgnore: false})

	c.EventSupport[named] = false
	if got, _ := c.reaction(); got != MustFail {
		t.Fatalf("a Worker declared not to support the event gave %v, want MustFail", got)
	}

	c.EventSupport[named] = true
	if got, _ := c.reaction(); got != MustComplete {
		t.Fatalf("a Worker declared to support the event gave %v, want MustComplete", got)
	}
}

// TestFailTaskKeepsTheServerStateInStep: the Server records the failure and
// schedules the task again, so the History and the attempt counter move even though
// the conversation stops there.
func TestFailTaskKeepsTheServerStateInStep(t *testing.T) {
	c := fresh()
	before := len(c.History)
	c.History = append(c.History, TaskFailed(), TaskScheduled())
	c.Attempt++

	if c.Attempt != 2 {
		t.Errorf("attempt is %d, want 2", c.Attempt)
	}
	if len(c.History) != before+2 {
		t.Errorf("history grew by %d events, want 2", len(c.History)-before)
	}
	if got, want := c.startedEventID(), int64(len(c.History)+1); got != want {
		t.Errorf("started_event_id for the retry is %d, want %d", got, want)
	}
}
