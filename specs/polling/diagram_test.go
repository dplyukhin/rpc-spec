package polling

import (
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/rpcspec/spec"
)

func factory(configure func(*Config)) func() *Config {
	return func() *Config {
		c := New("default", "rpcspec-tq", "TrivialWorkflow", "wf-1", "run-1")
		if configure != nil {
			configure(c)
		}
		return c
	}
}

// TestSpecificationGeneratesEveryPathItPermits is a check on the specification
// rather than on the explorer. Nothing is being checked against a peer here, so a
// failure means the specification cannot produce its own side of some conversation
// it allows — a constraint that cannot be solved, or a branch that leads somewhere
// it did not anticipate. Running against one real Worker only ever exercises the
// path that Worker happened to take; this covers the rest.
func TestSpecificationGeneratesEveryPathItPermits(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*Config)
	}{
		{"the bare polling conversation", nil},
		{"with the client call that starts the Workflow", func(c *Config) { c.StartWorkflow = true }},
		{"with an ignorable event of a type this API cannot name", func(c *Config) {
			c.History = append(c.History, Event{Type: 9999, MayIgnore: true})
		}},
		{"with a non-ignorable event of a type this API cannot name", func(c *Config) {
			c.History = append(c.History, Event{Type: 9999, MayIgnore: false})
		}},
		{"with a non-ignorable event of a named type", func(c *Config) {
			c.History = append(c.History, Event{
				Type:      int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED),
				MayIgnore: false,
			})
		}},
		{"with the Worker declared not to support the event", func(c *Config) {
			c.History = append(c.History, Event{Type: 9999, MayIgnore: false})
			c.EventSupport[9999] = false
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex := spec.Explore(factory(tc.configure), Conversation, spec.Bounds{Repeats: 1})

			for _, p := range ex.Failed() {
				t.Errorf("the specification could not generate a conversation it permits:\n%v", p.Err)
			}
			if len(ex.Paths) == 0 {
				t.Fatal("no paths were explored at all")
			}
			if !ex.Complete {
				t.Errorf("the search was cut short at max-paths, so this proves less than it looks: %d paths",
					len(ex.Paths))
			}
			t.Logf("%d path(s), %d branch(es) not followed", len(ex.Paths), ex.Pruned)
		})
	}
}

// TestUnknownEventBranchIsDrawnAsAnAlternative: the rule the prototype exists to
// demonstrate has to appear in the diagram as a genuine fork, not as whichever arm
// one particular Worker happened to take.
func TestUnknownEventBranchIsDrawnAsAnAlternative(t *testing.T) {
	ex := spec.Explore(
		factory(func(c *Config) {
			c.History = append(c.History, Event{Type: 9999, MayIgnore: false})
		}),
		Conversation,
		// Both loop branches closed off, leaving only the conversation's spine.
		spec.Bounds{RepeatsByLabel: map[string]int{"sticky-poll": 0, "long-poll-timeout": 0}},
	)
	got := spec.MermaidOptions{Detail: Detail}.RenderExploration(ex)

	if !strings.Contains(got, "alt worker-cannot-handle-event") {
		t.Fatalf("the unknown-event rule is not drawn as a fork:\n%s", got)
	}
	for _, want := range []string{
		"RespondWorkflowTaskFailedRequest",
		"else not worker-cannot-handle-event",
		"RespondWorkflowTaskCompletedRequest",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the diagram is missing %q:\n%s", want, got)
		}
	}
	// Both loop branches are closed off, so both must be named as unfollowed.
	flat := strings.ReplaceAll(got, "<br/>", " ")
	for _, label := range []string{"sticky-poll", "long-poll-timeout"} {
		if !strings.Contains(flat, "branch not followed: "+label+" = true") {
			t.Errorf("the %s loop was dropped without saying so:\n%s", label, got)
		}
	}
}

// TestDeclaredSupportRemovesTheForkFromTheDiagram is the parameterized mode seen
// from the drawing: telling the specification which Worker is in front of it turns
// two permitted reactions into one required one, and the alt disappears.
func TestDeclaredSupportRemovesTheForkFromTheDiagram(t *testing.T) {
	bounds := spec.Bounds{RepeatsByLabel: map[string]int{"sticky-poll": 0, "long-poll-timeout": 0}}
	ex := spec.Explore(
		factory(func(c *Config) {
			c.History = append(c.History, Event{Type: 9999, MayIgnore: false})
			c.EventSupport[9999] = false
		}),
		Conversation, bounds)
	got := spec.MermaidOptions{}.RenderExploration(ex)

	if strings.Contains(got, "worker-cannot-handle-event") {
		t.Errorf("a declared supported-event set left the branch in the diagram:\n%s", got)
	}
	if !strings.Contains(got, "RespondWorkflowTaskFailedRequest") {
		t.Errorf("the required refusal is not drawn:\n%s", got)
	}
	if strings.Contains(got, "RespondWorkflowTaskCompletedRequest") {
		t.Errorf("a completion the specification forbids is drawn:\n%s", got)
	}
}
