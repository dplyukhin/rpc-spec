package spec

import (
	"strings"
	"testing"

	wsv1 "go.temporal.io/api/workflowservice/v1"

	"google.golang.org/protobuf/proto"
)

// entry builds a transcript entry the way Send would, so these tests exercise the
// same Role derivation a real run uses rather than a hand-written Role.
func entry(t *testing.T, seq int, m proto.Message, mode Mode) Entry {
	t.Helper()
	role, err := roleOf(m.ProtoReflect().Descriptor().FullName())
	if err != nil {
		t.Fatalf("roleOf: %v", err)
	}
	return Entry{Seq: seq, Role: role, Mode: mode, Msg: m}
}

// mockServerRun is the shape of a run against a real Worker: the Worker's messages
// are checked and the Server's are generated.
func mockServerRun(t *testing.T) *Result {
	t.Helper()
	return &Result{Transcript: []Entry{
		entry(t, 1, &wsv1.StartWorkflowExecutionRequest{}, Check),
		entry(t, 2, &wsv1.StartWorkflowExecutionResponse{}, Synth),
		entry(t, 3, &wsv1.PollWorkflowTaskQueueRequest{}, Check),
		entry(t, 4, &wsv1.PollWorkflowTaskQueueResponse{}, Synth),
		entry(t, 5, &wsv1.RespondWorkflowTaskCompletedRequest{}, Check),
		entry(t, 6, &wsv1.RespondWorkflowTaskCompletedResponse{}, Synth),
	}}
}

func TestMermaidDrawsEveryMessageAsAnArrow(t *testing.T) {
	got := mockServerRun(t).Mermaid()

	if !strings.HasPrefix(got, "sequenceDiagram\n") {
		t.Fatalf("diagram does not open with the mermaid header:\n%s", got)
	}
	for _, want := range []string{
		"    Worker->>Server: PollWorkflowTaskQueueRequest\n",
		"    Server-->>Worker: PollWorkflowTaskQueueResponse\n",
		"    Worker->>Server: RespondWorkflowTaskCompletedRequest\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("diagram is missing %q:\n%s", want, got)
		}
	}
	// One arrow per transcript entry, and nothing else that looks like one.
	if n := strings.Count(got, "->>"); n != 6 {
		t.Errorf("drew %d arrows, want one per message (6):\n%s", n, got)
	}
}

// TestMermaidAnswersTheClientCallToTheClient is the case Direction alone gets
// wrong: a StartWorkflowExecutionResponse travels ToSDK, but it is answering the
// Client that made the call, not the Worker that is polling.
func TestMermaidAnswersTheClientCallToTheClient(t *testing.T) {
	got := mockServerRun(t).Mermaid()

	if !strings.Contains(got, "    Client->>Server: StartWorkflowExecutionRequest\n") {
		t.Errorf("the client call is not drawn from the Client:\n%s", got)
	}
	if !strings.Contains(got, "    Server-->>Client: StartWorkflowExecutionResponse\n") {
		t.Errorf("the client call's reply is not drawn back to the Client:\n%s", got)
	}
	if strings.Contains(got, "Worker: StartWorkflowExecutionResponse") {
		t.Errorf("the client call's reply was drawn to the Worker:\n%s", got)
	}
}

// TestMermaidLabelsTheMockedSide records which party the runner played, which is
// the one thing a diagram of a transcript knows that an ordinary trace does not.
func TestMermaidLabelsTheMockedSide(t *testing.T) {
	server := mockServerRun(t).Mermaid()
	if !strings.Contains(server, "participant Server as Server (mock)\n") {
		t.Errorf("a mock-Server run does not label the Server as mocked:\n%s", server)
	}
	if !strings.Contains(server, "participant Worker as Worker\n") {
		t.Errorf("a mock-Server run does not label the Worker as real:\n%s", server)
	}

	// The same specification run the other way: the Worker's messages are generated
	// and the Server's are checked.
	flipped := &Result{Transcript: []Entry{
		entry(t, 1, &wsv1.PollWorkflowTaskQueueRequest{}, Synth),
		entry(t, 2, &wsv1.PollWorkflowTaskQueueResponse{}, Check),
	}}
	worker := flipped.Mermaid()
	if !strings.Contains(worker, "participant Worker as Worker (mock)\n") {
		t.Errorf("a mock-Worker run does not label the Worker as mocked:\n%s", worker)
	}
	if !strings.Contains(worker, "participant Server as Server\n") {
		t.Errorf("a mock-Worker run does not label the Server as real:\n%s", worker)
	}
}

// TestMermaidOrdersLifelinesAndOmitsAbsentOnes: a run with no client call must not
// draw a Client lifeline, and the Server always sits on the right.
func TestMermaidOrdersLifelinesAndOmitsAbsentOnes(t *testing.T) {
	got := (&Result{Transcript: []Entry{
		entry(t, 1, &wsv1.PollWorkflowTaskQueueRequest{}, Check),
		entry(t, 2, &wsv1.PollWorkflowTaskQueueResponse{}, Synth),
	}}).Mermaid()

	if strings.Contains(got, "participant Client") {
		t.Errorf("drew a Client lifeline for a run with no client call:\n%s", got)
	}
	w := strings.Index(got, "participant Worker")
	s := strings.Index(got, "participant Server")
	if w < 0 || s < 0 || w > s {
		t.Errorf("lifelines are not ordered Worker then Server:\n%s", got)
	}
}

// TestMermaidDetailDistinguishesMessagesOfOneType covers the readability problem a
// bare transcript has: an empty long poll and a delivered task are both a
// PollWorkflowTaskQueueResponse, and nothing in the type tells them apart.
func TestMermaidDetailDistinguishesMessagesOfOneType(t *testing.T) {
	res := &Result{Transcript: []Entry{
		entry(t, 1, &wsv1.PollWorkflowTaskQueueResponse{}, Synth),
		entry(t, 2, &wsv1.PollWorkflowTaskQueueResponse{TaskToken: []byte("t")}, Synth),
	}}
	got := MermaidOptions{Detail: func(e Entry) string {
		r, ok := e.Msg.(*wsv1.PollWorkflowTaskQueueResponse)
		if !ok || len(r.TaskToken) > 0 {
			return ""
		}
		return "(empty)"
	}}.Render(res)

	if !strings.Contains(got, "PollWorkflowTaskQueueResponse (empty)\n") {
		t.Errorf("the detail hook did not annotate the empty poll:\n%s", got)
	}
	if !strings.Contains(got, "PollWorkflowTaskQueueResponse\n") {
		t.Errorf("the detail hook annotated the delivered task:\n%s", got)
	}
}

func TestMermaidEscapesTextThatWouldBreakTheDiagram(t *testing.T) {
	res := &Result{Transcript: []Entry{entry(t, 1, &wsv1.PollWorkflowTaskQueueRequest{}, Check)}}
	got := MermaidOptions{
		Title:  "a; b # c",
		Detail: func(Entry) string { return "one\ntwo" },
	}.Render(res)

	if strings.Contains(got, "a; b # c") {
		t.Errorf("the title was not escaped:\n%s", got)
	}
	if !strings.Contains(got, "one<br/>two") {
		t.Errorf("a newline in a label was not turned into a break:\n%s", got)
	}
	// The arrow and its label must still be on one line.
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if strings.Contains(line, "->>") && !strings.Contains(line, ":") {
			t.Errorf("an arrow line lost its label: %q", line)
		}
	}
}

// TestMermaidNotesTheBranchAndItsGrounds: a run took one arm of each branch, so
// there is no alt block to draw — but which arm, and why, is the thing that
// explains two arrows of the same message type.
func TestMermaidNotesTheBranchAndItsGrounds(t *testing.T) {
	res := &Result{
		Transcript: []Entry{
			entry(t, 1, &wsv1.PollWorkflowTaskQueueRequest{}, Check),
			entry(t, 2, &wsv1.PollWorkflowTaskQueueResponse{}, Synth),
		},
		Choices: []Choice{{
			AfterSeq: 1, Label: "long-poll-timeout", Taken: true,
			FromLookahead: true,
			Describe:      `next PollWorkflowTaskQueueResponse has empty "task_token"`,
		}},
	}
	got := res.Mermaid()

	want := `Note over Worker,Server: long-poll-timeout = true ` +
		`lookahead: next PollWorkflowTaskQueueResponse has empty "task_token"`
	if !strings.Contains(unwrap(got), want) {
		t.Errorf("the branch note is missing or worded differently:\nwant %q\ngot:\n%s", want, got)
	}
	// The note sits between the request it followed and the response it selected.
	req := strings.Index(got, "PollWorkflowTaskQueueRequest")
	note := strings.Index(got, "Note over")
	resp := strings.Index(got, "Server-->>Worker")
	if !(req < note && note < resp) {
		t.Errorf("the branch note is out of order among the messages:\n%s", got)
	}
}

// TestMermaidNamesTheQuestionNotTheAnswer: on the arm where a branch was *not*
// taken the lookahead's words are false, so they have to be presented as the
// question the branch turned on rather than written bare as though they held.
func TestMermaidNamesTheQuestionNotTheAnswer(t *testing.T) {
	res := &Result{
		Transcript: []Entry{entry(t, 1, &wsv1.PollWorkflowTaskQueueRequest{}, Check)},
		Choices: []Choice{{
			Label: "sticky-poll", Taken: false, FromLookahead: true,
			Describe: "next PollWorkflowTaskQueueRequest has task_queue.kind == Sticky(2)",
		}},
	}
	got := res.Mermaid()

	want := "sticky-poll = false lookahead: next PollWorkflowTaskQueueRequest has task_queue.kind == Sticky(2)"
	if !strings.Contains(unwrap(got), want) {
		t.Errorf("a branch that was not taken is worded as an assertion:\nwant %q\ngot:\n%s", want, got)
	}
}

// TestMermaidWrapsNotesSoTheyFitTheDiagram: a mermaid note is as wide as its
// longest line, and the lookahead descriptions run well past 120 characters, so an
// unwrapped note pushes off both edges of the drawing.
func TestMermaidWrapsNotesSoTheyFitTheDiagram(t *testing.T) {
	res := &Result{
		Transcript: []Entry{entry(t, 1, &wsv1.RespondWorkflowTaskFailedRequest{}, Check)},
		Choices: []Choice{{
			Label: "worker-cannot-handle-event", Taken: true, FromLookahead: true,
			Describe: "next message is a RespondWorkflowTaskFailedRequest rather than " +
				"a RespondWorkflowTaskCompletedRequest",
		}},
	}
	got := res.Mermaid()

	if !strings.Contains(got, "<br/>") {
		t.Fatalf("a note far past the wrap width was not broken:\n%s", got)
	}
	for _, line := range strings.Split(got, "\n") {
		if !strings.Contains(line, "Note over") {
			continue
		}
		for _, part := range strings.Split(strings.SplitN(line, ": ", 2)[1], "<br/>") {
			// Only an unbreakable single word may exceed the width; message type names
			// are long and splitting one would make it unrecognizable.
			if len(part) > noteWidth && len(strings.Fields(part)) > 1 {
				t.Errorf("note line is %d wide with a break available: %q", len(part), part)
			}
		}
	}
}

// unwrap flattens a rendered diagram's line breaks so an assertion on note text
// does not depend on where the wrap happens to fall.
func unwrap(s string) string { return strings.ReplaceAll(s, "<br/>", " ") }

// TestMermaidSaysWhenTheRunnerChoseRatherThanObserved: in synthesizer mode nothing
// on the wire decided the branch, and the diagram must not imply otherwise.
func TestMermaidSaysWhenTheRunnerChoseRatherThanObserved(t *testing.T) {
	res := &Result{
		Transcript: []Entry{entry(t, 1, &wsv1.PollWorkflowTaskQueueRequest{}, Synth)},
		Choices: []Choice{{
			Label: "sticky-poll", Taken: false, FromLookahead: false,
			Describe: "next PollWorkflowTaskQueueRequest has task_queue.kind == 2",
		}},
	}
	got := res.Mermaid()

	if !strings.Contains(unwrap(got), "sticky-poll = false policy") {
		t.Errorf("a policy decision is not marked as one:\n%s", got)
	}
	if strings.Contains(got, "task_queue.kind == 2") {
		t.Errorf("a policy decision cites a lookahead that never ran:\n%s", got)
	}
}

func TestMermaidChoiceNotesCanBeSuppressed(t *testing.T) {
	res := &Result{
		Transcript: []Entry{entry(t, 1, &wsv1.PollWorkflowTaskQueueRequest{}, Check)},
		Choices:    []Choice{{Label: "sticky-poll"}},
	}
	if strings.Contains(MermaidOptions{HideChoices: true}.Render(res), "Note over") {
		t.Error("HideChoices did not suppress the branch notes")
	}
	if !strings.Contains(MermaidOptions{}.Render(res), "Note over") {
		t.Error("branch notes should be drawn by default")
	}
}

func TestMermaidAutonumberIsOptIn(t *testing.T) {
	res := mockServerRun(t)
	if !strings.Contains(res.Mermaid(), "\n    autonumber\n") {
		t.Error("Mermaid() should number the arrows so they line up with Seq")
	}
	if strings.Contains(MermaidOptions{}.Render(res), "autonumber") {
		t.Error("the zero options value should not number the arrows")
	}
}
