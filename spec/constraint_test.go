package spec

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	wsv1 "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

// Every constraint must be both checkable and solvable — that is the property
// that lets one specification run in two directions. This table walks the whole
// closed set: solve a constraint into an empty message, then check the same
// constraint against the result. Anything that cannot do both round trip is not
// admissible in the sublanguage.
func TestEveryConstraintSolvesThenChecks(t *testing.T) {
	cases := []struct {
		name string
		msg  func() proto.Message
		c    Constraint
	}{
		{"Eq string", newPollResp, Eq("workflow_type.name", "Trivial")},
		{"Eq bytes", newPollResp, Eq("task_token", []byte("tok"))},
		{"Eq int32", newPollResp, Eq("attempt", int32(1))},
		{"Eq int64", newPollResp, Eq("started_event_id", int64(3))},
		{"Eq bool", newEvent, Eq("worker_may_ignore", true)},
		{"Eq enum", newEvent, Eq("event_type", enumspb.EVENT_TYPE_TIMER_FIRED)},
		{"Eq duration", newEvent, Eq(
			"workflow_execution_started_event_attributes.workflow_task_timeout", 7*time.Second)},
		{"OneOfValues", newPollReq, OneOfValues("task_queue.kind",
			enumspb.TASK_QUEUE_KIND_STICKY, enumspb.TASK_QUEUE_KIND_NORMAL)},
		{"NonEmpty bytes", newPollResp, NonEmpty("task_token")},
		{"NonEmpty string", newPollReq, NonEmpty("identity")},
		{"Present message", newEvent, Present("workflow_task_started_event_attributes")},
		{"Empty", newPollResp, Empty("next_page_token")},
		{"Absent", newPollResp, Absent("history")},
		{"EmptyList", newRespondResp, EmptyList("activity_tasks")},
		{"Opaque", newEvent, Opaque("event_time")},
		{"PositiveDuration", newEvent, PositiveDuration(
			"workflow_execution_started_event_attributes.workflow_task_timeout")},
		{"UnknownEnum", newEvent, UnknownEnum("event_type", 9999)},
		{"UnknownField", newEvent, UnknownField(31337, []byte("x"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.msg()
			if p := tc.c.solve(m.ProtoReflect(), &genctx{seed: new(int)}); p != nil {
				t.Fatalf("solving %s: %s", tc.c.describe(), p.detail)
			}
			// Round-trip through the wire, so what is checked is what a peer would
			// actually receive rather than an in-memory artefact.
			wire, err := proto.Marshal(m)
			if err != nil {
				t.Fatalf("marshalling: %v", err)
			}
			back := tc.msg()
			if err := proto.Unmarshal(wire, back); err != nil {
				t.Fatalf("unmarshalling: %v", err)
			}
			if p := tc.c.check(back.ProtoReflect()); p != nil {
				t.Fatalf("checking %s after solving it: %s", tc.c.describe(), p.detail)
			}
		})
	}
}

// Counter is separate because it advances shared state, so solving and checking
// have to be pointed at the same starting value.
func TestCounterSolvesThenChecks(t *testing.T) {
	next := int64(7)
	ev := &historypb.HistoryEvent{}
	if p := Counter("event_id", &next).solve(ev.ProtoReflect(), &genctx{seed: new(int)}); p != nil {
		t.Fatalf("solving Counter: %s", p.detail)
	}
	if ev.EventId != 7 {
		t.Fatalf("event_id is %d, want 7", ev.EventId)
	}
	if next != 8 {
		t.Fatalf("the counter is at %d, want 8", next)
	}

	next = 7
	if p := Counter("event_id", &next).check(ev.ProtoReflect()); p != nil {
		t.Fatalf("checking Counter: %s", p.detail)
	}
	if next != 8 {
		t.Fatalf("the counter is at %d after checking, want 8", next)
	}

	// A gap is what the constraint exists to catch.
	next = 9
	if p := Counter("event_id", &next).check(ev.ProtoReflect()); p == nil {
		t.Fatal("Counter accepted a non-contiguous value")
	}
}

// LengthBetween is a check-only clause: a list's length follows from what the
// nested specification appends, so there is nothing for it to solve.
func TestLengthBetween(t *testing.T) {
	h := &historypb.History{Events: []*historypb.HistoryEvent{{}, {}}}
	if p := LengthBetween("events", 1, 3).check(h.ProtoReflect()); p != nil {
		t.Fatalf("checking LengthBetween: %s", p.detail)
	}
	if p := LengthBetween("events", 3, 4).check(h.ProtoReflect()); p == nil {
		t.Fatal("LengthBetween accepted a list outside its bounds")
	}
}

func newPollReq() proto.Message     { return &wsv1.PollWorkflowTaskQueueRequest{} }
func newPollResp() proto.Message    { return &wsv1.PollWorkflowTaskQueueResponse{} }
func newRespondResp() proto.Message { return &wsv1.RespondWorkflowTaskCompletedResponse{} }
func newEvent() proto.Message       { return &historypb.HistoryEvent{} }
