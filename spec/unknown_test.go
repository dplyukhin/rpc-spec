package spec

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

// The claim that motivates Go as the host language is that values which do not
// exist yet can be constructed and put on the wire. These tests check that
// directly, without a Server or an SDK in the picture.

func TestUnknownEnumSurvivesTheWire(t *testing.T) {
	const number = 9999
	if v := enumspb.EventType_name[number]; v != "" {
		t.Fatalf("EventType %d is already named %q, so it is not a future value", number, v)
	}

	ev := &historypb.HistoryEvent{}
	if p := UnknownEnum("event_type", number).solve(ev.ProtoReflect(), &genctx{seed: new(int)}); p != nil {
		t.Fatalf("writing an unknown enum: %s", p.detail)
	}

	wire, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	var back historypb.HistoryEvent
	if err := proto.Unmarshal(wire, &back); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}
	if got := int32(back.EventType); got != number {
		t.Fatalf("event_type round-tripped as %d, want %d", got, number)
	}

	// And the checker side agrees, so the same clause can assert that a peer
	// produced a value it could not have understood.
	if p := UnknownEnum("event_type", number).check(back.ProtoReflect()); p != nil {
		t.Fatalf("checking an unknown enum: %s", p.detail)
	}
}

func TestUnknownEnumRefusesAKnownNumber(t *testing.T) {
	known := int32(enumspb.EVENT_TYPE_TIMER_FIRED)
	ev := &historypb.HistoryEvent{}
	p := UnknownEnum("event_type", known).solve(ev.ProtoReflect(), &genctx{seed: new(int)})
	if p == nil {
		t.Fatalf("UnknownEnum accepted %d, which is a named value", known)
	}
	t.Logf("refused, as it should be: %s", p.detail)
}

// TestEnumHasName covers the query a specification uses to tell an event type it
// may require every peer to handle from one it may not, when the number arrives as
// a parameter rather than as a constant.
func TestEnumHasName(t *testing.T) {
	if !EnumHasName[enumspb.EventType](int32(enumspb.EVENT_TYPE_TIMER_FIRED)) {
		t.Error("TIMER_FIRED has a name in this API, but EnumHasName says it does not")
	}
	if EnumHasName[enumspb.EventType](9999) {
		t.Error("event type 9999 has no name in this API, but EnumHasName says it does")
	}
	// The query is per enum: a number named in one enum is not thereby named in
	// another, so the answer depends on the type argument and not just the number.
	timerFired := int32(enumspb.EVENT_TYPE_TIMER_FIRED) // 18; TaskQueueKind stops at 3
	if !EnumHasName[enumspb.EventType](timerFired) || EnumHasName[enumspb.TaskQueueKind](timerFired) {
		t.Errorf("%d should be named as an EventType and nameless as a TaskQueueKind", timerFired)
	}
}

func TestUnknownFieldSurvivesTheWire(t *testing.T) {
	const number = 31337
	ev := &historypb.HistoryEvent{EventId: 1}
	c := UnknownField(number, []byte("from the future"))
	if p := c.solve(ev.ProtoReflect(), &genctx{seed: new(int)}); p != nil {
		t.Fatalf("writing an unknown field: %s", p.detail)
	}

	wire, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	var back historypb.HistoryEvent
	if err := proto.Unmarshal(wire, &back); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}
	if back.EventId != 1 {
		t.Errorf("event_id round-tripped as %d, want 1", back.EventId)
	}
	if p := c.check(back.ProtoReflect()); p != nil {
		t.Fatalf("the unknown field did not survive: %s", p.detail)
	}
}

// TestFieldPathsAreValidatedAgainstDescriptors covers the mitigation for paths
// being strings: a typo is a startup-time error naming the message and the field,
// not a silent pass.
func TestFieldPathsAreValidatedAgainstDescriptors(t *testing.T) {
	ev := &historypb.HistoryEvent{}
	p := Eq("worker_may_ignor", true).check(ev.ProtoReflect())
	if p == nil {
		t.Fatal("a misspelled field path was accepted")
	}
	t.Logf("rejected, as it should be: %s", p.detail)
}
