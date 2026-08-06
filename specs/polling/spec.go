// Package polling is a specification of the Workflow-task polling conversation
// between a Temporal SDK Worker and Temporal Server.
package polling

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	wsv1 "go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/rpcspec/spec"
)

// Config represents the current state of the conversation.
type Config struct {
	Namespace    string
	TaskQueue    string
	WorkflowType string
	WorkflowID   string
	RunID        string

	// StartWorkflow is set when the conversation includes the client call that
	// creates the Workflow.
	StartWorkflow bool

	// History is what the Server has stored for this Run when the conversation
	// begins. It is a parameter of the run, not something the conversation invents:
	// a Server does not make up History Events, it records what something caused,
	// and by the time a Worker polls, those causes have already happened.
	//
	// Parameterizing on the stored history is what lets the rule in Polling be
	// stated without pretending the Server fabricates an event. A history can hold
	// an event whose type this specification has no name for because a newer Worker's
	// commands, a client using a newer API, or a newer Server put it there before
	// this conversation began — the same setup the SDKs' own replay fixtures use,
	// where the history is a JSON file. Which of those causes it was is outside this
	// specification: modelling one as messages would need a second peer in the same
	// conversation, and this prototype runs a single specification process against a
	// single peer.
	History []Event

	// EventSupport is what is known about this Worker's own set of supported event
	// types: type number to whether the Worker handles it. Support is a property of
	// the Worker's build and is fixed for the whole execution — a Worker does not
	// learn or forget an event type between tasks — so this is a variable the
	// conversation solves for, not a choice it makes afresh at each task.
	//
	// It starts as the floor every Worker must handle (see requiredEventTypes). A run
	// that knows more about the Worker in front of it may say so up front, and then
	// every reaction is required rather than merely permitted:
	//
	//	cfg.EventSupport[int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED)] = false
	//
	// Whatever is left unstated is discovered from the Worker's first reply to a task
	// carrying the event, and enforced from then on.
	EventSupport map[int32]bool

	Phase       string // "new" | "running" | "closed"
	Attempt     int32  // which delivery of the current Workflow Task this is
	Poll        int    // outstanding long polls
	NextEventID int64
	OpenToken   []byte

	// TaskRejected is an outcome rather than an input: it records that the Worker
	// could not handle an event the History carried and failed the Workflow Task
	// rather than guessing.
	TaskRejected bool
}

// Event is one stored History Event, described only as far as the polling
// conversation cares.
type Event struct {
	// Type is event_type, held as a number rather than an enums.EventType because a
	// stored history may legitimately contain a type this specification has no name
	// for. Whether it has a name is a fact about the API this specification was
	// compiled against; whether a *Worker* recognizes it is a fact about that
	// Worker, and neither this field nor anything else here can state it.
	Type int32
	// MayIgnore is worker_may_ignore: the Server's statement about whether a Worker
	// that does not recognize this event can safely skip it.
	MayIgnore bool
	// Attrs constrains the event's attributes, where the specification models them.
	Attrs []spec.Constraint
}

// ExecutionStarted is the first event of any Run.
func ExecutionStarted(workflowType, taskQueue string) Event {
	return Event{
		Type: int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED),
		Attrs: []spec.Constraint{
			spec.Eq("workflow_execution_started_event_attributes.workflow_type.name", workflowType),
			spec.Eq("workflow_execution_started_event_attributes.task_queue.name", taskQueue),
			spec.PositiveDuration("workflow_execution_started_event_attributes.workflow_task_timeout"),
		},
	}
}

// TaskScheduled is the event that puts a Workflow Task on a task queue.
func TaskScheduled() Event {
	return Event{
		Type:  int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
		Attrs: []spec.Constraint{spec.Present("workflow_task_scheduled_event_attributes")},
	}
}

// TaskFailed is what a Server records when a Worker reports it could not process a
// task. The Server schedules another one after it, which is why FailTask appends
// both.
func TaskFailed() Event {
	return Event{
		Type:  int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED),
		Attrs: []spec.Constraint{spec.Present("workflow_task_failed_event_attributes")},
	}
}

// StartedHistory is what a Server holds for a Workflow that has just been started
// and whose first Workflow Task is waiting to be picked up. A run that wants a
// history something else has already added to appends to it.
func StartedHistory(workflowType, taskQueue string) []Event {
	return []Event{ExecutionStarted(workflowType, taskQueue), TaskScheduled()}
}

func New(namespace, taskQueue, workflowType, workflowID, runID string) *Config {
	c := &Config{
		Namespace: namespace, TaskQueue: taskQueue,
		WorkflowType: workflowType, WorkflowID: workflowID, RunID: runID,
		Phase: "new", NextEventID: 1, Attempt: 1,
		History:      StartedHistory(workflowType, taskQueue),
		EventSupport: make(map[int32]bool, len(requiredEventTypes)),
	}
	for t := range requiredEventTypes {
		c.EventSupport[t] = true
	}
	return c
}

// startedEventID is the event_id of the WorkflowTaskStarted event that answering
// the next poll creates: one past the stored History, since ids begin at 1.
func (c *Config) startedEventID() int64 { return int64(len(c.History)) + 1 }

// requiredEventTypes is the floor: the event types the contract requires every
// Worker to understand, because no Worker can process a Workflow Task without
// them. A history begins with WorkflowExecutionStarted, and the WorkflowTask*
// events bracket the tasks a replay walks through.
//
// It is deliberately an explicit list and not "every type this API has a name
// for". A Worker is as old as its build, and this specification may have been
// compiled long afterwards, so the names available here say nothing about what the
// Worker in front of us knows. Go SDK v1.47.0 has no handler for
// WORKFLOW_EXECUTION_OPTIONS_UPDATED even though the API this specification uses
// names it, and refusing such an event is exactly the behaviour the rule asks for.
//
// Everything above the floor is a feature some Worker may not have, so leaving a
// type out only *permits* a Worker to refuse it; it never obliges one to. Where the
// line really belongs is a contract decision that ought to be stated in the API
// alongside worker_may_ignore rather than invented in a specification; this is the
// prototype's reading of it.
var requiredEventTypes = map[int32]bool{
	int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED): true,
	int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED):    true,
	int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED):      true,
	int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED):    true,
	int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED):       true,
	int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT):    true,
}

// Reaction is what the specification requires of the Worker for the task it has
// just been handed.
type Reaction int

const (
	// MustComplete: every non-ignorable event in the task is one this Worker is known
	// to handle, so refusing the task would be a violation.
	MustComplete Reaction = iota
	// MustFail: the task carries a non-ignorable event this Worker is known not to
	// handle, so the rule obliges it to fail rather than guess.
	MustFail
	// EitherReaction: the task carries a non-ignorable event whose support is not yet
	// known. Nothing on the wire predicts the Worker's choice, and whichever it makes
	// tells the specification what this Worker's supported set contains — which is
	// then required for the rest of the execution.
	EitherReaction
)

// reaction reports what is required of the Worker for the task just delivered,
// together with the event types that task could reveal support for. The prototype
// delivers the whole History in every task, so every stored event is one the Worker
// has just been shown.
func (c *Config) reaction() (Reaction, []int32) {
	var unknown []int32
	unsupported := false
	for _, e := range c.History {
		if e.MayIgnore {
			// The Server has said a Worker that does not handle this event may skip it, so
			// the Worker's reply neither reveals nor is constrained by it.
			continue
		}
		if supported, known := c.EventSupport[e.Type]; known {
			if !supported {
				unsupported = true
			}
			continue
		}
		if !contains(unknown, e.Type) {
			unknown = append(unknown, e.Type)
		}
	}
	switch {
	case unsupported:
		// Already explained: the failure is attributable to the known type, so the task
		// reveals nothing about any other.
		return MustFail, nil
	case len(unknown) > 0:
		return EitherReaction, unknown
	default:
		return MustComplete, nil
	}
}

// learn records what the Worker's reply revealed about its supported set. A
// completion proves every candidate is supported, because a Worker that did not
// support one would have been obliged to fail. A failure proves only that *some*
// candidate is unsupported, which pins one down when the task carried a single
// candidate and says nothing about which it was when the task carried several.
func (c *Config) learn(candidates []int32, completed bool) {
	if completed {
		for _, t := range candidates {
			c.EventSupport[t] = true
		}
		return
	}
	if len(candidates) == 1 {
		c.EventSupport[candidates[0]] = false
	}
}

func contains(ts []int32, t int32) bool {
	for _, v := range ts {
		if v == t {
			return true
		}
	}
	return false
}

// Conversation is the whole modeled exchange.
func Conversation(s *spec.Session[Config]) {
	if s.C.StartWorkflow {
		StartWorkflow(s)
	}
	Polling(s)
}

// StartWorkflow is the client call that creates the Workflow. It is here so that
// the Worker has something to poll for.
func StartWorkflow(s *spec.Session[Config]) {
	// Client sends a StartWorkflowExecutionRequest
	spec.Send[*wsv1.StartWorkflowExecutionRequest](s,
		spec.Eq("namespace", s.C.Namespace),
		spec.Eq("workflow_id", s.C.WorkflowID),
		spec.Eq("workflow_type.name", s.C.WorkflowType),
		spec.Eq("task_queue.name", s.C.TaskQueue),
		spec.Eq("task_queue.kind", enumspb.TASK_QUEUE_KIND_NORMAL),
		spec.NonEmpty("request_id"),
		spec.NonEmpty("identity"))

	// Server responds with a StartWorkflowExecutionResponse
	started := spec.Send[*wsv1.StartWorkflowExecutionResponse](s,
		spec.NonEmpty("run_id"),
		spec.Absent("eager_workflow_task"))

	// Save the Run ID from the response
	s.C.RunID = started.RunId
}

func Polling(s *spec.Session[Config]) {
	for s.C.Phase != "closed" {
		PollUntilTask(s)

		// Which event types a Worker supports is a property of that Worker's build. The
		// Server does not know it, so neither does this specification — but it is fixed
		// for the whole execution, so it is a variable to be solved for rather than a
		// choice to be made again at every task. Config.EventSupport holds what is known
		// so far, and reaction turns that into what is required here:
		//
		//   - MustFail: the task carries a non-ignorable event this Worker has already
		//     shown it does not support. Having refused it once, it may not now accept
		//     it, so the branch is gone and the refusal is checked.
		//   - MustComplete: every non-ignorable event is one this Worker is known to
		//     support, so a refusal would be a violation.
		//   - EitherReaction: support for some event is not yet known. Nothing on the
		//     wire predicts the reply, so the Worker's own choice discriminates — and
		//     learn writes down what that choice revealed, which the two cases above
		//     then enforce for the rest of the execution.
		//
		// Ignorable events take no part in any of this: supported or not, the task is
		// completed, so the reply reveals nothing about them and requires nothing of
		// them.
		//
		// Both failing paths end the conversation, and not because there is nothing
		// left to require. The Server records the failure and schedules the task
		// again — FailTask keeps the History in step — but a Worker reports a Workflow
		// Task failure only on the first attempt and lets later attempts time out
		// instead, deliberately, to avoid spinning on a task it cannot process. Go,
		// Java and Core all do this:
		//
		//	// Only fail workflow task on first attempt, subsequent failure on the same
		//	// workflow task will timeout. — go.temporal.io/sdk internal_task_pollers.go
		//	// Only fail workflow task on the first attempt — WorkflowWorker.java
		//	// we shouldn't report it. EX: We have failed 2 or more attempts — sdk-core
		//
		// So consistency on a retried task shows up on the wire as silence, and
		// checking silence needs a time model this prototype does not have. What is
		// checkable is the other direction: once a Worker has shown it supports an
		// event type, MustComplete holds it to that for the rest of the execution.
		switch req, candidates := s.C.reaction(); req {
		case MustFail:
			FailTask(s)
			return

		case MustComplete:
			CompleteTask(s)

		default:
			if s.Maybe("worker-cannot-handle-event",
				spec.PeekIsType[*wsv1.RespondWorkflowTaskFailedRequest](
					&wsv1.RespondWorkflowTaskCompletedRequest{})) {
				FailTask(s)
				s.C.learn(candidates, false)
				return
			}
			CompleteTask(s)
			s.C.learn(candidates, true)
		}
	}
}

// PollUntilTask polls until a poll yields a Workflow Task.
func PollUntilTask(s *spec.Session[Config]) *wsv1.PollWorkflowTaskQueueResponse {
	for {
		// A Worker with a Workflow cache polls its own sticky queue as well as the
		// shared one.
		if s.Maybe("sticky-poll", spec.PeekEq[*wsv1.PollWorkflowTaskQueueRequest](
			"task_queue.kind", enumspb.TASK_QUEUE_KIND_STICKY)) {
			spec.Send[*wsv1.PollWorkflowTaskQueueRequest](s,
				spec.Eq("namespace", s.C.Namespace),
				spec.Eq("task_queue.kind", enumspb.TASK_QUEUE_KIND_STICKY),
				spec.Eq("task_queue.normal_name", s.C.TaskQueue),
				spec.NonEmpty("task_queue.name"),
				spec.NonEmpty("identity"))
			s.C.Poll++
			// TODO(Dan) Does this mean the empty poll response arrives before the next poll request is sent?
			//   What happens when an SDK issues multiple RPCs concurrently?
			EmptyPoll(s)
			continue
		}

		spec.Send[*wsv1.PollWorkflowTaskQueueRequest](s,
			spec.Eq("namespace", s.C.Namespace),
			spec.Eq("task_queue.name", s.C.TaskQueue),
			spec.Eq("task_queue.kind", enumspb.TASK_QUEUE_KIND_NORMAL),
			spec.NonEmpty("identity"))
		s.C.Poll++

		// A long poll may legitimately return nothing.
		if s.Maybe("long-poll-timeout", spec.PeekEmpty[*wsv1.PollWorkflowTaskQueueResponse]("task_token")) {
			EmptyPoll(s)
			continue
		}

		task := spec.Send[*wsv1.PollWorkflowTaskQueueResponse](s,
			spec.NonEmpty("task_token"),
			spec.Eq("workflow_execution.workflow_id", s.C.WorkflowID),
			spec.Eq("workflow_execution.run_id", s.C.RunID),
			spec.Eq("workflow_type.name", s.C.WorkflowType),
			spec.Eq("attempt", s.C.Attempt),
			// Nothing has been completed yet, so there is no replay boundary. A Workflow
			// Task that failed does not move it.
			spec.Eq("previous_started_event_id", int64(0)),
			spec.Eq("started_event_id", s.C.startedEventID()),
			spec.Empty("next_page_token"),
			spec.Nested("history.events", TaskHistory))
		s.C.OpenToken = task.TaskToken
		s.C.Poll--
		return task
	}
}

// CompleteTask is the Worker reporting that it processed the task.
func CompleteTask(s *spec.Session[Config]) {
	spec.Send[*wsv1.RespondWorkflowTaskCompletedRequest](s,
		spec.Eq("task_token", s.C.OpenToken),
		spec.NonEmpty("identity"),
		spec.Nested("commands", Commands))
	s.C.OpenToken = nil

	spec.Send[*wsv1.RespondWorkflowTaskCompletedResponse](s,
		spec.Absent("workflow_task"),
		spec.EmptyList("activity_tasks"))
}

// FailTask is the Worker failing the task in a controlled way: the cause says the
// Worker could not handle something, not that the Workflow was nondeterministic.
// The Workflow itself stays running, so Phase does not change — the Server records
// the failure and schedules the task again, which is the next attempt and the point
// at which the Worker has to be consistent with itself.
func FailTask(s *spec.Session[Config]) {
	spec.Send[*wsv1.RespondWorkflowTaskFailedRequest](s,
		spec.Eq("task_token", s.C.OpenToken),
		spec.Eq("cause", enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE),
		spec.Present("failure"))
	spec.Send[*wsv1.RespondWorkflowTaskFailedResponse](s)
	s.C.OpenToken = nil
	s.C.TaskRejected = true
	s.C.History = append(s.C.History, TaskFailed(), TaskScheduled())
	s.C.Attempt++
}

// EmptyPoll is the response to a poll that found no work.
func EmptyPoll(s *spec.Session[Config]) {
	spec.Send[*wsv1.PollWorkflowTaskQueueResponse](s,
		spec.Empty("task_token"),
		spec.Absent("history"))
	s.C.Poll--
}

// TaskHistory describes the events a Workflow Task carries: the History the
// Server had stored for this Run, then the WorkflowTaskStarted event that
// answering this poll creates. The specification does not decide what the stored
// part contains — that is Config.History, a parameter of the run.
func TaskHistory(s *spec.Session[Config], events spec.List) {
	// Every delivery carries the History from the beginning, so the event_id counter
	// restarts here rather than continuing across tasks.
	s.C.NextEventID = 1
	for _, e := range s.C.History {
		event(s, events, e)
	}
	event(s, events, Event{
		Type:  int32(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED),
		Attrs: []spec.Constraint{spec.Present("workflow_task_started_event_attributes")},
	})
	s.C.Phase = "running"
}

func event(s *spec.Session[Config], events spec.List, e Event) {
	all := []spec.Constraint{
		spec.Counter("event_id", &s.C.NextEventID),
		eventType(e.Type),
		spec.Opaque("event_time"),
		spec.Eq("worker_may_ignore", e.MayIgnore),
	}
	spec.Satisfy[*historypb.HistoryEvent](s, events.Append(), append(all, e.Attrs...)...)
}

// eventType constrains event_type to a number, whether or not the API this
// specification was compiled against has a name for it. UnknownEnum is the
// no-name case: the number has to go onto the wire bare, and asking for it says
// out loud that it really is nameless here.
func eventType(n int32) spec.Constraint {
	if spec.EnumHasName[enumspb.EventType](n) {
		return spec.Eq("event_type", enumspb.EventType(n))
	}
	return spec.UnknownEnum("event_type", n)
}

// Commands describes what the Worker asks the Server to do next.
func Commands(s *spec.Session[Config], cmds spec.List) {
	for cmds.More(1, 8) {
		c := cmds.Append()
		// Field constrains a field to one of the listed values and returns it, so the
		// branch below is always discriminated by something on the wire.
		switch spec.Field(s, c, "command_type",
			enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION) {

		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
			spec.Satisfy[*commandpb.Command](s, c,
				spec.Present("complete_workflow_execution_command_attributes"))
			s.C.Phase = "closed"
			return // a closing command must be last
		}
	}
}
