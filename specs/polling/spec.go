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

	Phase       string // "new" | "running" | "closed"
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

// StartedHistory is what a Server holds for a Workflow that has just been started
// and whose first Workflow Task is waiting to be picked up. A run that wants a
// history something else has already added to appends to it.
func StartedHistory(workflowType, taskQueue string) []Event {
	return []Event{ExecutionStarted(workflowType, taskQueue), TaskScheduled()}
}

func New(namespace, taskQueue, workflowType, workflowID, runID string) *Config {
	return &Config{
		Namespace: namespace, TaskQueue: taskQueue,
		WorkflowType: workflowType, WorkflowID: workflowID, RunID: runID,
		Phase: "new", NextEventID: 1,
		History: StartedHistory(workflowType, taskQueue),
	}
}

// taskEventCount is how many History Events the next Workflow Task carries: the
// stored History, plus the WorkflowTaskStarted event that answering the poll
// creates.
func (c *Config) taskEventCount() int64 { return int64(len(c.History)) + 1 }

// rejectableEvent reports whether the stored History holds an event a Worker is
// entitled to refuse: one not marked ignorable whose type has no name in the API
// this specification was compiled against, so no Worker can be required to
// understand it. The prototype delivers the whole History in the first task, so
// every stored event is one the Worker has just been shown.
func (c *Config) rejectableEvent() bool {
	for _, e := range c.History {
		if !e.MayIgnore && !spec.EnumHasName[enumspb.EventType](e.Type) {
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

		// Whether an event in the History is *unknown* is not something the Server,
		// and so not something this specification, can know: a Worker built against a
		// newer API recognizes the event type and handles it, an older one does not.
		// The branch is therefore not decided here. Both reactions are permitted and
		// the Worker's own reply discriminates:
		//
		//   - A Worker that does not recognize the event, told it may not ignore it,
		//     must fail the Workflow Task in a controlled way rather than assume the
		//     event is harmless.
		//   - A Worker that does recognize it handles it and completes the task.
		//
		// An ignorable event needs no branch: recognized or ignored, the task is
		// completed either way, and a Worker that failed it instead is caught below.
		//
		// The price of not presupposing is that "recognized the event" and "wrongly
		// swallowed an event it did not recognize" look identical on the wire, so this
		// branch cannot catch the latter. That needs an input the prototype does not
		// model: which event types the Worker under test knows, stated by whoever
		// launched it.
		if s.C.rejectableEvent() &&
			s.Maybe("worker-cannot-handle-event",
				spec.PeekIsType[*wsv1.RespondWorkflowTaskFailedRequest](
					&wsv1.RespondWorkflowTaskCompletedRequest{})) {
			FailTask(s)
			s.C.TaskRejected = true
			// A failed Workflow Task leaves the Workflow running and the Worker
			// polling; what the Server does next is beyond what this specification
			// models, so the conversation ends here.
			return
		}
		CompleteTask(s)
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

		startedEventID := s.C.NextEventID + s.C.taskEventCount() - 1
		task := spec.Send[*wsv1.PollWorkflowTaskQueueResponse](s,
			spec.NonEmpty("task_token"),
			spec.Eq("workflow_execution.workflow_id", s.C.WorkflowID),
			spec.Eq("workflow_execution.run_id", s.C.RunID),
			spec.Eq("workflow_type.name", s.C.WorkflowType),
			spec.Eq("attempt", int32(1)),
			spec.Eq("previous_started_event_id", int64(0)),
			spec.Eq("started_event_id", startedEventID),
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
// The Workflow itself stays running, so Phase does not change.
func FailTask(s *spec.Session[Config]) {
	spec.Send[*wsv1.RespondWorkflowTaskFailedRequest](s,
		spec.Eq("task_token", s.C.OpenToken),
		spec.Eq("cause", enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE),
		spec.Present("failure"))
	spec.Send[*wsv1.RespondWorkflowTaskFailedResponse](s)
	s.C.OpenToken = nil
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
	if s.C.Phase != "new" {
		// The prototype models a Workflow that completes in its first task.
		return
	}
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
