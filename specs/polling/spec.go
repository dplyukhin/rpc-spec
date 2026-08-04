// Package polling is a specification of the Workflow-task polling conversation
// between a Temporal SDK Worker and Temporal Server.
//
// It is an ordinary imperative Go program. Nothing in it says whether a message
// is being checked against a real peer or generated for a mocked one; the runner
// decides that per direction. The same code below therefore drives a mock Server
// that a real Worker connects to, and a mock Worker that connects to a real
// Server.
package polling

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	wsv1 "go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/rpcspec/spec"
)

// Config is the specification's model of the conversation. It is ordinary
// mutable Go state: because this prototype never backtracks, nothing has to be
// copied or rolled back.
type Config struct {
	Namespace    string
	TaskQueue    string
	WorkflowType string
	WorkflowID   string
	RunID        string

	// StartWorkflow is set when the conversation includes the client call that
	// creates the Workflow. A mock Server never sees it, because the Worker is not
	// the one that starts Workflows; a mock Worker has to make it so there is a
	// task to poll for.
	StartWorkflow bool

	Phase       string // "new" | "running" | "closed"
	Poll        int    // outstanding long polls
	NextEventID int64
	OpenToken   []byte

	// The compatibility rule under test: deliver a History Event whose type this
	// Worker cannot know, and require the documented reaction.
	InjectUnknownEvent bool
	UnknownEventType   int32
	UnknownIgnorable   bool
}

// New returns a Config with the invariants a run starts from.
func New(namespace, taskQueue, workflowType, workflowID, runID string) *Config {
	return &Config{
		Namespace: namespace, TaskQueue: taskQueue,
		WorkflowType: workflowType, WorkflowID: workflowID, RunID: runID,
		Phase: "new", NextEventID: 1, UnknownEventType: 9999,
	}
}

// taskEventCount is how many History Events the next Workflow Task carries. The
// Server reports which event started the task, so the count is part of the poll
// response and not only of the history.
func (c *Config) taskEventCount() int64 {
	n := int64(3) // execution started, task scheduled, task started
	if c.InjectUnknownEvent {
		n++
	}
	return n
}

// Conversation is the whole modeled exchange.
func Conversation(s *spec.Session[Config]) {
	if s.C.StartWorkflow {
		StartWorkflow(s)
	}
	Polling(s)
}

// StartWorkflow is the client call that creates the Workflow. It is here so that
// a mock Worker has something to poll for; a mock Server simply never runs it.
func StartWorkflow(s *spec.Session[Config]) {
	spec.Send[*wsv1.StartWorkflowExecutionRequest](s,
		spec.Eq("namespace", s.C.Namespace),
		spec.Eq("workflow_id", s.C.WorkflowID),
		spec.Eq("workflow_type.name", s.C.WorkflowType),
		spec.Eq("task_queue.name", s.C.TaskQueue),
		spec.Eq("task_queue.kind", enumspb.TASK_QUEUE_KIND_NORMAL),
		spec.NonEmpty("request_id"),
		spec.NonEmpty("identity"))

	// Everything downstream of a Send is plain Go over the returned message.
	started := spec.Send[*wsv1.StartWorkflowExecutionResponse](s,
		spec.NonEmpty("run_id"),
		spec.Absent("eager_workflow_task"))
	s.C.RunID = started.RunId
}

// Polling is the Workflow-task loop: poll, receive a task and its history,
// respond with commands.
func Polling(s *spec.Session[Config]) {
	for s.C.Phase != "closed" {
		PollUntilTask(s)

		// The compatibility rule, stated once and enforced in both directions: an
		// unknown History Event marked ignorable must not disturb the Worker, and an
		// unknown event not so marked must fail the Workflow Task rather than being
		// silently accepted.
		if s.C.InjectUnknownEvent && !s.C.UnknownIgnorable {
			FailTask(s)
			continue
		}
		CompleteTask(s)
	}
}

// PollUntilTask polls until a poll yields a Workflow Task, tolerating sticky
// polls and long-poll timeouts along the way. It is an ordinary function: in a
// DSL that reified control flow this would have to be a named rule with an
// explicit loop node.
func PollUntilTask(s *spec.Session[Config]) *wsv1.PollWorkflowTaskQueueResponse {
	for {
		// A Worker with a Workflow cache polls its own sticky queue as well as the
		// shared one. Those polls are distinguishable — the kind is on the wire — so
		// the branch is discriminated by an observable field rather than guessed.
		if s.Maybe("sticky-poll", spec.PeekEq[*wsv1.PollWorkflowTaskQueueRequest](
			"task_queue.kind", enumspb.TASK_QUEUE_KIND_STICKY)) {
			spec.Send[*wsv1.PollWorkflowTaskQueueRequest](s,
				spec.Eq("namespace", s.C.Namespace),
				spec.Eq("task_queue.kind", enumspb.TASK_QUEUE_KIND_STICKY),
				spec.Eq("task_queue.normal_name", s.C.TaskQueue),
				spec.NonEmpty("task_queue.name"),
				spec.NonEmpty("identity"))
			s.C.Poll++
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
			spec.Nested("history.events", History))
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
func FailTask(s *spec.Session[Config]) {
	spec.Send[*wsv1.RespondWorkflowTaskFailedRequest](s,
		spec.Eq("task_token", s.C.OpenToken),
		spec.Eq("cause", enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE),
		spec.Present("failure"))
	spec.Send[*wsv1.RespondWorkflowTaskFailedResponse](s)
	s.C.OpenToken = nil
	s.C.Phase = "closed"
}

// EmptyPoll is the response to a poll that found no work.
func EmptyPoll(s *spec.Session[Config]) {
	spec.Send[*wsv1.PollWorkflowTaskQueueResponse](s,
		spec.Empty("task_token"),
		spec.Absent("history"))
	s.C.Poll--
}

// History describes the events a Workflow Task carries. The wire is just the
// outermost cursor; a list of events is described with the same primitives.
func History(s *spec.Session[Config], events spec.List) {
	if s.C.Phase != "new" {
		// The prototype models a Workflow that completes in its first task.
		return
	}
	event(s, events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		spec.Eq("workflow_execution_started_event_attributes.workflow_type.name", s.C.WorkflowType),
		spec.Eq("workflow_execution_started_event_attributes.task_queue.name", s.C.TaskQueue),
		spec.PositiveDuration("workflow_execution_started_event_attributes.workflow_task_timeout"))

	if s.C.InjectUnknownEvent {
		// An event type that does not exist. In synthesizer mode the number is
		// written straight onto the wire, which is how an old Worker can be shown a
		// message only a future Server could produce.
		spec.Satisfy[*historypb.HistoryEvent](s, events.Append(),
			spec.Counter("event_id", &s.C.NextEventID),
			spec.UnknownEnum("event_type", s.C.UnknownEventType),
			spec.Opaque("event_time"),
			spec.Eq("worker_may_ignore", s.C.UnknownIgnorable))
	}

	event(s, events, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		spec.Present("workflow_task_scheduled_event_attributes"))
	event(s, events, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		spec.Present("workflow_task_started_event_attributes"))
	s.C.Phase = "running"
}

// event is an ordinary helper function, not a rule object.
func event(s *spec.Session[Config], events spec.List, typ enumspb.EventType, cs ...spec.Constraint) {
	all := []spec.Constraint{
		spec.Counter("event_id", &s.C.NextEventID),
		spec.Eq("event_type", typ),
		spec.Opaque("event_time"),
	}
	spec.Satisfy[*historypb.HistoryEvent](s, events.Append(), append(all, cs...)...)
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
