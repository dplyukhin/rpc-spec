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

	Phase       string // "new" | "running" | "closed"
	Poll        int    // outstanding long polls
	NextEventID int64
	OpenToken   []byte

	InjectUnknownEvent bool
	UnknownEventType   int32
	UnknownIgnorable   bool
}

func New(namespace, taskQueue, workflowType, workflowID, runID string) *Config {
	return &Config{
		Namespace: namespace, TaskQueue: taskQueue,
		WorkflowType: workflowType, WorkflowID: workflowID, RunID: runID,
		Phase: "new", NextEventID: 1, UnknownEventType: 9999,
	}
}

// taskEventCount is how many History Events the next Workflow Task carries.
func (c *Config) taskEventCount() int64 {
	n := int64(3) // execution started, task scheduled, task started
	if c.InjectUnknownEvent {
		n++ // an extra event that the Worker may or may not ignore
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

		if s.C.InjectUnknownEvent && !s.C.UnknownIgnorable {
			// If an event is unknown to the worker, the worker should check
			// the "worker_may_ignore" field. If true, the worker should ignore the event.
			// If false, the worker should fail the workflow task.
			FailTask(s)
			continue
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

// History describes the events a Workflow Task carries.
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
