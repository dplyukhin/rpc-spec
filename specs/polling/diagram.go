package polling

import (
	wsv1 "go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/rpcspec/spec"
)

// Detail annotates a message in a sequence diagram with what its type does not
// say. A long poll that timed out and a long poll that delivered a task are both a
// PollWorkflowTaskQueueResponse, so without this the two are drawn as identical
// arrows and the reason the conversation looped is invisible.
//
// It lives here rather than in the renderer because which messages need
// distinguishing is a fact about this conversation, not about mermaid.
func Detail(e spec.Entry) string {
	r, ok := e.Msg.(*wsv1.PollWorkflowTaskQueueResponse)
	if !ok || len(r.TaskToken) > 0 {
		return ""
	}
	return "(no task)"
}
