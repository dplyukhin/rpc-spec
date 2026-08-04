// Package test holds the evidence: the same specification driving a mock Server
// that a real Go SDK Worker talks to, and a mock Worker that talks to a real
// Temporal Server.
package test

import (
	"go.temporal.io/sdk/workflow"
)

// TrivialWorkflow is the whole Worker-side program. Mock Server mode needs no
// language-specific adapter beyond something like this: any SDK's Worker can be
// pointed at a different address.
func TrivialWorkflow(ctx workflow.Context) (string, error) {
	return "done", nil
}
