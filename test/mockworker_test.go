package test

import (
	"context"
	"os/exec"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.temporal.io/rpcspec/runner"
	"go.temporal.io/rpcspec/spec"
	"go.temporal.io/rpcspec/specs/polling"
)

// TestMockWorkerAgainstRealServer runs the same specification as the SDK: messages
// the SDK produces are generated, messages the Server produces are checked. There
// is no Worker process at all — the specification is the Worker.
func TestMockWorkerAgainstRealServer(t *testing.T) {
	cliPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not installed, so there is no real Server to run against")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  cliPath,
		LogLevel:      "error",
		ClientOptions: &client.Options{Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("starting the dev server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	t.Logf("dev server listening on %s", srv.FrontendHostPort())

	conn, err := grpc.NewClient(srv.FrontendHostPort(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing the dev server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	mock := runner.NewMockWorker(conn)
	mock.Log = func(f string, a ...any) { t.Logf(f, a...) }

	// The Workflow ID has to be fresh, but the specification must stay a pure
	// function of the oracle, so the value is supplied from outside rather than
	// generated inside the specification.
	wfID := "rpcspec-" + t.Name()
	cfg := polling.New("default", "rpcspec-tq", "TrivialWorkflow", wfID, "")
	cfg.StartWorkflow = true

	res, err := spec.Run(cfg, polling.Conversation, spec.Options{
		Transport: mock,
		Trace:     func(f string, a ...any) { t.Logf(f, a...) },
	})
	if err != nil {
		t.Fatalf("the real Server's side of the conversation was not permitted by the specification:\n%v", err)
	}
	t.Logf("conversation permitted:\n%s", res)

	if cfg.Phase != "closed" {
		t.Fatalf("specification finished in phase %q, want %q", cfg.Phase, "closed")
	}

	// Out-of-band confirmation that the generated conversation really drove the
	// Workflow to completion, rather than merely being accepted.
	desc, err := srv.Client().DescribeWorkflowExecution(ctx, wfID, cfg.RunID)
	if err != nil {
		t.Fatalf("describing the Workflow: %v", err)
	}
	if got := desc.WorkflowExecutionInfo.Status; got != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("Workflow status is %v, want COMPLETED", got)
	}
	t.Logf("the real Server reports %s as %v", wfID, desc.WorkflowExecutionInfo.Status)
}
