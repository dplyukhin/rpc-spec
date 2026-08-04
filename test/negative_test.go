package test

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	wsv1 "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.temporal.io/rpcspec/runner"
	"go.temporal.io/rpcspec/spec"
	"go.temporal.io/rpcspec/specs/polling"
)

// The tests above show a specification accepting correct behaviour. These show it
// rejecting incorrect behaviour, in both directions. Without them, a
// specification that checked nothing would look identical.

// TestConsumerBugIsDetected takes the unknown-History-Event rule and gets it
// backwards: it delivers an event that is not marked ignorable and then requires
// the Worker to carry on regardless. A real Go SDK Worker fails the task, as the
// rule says it must, so the specification must reject the conversation.
func TestConsumerBugIsDetected(t *testing.T) {
	cfg := polling.New("default", "rpcspec-tq", "TrivialWorkflow", "wf-neg-1", "run-neg-1")
	cfg.InjectUnknownEvent = true
	cfg.UnknownIgnorable = false

	srv, err := runner.StartMockServer(&runner.MockServer{
		Modeled: modeled,
		// Short, because this run is expected to wait for a message that never comes.
		RecvTimeout:   3 * time.Second,
		EmptyPollHold: 5 * time.Second,
		Log:           func(f string, a ...any) { t.Logf(f, a...) },
	})
	if err != nil {
		t.Fatalf("starting the mock Server: %v", err)
	}
	t.Cleanup(srv.Stop)

	// A specification that requires the Worker to tolerate an event it must not.
	tolerant := func(s *spec.Session[polling.Config]) {
		polling.PollUntilTask(s)
		polling.CompleteTask(s)
	}

	done := runSpec(t, srv, cfg, tolerant)
	stopWorker := startGoWorker(t, srv.Addr(), cfg)
	defer stopWorker()

	err = <-done
	if err == nil {
		t.Fatal("the specification accepted a conversation that breaks the unknown-event rule")
	}
	t.Logf("rejected, as it should be:\n%v", err)

	// The diagnostic has to say what did happen, not just that something did not.
	if !strings.Contains(err.Error(), "RespondWorkflowTaskFailed") {
		t.Errorf("the diagnostic does not mention the reaction the Worker actually had:\n%v", err)
	}
}

// TestProducerBugIsDetected adds a clause the real Server violates — claiming the
// first delivery of a Workflow Task is attempt 2 — and checks that the runner
// locates it precisely. This is the direction a Server author cares about: learn
// from a shared contract that a response is wrong.
func TestProducerBugIsDetected(t *testing.T) {
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

	conn, err := grpc.NewClient(srv.FrontendHostPort(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing the dev server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cfg := polling.New("default", "rpcspec-tq", "TrivialWorkflow", "rpcspec-"+t.Name(), "")
	cfg.StartWorkflow = true

	wrongAttempt := func(s *spec.Session[polling.Config]) {
		polling.StartWorkflow(s)
		spec.Send[*wsv1.PollWorkflowTaskQueueRequest](s,
			spec.Eq("namespace", s.C.Namespace),
			spec.Eq("task_queue.name", s.C.TaskQueue),
			spec.Eq("task_queue.kind", enumspb.TASK_QUEUE_KIND_NORMAL),
			spec.NonEmpty("identity"))
		spec.Send[*wsv1.PollWorkflowTaskQueueResponse](s,
			spec.NonEmpty("task_token"),
			spec.Eq("attempt", int32(2)))
	}

	_, err = spec.Run(cfg, wrongAttempt, spec.Options{Transport: runner.NewMockWorker(conn)})
	if err == nil {
		t.Fatal("the specification accepted a response that does not satisfy it")
	}
	t.Logf("rejected, as it should be:\n%v", err)

	var v *spec.Violation
	if !errors.As(err, &v) {
		t.Fatalf("expected a spec.Violation, got %T", err)
	}
	if v.Cursor != "attempt" {
		t.Errorf("violation is at %q, want %q", v.Cursor, "attempt")
	}
	if v.SpecFile == "" || v.SpecLine == 0 {
		t.Errorf("violation does not point at a line of specification: %+v", v)
	}
	if !strings.Contains(v.Detail, "observed") {
		t.Errorf("violation does not report what was observed: %q", v.Detail)
	}
}
