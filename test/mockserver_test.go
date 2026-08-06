package test

import (
	"testing"
	"time"

	wsv1 "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/rpcspec/runner"
	"go.temporal.io/rpcspec/spec"
	"go.temporal.io/rpcspec/specs/polling"
)

// modeled is the set of RPCs the polling specification describes. Everything else
// a real Worker calls — GetSystemInfo, DescribeNamespace, activity polls,
// ShutdownWorker — is permitted without being modeled.
var modeled = []string{
	"PollWorkflowTaskQueue",
	"RespondWorkflowTaskCompleted",
	"RespondWorkflowTaskFailed",
}

// TestMockServerAgainstRealGoSDK runs the specification as the Server: messages
// the SDK produces are checked, messages the Server produces are generated. The
// Worker is an unmodified Go SDK Worker; the only thing that changes is the
// address it dials.
func TestMockServerAgainstRealGoSDK(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*polling.Config)
		// wantRejected is what this particular Worker is expected to do, not what
		// the specification requires. The specification permits either reaction to
		// an event whose type the Worker may or may not recognize; the Go SDK linked
		// into this test has no name for event type 9999, so it is the Worker that
		// makes the rule bite here.
		wantRejected bool
	}{
		{
			name:      "trivial workflow completes",
			configure: func(*polling.Config) {},
		},
		{
			// The Server may add informational events without waiting for every
			// Worker to upgrade, so long as it marks them ignorable.
			name: "extra history event marked ignorable is tolerated",
			configure: func(c *polling.Config) {
				c.InjectExtraEvent = true
				c.ExtraEventIgnorable = true
			},
		},
		{
			// An event that might affect Workflow state and that the Worker does not
			// recognize must fail the task in a controlled way rather than being
			// silently accepted.
			name: "extra history event not marked ignorable is rejected by a Worker that does not know it",
			configure: func(c *polling.Config) {
				c.InjectExtraEvent = true
				c.ExtraEventIgnorable = false
			},
			wantRejected: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := polling.New("default", "rpcspec-tq", "TrivialWorkflow", "wf-1", "run-1")
			tc.configure(cfg)

			srv := startMock(t)
			done := runSpec(t, srv, cfg, polling.Conversation)
			stopWorker := startGoWorker(t, srv.Addr(), cfg)

			if err := <-done; err != nil {
				t.Fatalf("the conversation was not permitted by the specification:\n%v", err)
			}
			if cfg.ExtraEventRejected != tc.wantRejected {
				t.Fatalf("the Worker rejected the extra event: %v, want %v",
					cfg.ExtraEventRejected, tc.wantRejected)
			}
			// A rejected task leaves the Workflow running; only a completed one closes it.
			wantPhase := "closed"
			if tc.wantRejected {
				wantPhase = "running"
			}
			if cfg.Phase != wantPhase {
				t.Fatalf("specification finished in phase %q, want %q", cfg.Phase, wantPhase)
			}
			if cfg.Poll != 0 {
				t.Errorf("specification finished with %d poll(s) outstanding, want 0", cfg.Poll)
			}

			if tc.wantRejected {
				// The rule says the Worker must stay alive and keep working, so it must
				// still be polling. That is itself checkable with the same machinery.
				live := <-runSpec(t, srv, cfg, func(s *spec.Session[polling.Config]) {
					spec.Send[*wsv1.PollWorkflowTaskQueueRequest](s,
						spec.Eq("namespace", s.C.Namespace),
						spec.NonEmpty("identity"))
				})
				if live != nil {
					t.Fatalf("the Worker stopped polling after failing the task: %v", live)
				}
			}

			stopWorker()
		})
	}
}

// TestBranchOnWorkerReplyResolvesBothWays covers the arm of the unknown-event
// branch that no Worker in this repository can take: the one where the Worker
// recognizes the event and completes the task. A lookahead over message types has
// to answer "no" as promptly as it answers "yes", because a lookahead that only
// resolved when the failure arrived would be indistinguishable from a hang.
func TestBranchOnWorkerReplyResolvesBothWays(t *testing.T) {
	cfg := polling.New("default", "rpcspec-tq", "TrivialWorkflow", "wf-both-1", "run-both-1")
	// Marked ignorable, so the real Go SDK completes the task even though it does
	// not know the event type. The reply is what the branch is reading, not the
	// reason behind it.
	cfg.InjectExtraEvent = true
	cfg.ExtraEventIgnorable = true

	// Short, because a lookahead that fails to resolve would otherwise sit here.
	srv := startMockWithTimeout(t, 3*time.Second)

	body := func(s *spec.Session[polling.Config]) {
		polling.PollUntilTask(s)
		if s.Maybe("worker-cannot-handle-event",
			spec.PeekIsType[*wsv1.RespondWorkflowTaskFailedRequest](
				&wsv1.RespondWorkflowTaskCompletedRequest{})) {
			t.Error("the lookahead reported a failure the Worker never sent")
			polling.FailTask(s)
			return
		}
		polling.CompleteTask(s)
	}

	done := runSpec(t, srv, cfg, body)
	stopWorker := startGoWorker(t, srv.Addr(), cfg)
	defer stopWorker()

	if err := <-done; err != nil {
		t.Fatalf("the conversation was not permitted by the specification:\n%v", err)
	}
}

func startMock(t *testing.T) *runner.MockServer {
	t.Helper()
	return startMockWithTimeout(t, 30*time.Second)
}

func startMockWithTimeout(t *testing.T, recvTimeout time.Duration) *runner.MockServer {
	t.Helper()
	srv, err := runner.StartMockServer(&runner.MockServer{
		Modeled:       modeled,
		RecvTimeout:   recvTimeout,
		EmptyPollHold: 5 * time.Second,
		Log:           func(f string, a ...any) { t.Logf(f, a...) },
	})
	if err != nil {
		t.Fatalf("starting the mock Server: %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv
}

// runSpec executes a specification on its own goroutine and reports the verdict.
func runSpec[C any](t *testing.T, tr spec.Transport, cfg *C, body func(*spec.Session[C])) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		res, err := spec.Run(cfg, body, spec.Options{
			Transport: tr,
			Trace:     func(f string, a ...any) { t.Logf(f, a...) },
		})
		if err == nil && res != nil {
			t.Logf("conversation permitted:\n%s", res)
		}
		done <- err
	}()
	return done
}

// startGoWorker dials the mock and starts a real Go SDK Worker against it.
func startGoWorker(t *testing.T, addr string, cfg *polling.Config) func() {
	t.Helper()
	c, err := client.Dial(client.Options{
		HostPort:  addr,
		Namespace: cfg.Namespace,
		Logger:    sdklog.NewStructuredLogger(testLogger(t)),
		// Worker heartbeats are not part of this contract; an empty
		// DescribeNamespace response already disables them, and this is belt and
		// braces.
		WorkerHeartbeatInterval: -1,
	})
	if err != nil {
		t.Fatalf("dialing the mock Server: %v", err)
	}

	w := worker.New(c, cfg.TaskQueue, worker.Options{
		// No activities are registered, but the Worker would poll for them anyway.
		LocalActivityWorkerOnly: true,
	})
	w.RegisterWorkflowWithOptions(TrivialWorkflow, workflow.RegisterOptions{Name: cfg.WorkflowType})
	if err := w.Start(); err != nil {
		t.Fatalf("starting the Worker: %v", err)
	}
	return func() {
		w.Stop()
		c.Close()
	}
}
