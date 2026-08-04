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
		name        string
		configure   func(*polling.Config)
		expectFails bool
	}{
		{
			name:      "trivial workflow completes",
			configure: func(*polling.Config) {},
		},
		{
			// The Server may add informational events without waiting for every
			// Worker to upgrade, so long as it marks them ignorable.
			name: "unknown history event marked ignorable is tolerated",
			configure: func(c *polling.Config) {
				c.InjectUnknownEvent = true
				c.UnknownIgnorable = true
			},
		},
		{
			// An unknown event that might affect Workflow state must fail the task in
			// a controlled way rather than being silently accepted.
			name: "unknown history event not marked ignorable fails the task",
			configure: func(c *polling.Config) {
				c.InjectUnknownEvent = true
				c.UnknownIgnorable = false
			},
			expectFails: true,
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
			if cfg.Phase != "closed" {
				t.Fatalf("specification finished in phase %q, want %q", cfg.Phase, "closed")
			}
			if cfg.Poll != 0 {
				t.Errorf("specification finished with %d poll(s) outstanding, want 0", cfg.Poll)
			}

			if tc.expectFails {
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

func startMock(t *testing.T) *runner.MockServer {
	t.Helper()
	srv, err := runner.StartMockServer(&runner.MockServer{
		Modeled:       modeled,
		RecvTimeout:   30 * time.Second,
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
