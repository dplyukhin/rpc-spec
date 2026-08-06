// Command mockserver runs the polling specification as a mock Temporal Server, so
// a Worker in any language can be pointed at it by hand. Mock Server mode needs
// no language-specific adapter: the only per-language artifact is a Worker program
// that registers a Workflow.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/rpcspec/runner"
	"go.temporal.io/rpcspec/spec"
	"go.temporal.io/rpcspec/specs/polling"
)

func main() {
	var (
		namespace    = flag.String("namespace", "default", "namespace the Worker will use")
		taskQueue    = flag.String("task-queue", "rpcspec-tq", "task queue the Worker will poll")
		workflowType = flag.String("workflow-type", "TrivialWorkflow", "registered Workflow name")
		storedEvent  = flag.Bool("stored-event", false,
			"have the Server already hold one more History Event, as something else would have caused")
		eventType = flag.Int("event-type", 9999,
			"the event_type of that event; 9999 has no name in this API, and a named type the Worker "+
				"has no handler for does just as well")
		ignorable = flag.Bool("ignorable", true, "mark that event worker_may_ignore")
		timeout   = flag.Duration("timeout", 60*time.Second, "how long to wait for the Worker")
	)
	flag.Parse()

	cfg := polling.New(*namespace, *taskQueue, *workflowType, "wf-1", "run-1")
	if *storedEvent {
		// It landed while the first Workflow Task was waiting on the queue.
		cfg.History = append(cfg.History, polling.Event{
			Type: int32(*eventType), MayIgnore: *ignorable,
		})
	}

	srv, err := runner.StartMockServer(&runner.MockServer{
		Modeled: []string{
			"PollWorkflowTaskQueue",
			"RespondWorkflowTaskCompleted",
			"RespondWorkflowTaskFailed",
		},
		RecvTimeout:   *timeout,
		EmptyPollHold: 5 * time.Second,
		Log:           func(f string, a ...any) { log.Printf(f, a...) },
	})
	if err != nil {
		log.Fatalf("starting the mock Server: %v", err)
	}
	defer srv.Stop()

	fmt.Printf("mock Temporal Server listening on %s\n", srv.Addr())
	fmt.Printf("point a Worker at it, for example:\n")
	fmt.Printf("  client.Dial(client.Options{HostPort: %q, Namespace: %q})\n", srv.Addr(), *namespace)
	fmt.Println()

	res, err := spec.Run(cfg, polling.Conversation, spec.Options{
		Transport: srv,
		Trace:     func(f string, a ...any) { log.Printf(f, a...) },
	})
	if res != nil {
		fmt.Printf("\nconversation:\n%s", res)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n", err)
		os.Exit(1)
	}
	fmt.Println("the conversation was permitted by the specification")
}
