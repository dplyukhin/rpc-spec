// Command specdiagram draws a specification as a mermaid sequence diagram.
//
// Unlike the diagrams the tests emit, this one is not of a conversation that
// happened. There is no Worker, no Server and no network: the specification is run
// with a transport that has no peer, once per path through its branches, and the
// paths are merged back into one drawing whose alt blocks are the specification's
// own choices. What comes out describes the contract rather than an observation of
// it.
//
//	go run ./cmd/specdiagram -unknown-event -o polling.mmd
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/rpcspec/spec"
	"go.temporal.io/rpcspec/specs/polling"
)

func main() {
	var (
		out         = flag.String("o", "", "write the diagram to this file instead of stdout")
		title       = flag.String("title", "the polling conversation", "diagram title")
		repeats     = flag.Int("repeats", 1, "how many times one branch may be taken on a single path; this is what bounds the poll loop")
		maxChoices  = flag.Int("max-choices", 12, "branch decisions allowed on a single path")
		maxMessages = flag.Int("max-messages", 60, "messages allowed on a single path")
		maxPaths    = flag.Int("max-paths", 200, "paths to explore in total")
		noLoop      = flag.Bool("no-loop", false, "do not enter the polling loop at all, leaving only the conversation's spine")

		startWorkflow = flag.Bool("start-workflow", false, "include the client call that creates the Workflow")
		unknownEvent  = flag.String("unknown-event", "",
			`add a non-ignorable stored History Event the Worker may not handle, opening the
        	worker-cannot-handle-event branch: "nameless" for a type this API cannot name,
        	"named" for one it can`)
	)
	flag.Parse()

	newConfig, err := configFactory(*startWorkflow, *unknownEvent)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	bounds := spec.Bounds{
		Repeats:     *repeats,
		MaxChoices:  *maxChoices,
		MaxMessages: *maxMessages,
		MaxPaths:    *maxPaths,
	}
	if *noLoop {
		// Both branches that re-enter the poll loop, held at zero. The branches are
		// still drawn — as unfollowed — so the diagram does not pretend the loop is not
		// there.
		bounds.RepeatsByLabel = map[string]int{"sticky-poll": 0, "long-poll-timeout": 0}
	}

	ex := spec.Explore(newConfig, polling.Conversation, bounds)
	report(ex)

	diagram := spec.MermaidOptions{Title: *title, Detail: polling.Detail}.RenderExploration(ex)
	if *out == "" {
		fmt.Print(diagram)
		return
	}
	if err := os.WriteFile(*out, []byte(diagram), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "writing the diagram: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", *out)
}

func configFactory(startWorkflow bool, unknownEvent string) (func() *polling.Config, error) {
	var extra []polling.Event
	switch unknownEvent {
	case "":
	case "nameless":
		// A type this API has no name for at all.
		extra = append(extra, polling.Event{Type: 9999, MayIgnore: false})
	case "named":
		// A type this API names but an older Worker may still have no handler for.
		extra = append(extra, polling.Event{
			Type:      int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED),
			MayIgnore: false,
		})
	default:
		return nil, fmt.Errorf("-unknown-event must be %q or %q, not %q", "nameless", "named", unknownEvent)
	}

	return func() *polling.Config {
		c := polling.New("default", "rpcspec-tq", "TrivialWorkflow", "wf-1", "run-1")
		c.StartWorkflow = startWorkflow
		c.History = append(c.History, extra...)
		return c
	}, nil
}

// report says what the exploration covered and, more importantly, what it did not.
// A diagram that quietly stopped looking would read as though it were the whole
// contract.
func report(ex *spec.Exploration) {
	var b strings.Builder
	fmt.Fprintf(&b, "explored %d path(s)", len(ex.Paths))
	if n := ex.Truncated(); n > 0 {
		fmt.Fprintf(&b, ", %d stopped at a bound", n)
	}
	if ex.Pruned > 0 {
		fmt.Fprintf(&b, ", %d branch(es) not followed (repeats=%d)", ex.Pruned, ex.Bounds.Repeats)
	}
	if !ex.Complete {
		fmt.Fprintf(&b, "; the search itself was cut short at max-paths=%d", ex.Bounds.MaxPaths)
	}
	fmt.Fprintln(os.Stderr, b.String())

	for _, p := range ex.Failed() {
		fmt.Fprintf(os.Stderr, "\na path could not be generated:\n%v\n", p.Err)
	}
}
