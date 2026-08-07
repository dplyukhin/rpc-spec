package spec

import (
	"fmt"
	"strings"
)

// A transcript is already a sequence diagram: Send is the only thing that appends
// to it, so it holds the wire messages and nothing else — nested Satisfy calls for
// History Events and Commands do not appear. Rendering it is therefore a fold, and
// this file is split so that the fold is the only part specific to a single run:
// endpointsOf and renderEntry decide how one message is drawn, and a later renderer
// over a *tree* of runs can reuse them unchanged.

// Party is one lifeline in the diagram.
type Party string

const (
	PartyClient Party = "Client"
	PartyWorker Party = "Worker"
	PartyServer Party = "Server"
)

// partyOrder is the left-to-right order of lifelines. The two callers sit on the
// left and the Server on the right, which is the direction every arrow in a
// conversation like this one points.
var partyOrder = []Party{PartyClient, PartyWorker, PartyServer}

// clientMethods are the WorkflowService methods a *client* calls rather than a
// Worker. Unlike roleOf, this cannot be derived from the service descriptor: the
// API states a method's input, output and transport, but never who is expected to
// call it.
//
// The one descriptor-level signal that looks like it would work does not. Worker
// methods carry no google.api.http option — the proto says so in as many words,
// "We do not expose worker API to HTTP" — but neither do deprecated and internal
// methods such as DeprecateNamespace and ScanWorkflowExecutions, so absence of the
// annotation means "not reachable over HTTP" and not "called by a Worker".
//
// So this is a hand-maintained list, deliberately confined to methods that are
// unambiguously client-driven, rather than an attempt at a full taxonomy of the
// service. Anything absent is drawn as the Worker, which is the right default for
// a conversation about Workflow Tasks; MermaidOptions.Caller overrides it.
var clientMethods = map[string]bool{
	"StartWorkflowExecution":           true,
	"SignalWorkflowExecution":          true,
	"SignalWithStartWorkflowExecution": true,
	"QueryWorkflow":                    true,
	"UpdateWorkflowExecution":          true,
	"RequestCancelWorkflowExecution":   true,
	"TerminateWorkflowExecution":       true,
	"ResetWorkflowExecution":           true,
	"DescribeWorkflowExecution":        true,
	"GetWorkflowExecutionHistory":      true,
}

// callerOf is the Server's counterparty for a method: whoever sends the request
// and receives the response.
func callerOf(method string) Party {
	if clientMethods[method] {
		return PartyClient
	}
	return PartyWorker
}

// endpointsOf reports which lifeline a message leaves and which it arrives at.
// Direction distinguishes only SDK from Server, so the counterparty comes from the
// method: a StartWorkflowExecutionResponse travels ToSDK, but it is answering the
// Client, not the Worker.
func endpointsOf(role Role) (from, to Party) {
	peer := callerOf(role.Method)
	if role.Direction == ToSDK {
		return PartyServer, peer
	}
	return peer, PartyServer
}

// MsgType is the message's short type name, e.g. PollWorkflowTaskQueueRequest.
func (e Entry) MsgType() string {
	if e.Msg == nil {
		return e.Role.Method
	}
	return string(e.Msg.ProtoReflect().Descriptor().Name())
}

// MermaidOptions configures the rendering. The zero value is usable.
type MermaidOptions struct {
	// Title, if set, is written above the diagram.
	Title string
	// Autonumber numbers the arrows, so a diagram and a transcript can be read
	// against each other by Seq.
	Autonumber bool
	// Detail, if set, appends per-message text to an arrow's label — a field value
	// that distinguishes two messages of the same type, say. Returning "" adds
	// nothing.
	Detail func(Entry) string
	// Caller overrides which lifeline is the Server's counterparty for a method.
	// Returning "" falls back to the default.
	Caller func(Role) Party
	// HideChoices suppresses the branch-decision notes. They are shown by default,
	// because two arrows of the same message type are often distinguishable only by
	// the branch that selected them.
	HideChoices bool
}

// Mermaid renders the run as a mermaid sequence diagram.
func (r *Result) Mermaid() string {
	return MermaidOptions{Autonumber: true}.Render(r)
}

// Render draws one run. Every message becomes an arrow, in the order the run
// produced them; a request and its response are not paired, because several
// interchangeable long polls may be outstanding at once and it is the transport,
// not the specification, that correlates them.
func (o MermaidOptions) Render(r *Result) string {
	var b strings.Builder
	b.WriteString("sequenceDiagram\n")
	if o.Title != "" {
		fmt.Fprintf(&b, "    title %s\n", mermaidText(o.Title))
	}
	if o.Autonumber {
		b.WriteString("    autonumber\n")
	}
	if r == nil {
		return b.String()
	}
	lifelines := o.participants(r.Transcript)
	for _, p := range lifelines {
		fmt.Fprintf(&b, "    participant %s as %s\n", p.party, mermaidText(p.label()))
	}
	span := spanOf(lifelines)
	for _, st := range r.Steps() {
		switch {
		case st.Entry != nil:
			b.WriteString(o.renderEntry(*st.Entry))
		// A note has to sit over some lifeline, so a choice with no messages around it
		// has nowhere to be drawn.
		case !o.HideChoices && span != "":
			b.WriteString(renderChoice(*st.Choice, span))
		}
	}
	return b.String()
}

// renderChoice draws a branch decision as a note across the conversation. A run
// took one arm, so there is no alt block to draw here — only a record of which arm
// and on what grounds.
func renderChoice(c Choice, span string) string {
	// The second line names the question the branch turned on rather than asserting
	// it: writing the lookahead's words bare would read as though they held, which is
	// wrong on the arm where the branch was not taken.
	src := "policy"
	if c.FromLookahead {
		src = "lookahead: " + c.Describe
	}
	head := fmt.Sprintf("%s = %v", c.Label, c.Taken)
	return fmt.Sprintf("    Note over %s: %s\n", span, wrapNote(mermaidText(head), mermaidText(src)))
}

// noteWidth is where note text is broken. A mermaid note is drawn as wide as its
// longest line needs, so an unwrapped lookahead description — they run past 120
// characters — pushes the note off both edges of the diagram.
const noteWidth = 52

// wrapNote joins lines with the only break a mermaid note honours.
func wrapNote(lines ...string) string {
	var out []string
	for _, line := range lines {
		out = append(out, wrapWords(line, noteWidth)...)
	}
	return strings.Join(out, "<br/>")
}

// wrapWords breaks on spaces only, so an over-long single word overflows rather
// than being cut in half. Message type names are long and splitting one would make
// it unrecognizable.
func wrapWords(s string, width int) []string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return []string{""}
	}
	var lines []string
	cur := words[0]
	for _, w := range words[1:] {
		if len(cur)+1+len(w) > width {
			lines = append(lines, cur)
			cur = w
			continue
		}
		cur += " " + w
	}
	return append(lines, cur)
}

// spanOf is the lifeline range a note stretches across: mermaid reads the first
// and last names, so this covers everything in between.
func spanOf(ls []lifeline) string {
	switch len(ls) {
	case 0:
		return ""
	case 1:
		return string(ls[0].party)
	default:
		return string(ls[0].party) + "," + string(ls[len(ls)-1].party)
	}
}

// renderEntry draws one message. This is the part a tree renderer reuses, so it
// takes an Entry and returns a line rather than writing into a run-shaped buffer.
func (o MermaidOptions) renderEntry(e Entry) string {
	from, to := o.endpoints(e.Role)
	// A dashed arrow for the reply, which is the usual reading of a return.
	arrow := "->>"
	if e.Role.Direction == ToSDK {
		arrow = "-->>"
	}
	label := e.MsgType()
	if o.Detail != nil {
		if d := o.Detail(e); d != "" {
			label += " " + d
		}
	}
	return fmt.Sprintf("    %s%s%s: %s\n", from, arrow, to, mermaidText(label))
}

func (o MermaidOptions) endpoints(role Role) (from, to Party) {
	from, to = endpointsOf(role)
	if o.Caller == nil {
		return from, to
	}
	if p := o.Caller(role); p != "" {
		if role.Direction == ToSDK {
			return PartyServer, p
		}
		return p, PartyServer
	}
	return from, to
}

// lifeline is a party that appears in this run, and whether the runner played it.
type lifeline struct {
	party Party
	// mocked is true when every message this party produced was generated rather
	// than checked, which is what makes the diagram say which side was real.
	mocked bool
}

func (l lifeline) label() string {
	if l.mocked {
		return string(l.party) + " (mock)"
	}
	return string(l.party)
}

// participants lists the lifelines this run touched, in a stable order.
func (o MermaidOptions) participants(entries []Entry) []lifeline {
	seen := make(map[Party]bool)
	// A party is drawn as mocked only if it produced at least one message and every
	// one of them was generated. Mode is decided per direction, so this is uniform
	// in practice; treating a mixture as "real" keeps the label from overclaiming.
	produced := make(map[Party]int)
	synth := make(map[Party]int)
	for _, e := range entries {
		// The message leaves its producer, so from is the party whose Mode this is.
		from, to := o.endpoints(e.Role)
		seen[from], seen[to] = true, true
		produced[from]++
		if e.Mode == Synth {
			synth[from]++
		}
	}
	var out []lifeline
	for _, p := range partyOrder {
		if !seen[p] {
			continue
		}
		out = append(out, lifeline{party: p, mocked: produced[p] > 0 && produced[p] == synth[p]})
	}
	return out
}

// mermaidText makes a string safe to sit after the colon of an arrow. Semicolons
// separate statements and '#' opens an entity code, so both have to be escaped; a
// newline would end the arrow entirely.
func mermaidText(s string) string {
	r := strings.NewReplacer(
		"#", "#35;",
		";", "#59;",
		"\r\n", "<br/>",
		"\n", "<br/>",
		"\r", "<br/>",
	)
	return r.Replace(s)
}
