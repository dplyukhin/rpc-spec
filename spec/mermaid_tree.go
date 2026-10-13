package spec

import (
	"fmt"
	"strings"
)

// A single run draws as a list of arrows. An exploration draws as a tree, because
// the paths share prefixes: two runs that agreed on their first i choices produced
// the same messages up to the point where choice i+1 was consumed, by
// determinism. So merging the paths is a zip on the step streams Steps already
// produces, not a diff — and the shared prefixes become the trunk of the diagram
// while the divergences become alt blocks.

// treeNode is a point in the conversation: the messages that happen from here
// until the next decision, and then that decision.
type treeNode struct {
	msgs []Entry
	br   *treeBranch
	// end describes a node with no decision after it.
	end leafKind
	err error
	// filled distinguishes a node whose messages have been recorded from a fresh
	// one. Every path through a node produces the same messages, so only the first
	// writes them.
	filled bool
}

type treeBranch struct {
	choice  Choice
	yes, no *treeNode
}

type leafKind int

const (
	// leafEnd is a conversation that finished.
	leafEnd leafKind = iota
	// leafTruncated is one that stopped at a bound and continues beyond the drawing.
	leafTruncated
	// leafFailed is one the specification could not produce.
	leafFailed
)

func buildTree(ex *Exploration) *treeNode {
	root := &treeNode{}
	for _, p := range ex.Paths {
		if p.Result == nil {
			continue
		}
		insertPath(root, p.Result.Steps(), p)
	}
	return root
}

func insertPath(n *treeNode, steps []Step, p Path) {
	i := 0
	var msgs []Entry
	for i < len(steps) && steps[i].Entry != nil {
		msgs = append(msgs, *steps[i].Entry)
		i++
	}
	if !n.filled {
		n.msgs, n.filled = msgs, true
	}

	if i == len(steps) {
		switch {
		case p.Err != nil:
			n.end, n.err = leafFailed, p.Err
		case p.Truncated:
			n.end = leafTruncated
		default:
			n.end = leafEnd
		}
		return
	}

	c := *steps[i].Choice
	if n.br == nil {
		n.br = &treeBranch{choice: c}
	}
	arm := &n.br.no
	if c.Taken {
		arm = &n.br.yes
	}
	if *arm == nil {
		*arm = &treeNode{}
	}
	insertPath(*arm, steps[i+1:], p)
}

// Mermaid renders the whole exploration: every conversation the specification
// permits, with the branches drawn as alt blocks.
func (ex *Exploration) Mermaid() string { return MermaidOptions{}.RenderExploration(ex) }

// RenderExploration draws the specification rather than a run of it. Autonumbering
// is deliberately not offered here: a number would suggest an order across
// alternatives that are not in the same conversation.
func (o MermaidOptions) RenderExploration(ex *Exploration) string {
	var b strings.Builder
	b.WriteString("sequenceDiagram\n")
	if o.Title != "" {
		fmt.Fprintf(&b, "%stitle %s\n", step, mermaidText(o.Title))
	}
	if ex == nil {
		return b.String()
	}

	var all []Entry
	for _, p := range ex.Paths {
		if p.Result != nil {
			all = append(all, p.Result.Transcript...)
		}
	}
	lifelines := o.participants(all)
	for i := range lifelines {
		// Every lifeline is generated under the paper transport, so labelling them all
		// "(mock)" would say nothing. This is a drawing of the contract, not of a run
		// against a peer.
		lifelines[i].mocked = false
	}
	for _, l := range lifelines {
		fmt.Fprintf(&b, "%sparticipant %s as %s\n", step, l.party, mermaidText(l.label()))
	}

	r := &treeRender{
		o:    o,
		b:    &b,
		span: spanOf(lifelines),
		seen: map[string]bool{},
		left: map[*treeNode]bool{},
	}
	r.node(buildTree(ex), step)
	return b.String()
}

// treeRender carries the state a tree walk needs that a fold over one run does
// not: which branch points are already on the path from the root, and which nodes
// have been hoisted out of an enclosing loop and so must not be drawn again where
// they sit.
type treeRender struct {
	o    MermaidOptions
	b    *strings.Builder
	span string
	seen map[string]bool
	left map[*treeNode]bool
}

func (r *treeRender) node(n *treeNode, indent string) {
	if r.left[n] {
		// The arm cannot simply be left empty: a blank arm reads as "and then nothing
		// happens", which is not what leaving a loop means.
		r.note(indent, "leaves the loop and continues below")
		return
	}

	for _, e := range n.msgs {
		r.b.WriteString(r.o.renderEntry(e, indent))
	}

	if n.br == nil {
		r.leaf(n, indent)
		return
	}

	// Coming back to a branch point already on this path is the specification
	// looping. What lies below it is another unrolling of the same block, so it is
	// not drawn: either an enclosing loop box already says the block repeats, or
	// this note does.
	id := branchID(n.br.choice)
	if id != "" && r.seen[id] {
		r.note(indent, "goes round again from "+quoted(n.br.choice.Label))
		return
	}

	if exit := loopExit(n, r.seen); exit != nil {
		r.loop(n, exit, id, indent)
		return
	}
	r.branch(n, id, indent)
}

// loop draws one pass through the block and then, once, everything that follows
// it. Without this the whole tail of the conversation is repeated under every
// unrolling the bounds happened to allow.
func (r *treeRender) loop(n, exit *treeNode, id, indent string) {
	fmt.Fprintf(r.b, "%sloop repeats from %s\n", indent, mermaidText(quoted(n.br.choice.Label)))
	r.left[exit] = true
	r.branch(n, id, indent+step)
	delete(r.left, exit)
	fmt.Fprintf(r.b, "%send\n", indent)

	// Hoisted clear of the box, because it happens once however many times the block
	// repeated.
	r.node(exit, indent)
}

func (r *treeRender) branch(n *treeNode, id, indent string) {
	if id != "" {
		r.seen[id] = true
		defer delete(r.seen, id)
	}

	// A branch with only one arm explored is not drawn as an alt. An alt whose other
	// arm holds nothing but "not explored" costs a level of nesting and a pair of
	// rules to say something a note says better — and in a loop-shaped
	// specification, where the bounds cut every unrolling short, most branches are
	// this shape. The unexplored arm is still named: leaving it out would claim the
	// branch does not exist.
	if n.br.yes == nil || n.br.no == nil {
		missing, present := true, n.br.no
		if n.br.no == nil {
			missing, present = false, n.br.yes
		}
		r.note(indent, fmt.Sprintf("branch not followed: %s = %v", n.br.choice.Label, missing),
			"exploration bounds stop here")
		r.node(present, indent)
		return
	}

	// The alt arm asserts the lookahead's description, which is what holds there.
	// The else arm only negates the label, because negating the description
	// automatically would produce something that reads as nonsense.
	title := n.br.choice.Label
	if d := n.br.choice.Describe; d != "" {
		title += " — " + d
	}
	fmt.Fprintf(r.b, "%salt %s\n", indent, wrapNote(mermaidText(title)))
	r.node(n.br.yes, indent+step)
	fmt.Fprintf(r.b, "%selse not %s\n", indent, mermaidText(n.br.choice.Label))
	r.node(n.br.no, indent+step)
	fmt.Fprintf(r.b, "%send\n", indent)
}

func (r *treeRender) leaf(n *treeNode, indent string) {
	switch n.end {
	case leafTruncated:
		r.note(indent, "bound reached: the conversation continues past this point")
	case leafFailed:
		r.note(indent, "the specification could not generate its own side here", firstLine(n.err))
	}
}

func (r *treeRender) note(indent string, lines ...string) {
	if r.span == "" {
		return
	}
	escaped := make([]string, len(lines))
	for i, l := range lines {
		escaped[i] = mermaidText(l)
	}
	fmt.Fprintf(r.b, "%sNote over %s: %s\n", indent, r.span, wrapNote(escaped...))
}

func quoted(s string) string { return `"` + s + `"` }

// firstLine keeps a diagnostic to something a note can hold; a Violation renders
// as a whole report.
func firstLine(err error) string {
	if err == nil {
		return ""
	}
	s, _, _ := strings.Cut(err.Error(), "\n")
	return s
}
