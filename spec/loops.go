package spec

// A specification loops by executing the same branch twice: PollUntilTask polls
// until a task arrives, and every pass reaches the same `if s.Maybe("sticky-poll",
// ...)` on the same line. Exploring it therefore produces a tree in which that
// branch point recurs beneath itself, once per unrolling the bounds allowed — and
// because every unrolling carries its own copy of everything that follows the
// loop, an unrolled drawing repeats the whole tail of the conversation.
//
// Recognizing the recurrence collapses that. What is drawn is one pass through the
// loop and one copy of the tail, which is both smaller and a better description:
// the specification does not say "poll at most three times", it says "poll until a
// task arrives".

// branchID identifies a branch point in the specification's source. The label
// alone will not do — one label is answered afresh on every pass through the loop
// it controls — so the source line is what distinguishes "the same branch again"
// from "a different branch that happens to share a name".
//
// It is empty when the Choice carries no Site, which is the case for a
// hand-constructed one. Loop detection is then off, rather than guessing from
// labels.
//
// Site comes from callSite, which walks out to the first frame outside this
// package, so a specification written *inside* package spec — only test helpers
// are — reports the testing frame instead of its own line, and every branch in it
// shares a Site. The label still separates them, so the identity holds; it is the
// per-branch precision that is lost. Real specifications live in their own
// package, where the site is the exact line.
func branchID(c Choice) string {
	if c.Site == "" {
		return ""
	}
	return c.Site + " " + c.Label
}

// scope is one loop analysis: the branch point being looked for, the branch points
// of enclosing loops, and a memo over the walk.
type scope struct {
	id string
	// outer are the branch points already on the path from the root. They are hard
	// boundaries. An enclosing loop's back edge leads to another unrolling of the
	// enclosing block, and anything found down there belongs to that loop, not this
	// one — walking through it would make an inner branch look as though it came
	// back round to itself when what actually came round was the outer loop.
	outer map[string]bool
	memo  map[*treeNode]bool
}

// reaches reports whether the loop's branch point occurs at or beneath x. A
// recurrence stops the walk: what lies below it is a further unrolling, which is
// the very thing the loop box stands for.
func (s *scope) reaches(x *treeNode) bool {
	if x == nil || x.br == nil {
		return false
	}
	if v, ok := s.memo[x]; ok {
		return v
	}
	id := branchID(x.br.choice)
	var v bool
	switch {
	case id == s.id:
		v = true
	case s.outer[id]:
		v = false // an enclosing loop's back edge: this loop's body ends here
	default:
		v = s.reaches(x.br.yes) || s.reaches(x.br.no)
	}
	s.memo[x] = v
	return v
}

// loopExit reports where a loop headed at n is left, or nil if n heads no loop or
// heads one whose shape cannot be drawn as a single box.
//
// The loop's body is the natural loop: every node that can get back to the branch
// point. Anything hanging off it that cannot is what follows the loop, and to draw
// the body once there must be exactly one such place — otherwise the conversation
// leaves the loop in several different states and hoisting any one of them out
// would misrepresent the others. Those fall back to the unrolled drawing.
func loopExit(n *treeNode, outer map[string]bool) *treeNode {
	if n.br == nil {
		return nil
	}
	id := branchID(n.br.choice)
	if id == "" {
		return nil
	}
	s := &scope{id: id, outer: outer, memo: map[*treeNode]bool{}}
	if !s.reaches(n.br.yes) && !s.reaches(n.br.no) {
		return nil // the branch never comes round again, so there is no loop
	}

	var exits []*treeNode
	s.collectExits(n, &exits)
	if len(exits) != 1 {
		return nil
	}
	return exits[0]
}

func (s *scope) collectExits(n *treeNode, out *[]*treeNode) {
	if n.br == nil {
		return
	}
	for _, c := range []*treeNode{n.br.yes, n.br.no} {
		if c == nil {
			continue
		}
		if !s.reaches(c) {
			// Nothing below here comes back, so this is where the conversation goes on.
			*out = append(*out, c)
			continue
		}
		if c.br != nil && branchID(c.br.choice) == s.id {
			continue // the loop goes round here rather than leaving
		}
		s.collectExits(c, out)
	}
}
