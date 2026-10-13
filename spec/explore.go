package spec

import (
	"errors"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Exploring a specification means running it with no peer at all and taking every
// branch, rather than observing the one path a particular conversation happened to
// follow. What comes out describes the contract instead of a run of it.
//
// Two properties of the rest of this package make it work. A run is a
// deterministic function of the booleans Policy returns — Maybe is the only place
// nondeterminism enters, and the constraint solver's genctx is a counter — so
// replaying a prefix of choices reproduces a run exactly, and the search needs no
// continuations or snapshots, only re-execution. And in synthesizer mode the whole
// Transport surface reduces to Mode and Emit, so there is a transport with no peer
// behind it.

// ErrTruncated reports that a path stopped at an exploration bound rather than
// ending on its own. It is not a specification failure.
var ErrTruncated = errors.New("exploration bound reached")

// paper is the transport with nothing on the other side. Both directions are
// generated, which is a combination no live run has: a real transport checks one
// direction and generates the other. That is the point — the diagram is of
// everything the specification permits, not of what one peer would discriminate.
type paper struct {
	maxMessages int
	sent        int
}

func (*paper) Mode(Direction) Mode { return Synth }

// Recv and Peek are unreachable: Send only receives when Mode is Check, and Maybe
// only looks ahead when Mode is Check. They report rather than panic so that a
// future mode combination fails legibly instead of crashing.
func (*paper) Recv(Direction, protoreflect.FullName) (proto.Message, error) {
	return nil, errors.New("the paper transport has no peer, so nothing can be received")
}

func (*paper) Peek(Direction, []protoreflect.FullName) (proto.Message, error) {
	return nil, errors.New("the paper transport has no peer, so there is nothing to look ahead at")
}

// Emit discards the message, and counts it so that a specification which never
// stops talking is stopped. Send turns this error into an abort, which Run reports
// along with the partial transcript.
func (t *paper) Emit(Role, proto.Message) error {
	t.sent++
	if t.sent > t.maxMessages {
		return ErrTruncated
	}
	return nil
}

// prefix is the Policy the explorer drives. Choices before n are replayed from the
// vector; everything past it defaults to false, so one vector names one path.
type prefix struct {
	choices []bool
	n       int
	max     int
}

func (p *prefix) decide(string) bool {
	if p.n >= p.max {
		// A Policy returns a bool and has no way to report a problem, but a
		// specification that keeps branching has to be stopped somehow. This leaves by
		// the same route a transport failure does, so Run recovers it and hands back
		// the partial run rather than unwinding past it.
		panic(&abortErr{err: ErrTruncated})
	}
	taken := p.n < len(p.choices) && p.choices[p.n]
	p.n++
	return taken
}

// Bounds keep the search finite. A specification loops until something ends it —
// Polling runs until the Workflow closes, and a branch that keeps answering "the
// long poll timed out" never gets there — so exploring one is always exploring a
// bounded unrolling of it, and the bounds belong in the output.
type Bounds struct {
	// MaxChoices caps the branch decisions on a single path.
	MaxChoices int
	// MaxMessages caps the messages on a single path, for a specification that can
	// loop without branching.
	MaxMessages int
	// MaxPaths caps the whole search.
	MaxPaths int
	// Repeats caps how many times one branch may be answered yes along a single
	// path. This is the bound that matters in practice: a loop in a specification is
	// a branch that keeps saying yes, so without it the search spends everything it
	// has on ever-longer unrollings of the same two or three shapes. Zero means the
	// default of 1, which draws each loop entered at most once.
	Repeats int
	// RepeatsByLabel overrides Repeats for named branches.
	RepeatsByLabel map[string]int
}

func (b Bounds) withDefaults() Bounds {
	if b.MaxChoices <= 0 {
		b.MaxChoices = 12
	}
	if b.MaxMessages <= 0 {
		b.MaxMessages = 60
	}
	if b.MaxPaths <= 0 {
		b.MaxPaths = 200
	}
	if b.Repeats <= 0 {
		b.Repeats = 1
	}
	return b
}

func (b Bounds) repeatsFor(label string) int {
	if v, ok := b.RepeatsByLabel[label]; ok {
		return v
	}
	return b.Repeats
}

// Path is one conversation the specification permits, and how it ended.
type Path struct {
	Result *Result
	// Truncated marks a path that stopped at a bound. The conversation continues
	// past this point; the search simply did not follow it.
	Truncated bool
	// Err is a failure that is not a bound. Under the paper transport nothing is
	// being checked, so this means the specification could not generate its own
	// side — a bug in the specification rather than a rejected conversation.
	Err error
}

// Exploration is every path the search reached, with what it left out.
type Exploration struct {
	Paths  []Path
	Bounds Bounds
	// Complete is false when MaxPaths cut the search short, so paths that exist were
	// never run at all.
	Complete bool
	// Pruned counts branches the Repeats bound declined to follow.
	Pruned int
}

// Truncated counts paths that stopped at a bound.
func (ex *Exploration) Truncated() int {
	n := 0
	for _, p := range ex.Paths {
		if p.Truncated {
			n++
		}
	}
	return n
}

// Failed collects paths the specification could not run to the end.
func (ex *Exploration) Failed() []Path {
	var out []Path
	for _, p := range ex.Paths {
		if p.Err != nil {
			out = append(out, p)
		}
	}
	return out
}

// Explore enumerates the conversations a specification permits.
//
// It takes a factory rather than a configuration because every path re-executes
// the specification from the top, and a specification mutates its configuration as
// it goes: the polling one advances a phase, appends to a History and counts
// attempts. Replaying a prefix into a configuration another path had already moved
// would explore something that is not in the specification at all.
func Explore[C any](newConfig func() *C, body func(*Session[C]), b Bounds) *Exploration {
	b = b.withDefaults()
	ex := &Exploration{Bounds: b, Complete: true}

	// Breadth-first, so that hitting MaxPaths leaves a picture that is complete near
	// the root rather than one with whole shallow branches missing.
	queue := [][]bool{nil}
	for len(queue) > 0 {
		if len(ex.Paths) >= b.MaxPaths {
			ex.Complete = false
			break
		}
		pre := queue[0]
		queue = queue[1:]

		p := runPath(newConfig(), body, pre, b)
		ex.Paths = append(ex.Paths, p)
		if p.Err != nil {
			// The specification could not produce this conversation, so there is nothing
			// downstream of it to explore.
			continue
		}

		// Every choice this path defaulted to false is a branch nobody has taken yet.
		// Scheduling prefix[:i] ++ [true] for each is what enumerates each path exactly
		// once: the child agrees with this path up to i, so it is bound to reach choice
		// i and diverge there.
		cs := p.Result.Choices
		for i := len(pre); i < len(cs); i++ {
			if cs[i].Taken {
				continue
			}
			if takenBefore(cs[:i], cs[i].Label) >= b.repeatsFor(cs[i].Label) {
				ex.Pruned++
				continue
			}
			next := make([]bool, i+1)
			for j := 0; j < i; j++ {
				next[j] = cs[j].Taken
			}
			next[i] = true
			queue = append(queue, next)
		}
	}
	return ex
}

func takenBefore(before []Choice, label string) int {
	n := 0
	for _, c := range before {
		if c.Taken && c.Label == label {
			n++
		}
	}
	return n
}

func runPath[C any](cfg *C, body func(*Session[C]), pre []bool, b Bounds) Path {
	res, err := Run(cfg, body, Options{
		Transport: &paper{maxMessages: b.MaxMessages},
		Policy:    (&prefix{choices: pre, max: b.MaxChoices}).decide,
	})
	p := Path{Result: res}
	switch {
	case err == nil:
	case errors.Is(err, ErrTruncated):
		p.Truncated = true
	default:
		p.Err = err
	}
	return p
}
