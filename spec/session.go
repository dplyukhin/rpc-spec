package spec

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Mode is what the runner does with a message: check it against a real peer, or
// generate it on behalf of a mocked one. A specification never mentions its
// mode; the runner decides, per direction.
type Mode int

const (
	// Check means the message's producer is real, so the runner verifies it.
	Check Mode = iota
	// Synth means the message's producer is mocked, so the runner generates it.
	Synth
)

func (m Mode) String() string {
	if m == Check {
		return "check"
	}
	return "synth"
}

// Transport is the wire the specification runs over. Its two implementations —
// a mock Server and a mock Worker — are what make checker mode and synthesizer
// mode the same mode. Correlation between a request and its response is the
// transport's job, so a specification never matches them up by inspecting
// fields.
//
// A blocking Send registers a pattern — here just a message type, because this
// prototype runs a single specification process. The runner delivers the first
// observed message matching that pattern and holds the rest back, which is what
// makes interchangeable concurrency (several identical long polls outstanding at
// once) tolerable without a scheduler.
type Transport interface {
	// Mode reports whether messages travelling in dir come from a real peer.
	Mode(dir Direction) Mode
	// Recv returns the first observed message of type want travelling in dir,
	// blocking until one arrives.
	Recv(dir Direction, want protoreflect.FullName) (proto.Message, error)
	// Peek is Recv without consuming the message, over a set of wanted types: it
	// returns the first observed message whose type is any of them. A lookahead
	// often has to ask which of several replies the peer chose, so naming a single
	// type would block until the timeout whenever the peer chose another.
	Peek(dir Direction, want []protoreflect.FullName) (proto.Message, error)
	// Emit hands a generated message to the peer.
	Emit(role Role, m proto.Message) error
}

// Policy resolves the choices a synthesizer has to make. A full runner would
// explore these by re-executing the specification with a modified choice prefix;
// this prototype takes a fixed policy instead, which is enough to drive a mock
// but does no searching or shrinking.
type Policy func(label string) bool

// core is the mode-independent machinery behind a Session. It is separated from
// Session[C] so that the constraint and cursor code can be written once, without
// the configuration type parameter infecting everything.
type core struct {
	t      Transport
	policy Policy
	trace  func(string, ...any)

	// self is the *Session[C] handed to the specification, so that a nested
	// cursor function can be invoked with the right static type.
	self any

	seq        int
	seed       int
	transcript []Entry
	choices    []Choice

	// The message currently being checked or generated, for diagnostics.
	curRole Role
	curType protoreflect.FullName
	curMsg  proto.Message
}

// Session is the handle through which every effect in a specification passes.
// Because it is the only channel to the outside world, a specification is a pure
// function of the oracle: no I/O, no clock reads, no nondeterminism.
type Session[C any] struct {
	*core
	// C is the specification's own state. It is ordinary mutable Go state; there
	// is no transaction management because nothing is rolled back.
	C *C
}

// Result is the conversation a run produced: the messages, and the branch
// decisions that selected them.
type Result struct {
	Transcript []Entry
	Choices    []Choice
}

func (r *Result) String() string {
	s := ""
	for _, st := range r.Steps() {
		if st.Entry != nil {
			s += st.Entry.String() + "\n"
		} else {
			s += "    " + st.Choice.String() + "\n"
		}
	}
	return s
}

// Options configures a run.
type Options struct {
	Transport Transport
	Policy    Policy
	// Trace, if set, receives a line per message.
	Trace func(format string, args ...any)
}

// Run executes a specification once. The specification is ordinary Go, so it
// signals failure by panicking with a Violation, which is recovered here.
func Run[C any](cfg *C, body func(*Session[C]), o Options) (res *Result, err error) {
	co := &core{t: o.Transport, policy: o.Policy, trace: o.Trace}
	if co.policy == nil {
		co.policy = func(string) bool { return false }
	}
	if co.trace == nil {
		co.trace = func(string, ...any) {}
	}
	s := &Session[C]{core: co, C: cfg}
	co.self = s

	defer func() {
		res = &Result{Transcript: co.transcript, Choices: co.choices}
		switch r := recover().(type) {
		case nil:
		case *specFailure:
			r.v.Transcript, r.v.Choices = co.transcript, co.choices
			err = r.v
		case *abortErr:
			err = r.err
		default:
			panic(r)
		}
	}()

	body(s)
	return &Result{Transcript: co.transcript, Choices: co.choices}, nil
}

// abortErr is a transport-level failure, as distinct from a specification
// violation: the conversation could not continue at all.
type abortErr struct{ err error }

func (co *core) abort(err error) {
	panic(&abortErr{err: fmt.Errorf("at message #%d: %w", co.seq+1, err)})
}

func (co *core) fail(cursor, constraint string, p *problem) {
	file, line, fn := callSite()
	at := cursor
	if p.path != "" {
		if at != "" {
			at += "."
		}
		at += p.path
	}
	panic(&specFailure{v: &Violation{
		SpecFile: file, SpecLine: line, SpecFunc: fn,
		Seq: co.seq, Role: co.curRole, MsgType: co.curType,
		Cursor: at, Constraint: constraint, Detail: p.detail,
		Msg: co.curMsg,
	}})
}

// Send names a message, constrains it, and returns it. Whether the message is
// checked against a real peer or generated for a mocked one is decided by the
// runner, not by this call.
func Send[T proto.Message, C any](s *Session[C], cs ...Constraint) T {
	var zero T
	md := zero.ProtoReflect().Descriptor()
	role, err := roleOf(md.FullName())
	if err != nil {
		s.abort(err)
	}

	mode := s.t.Mode(role.Direction)
	s.seq++

	var msg proto.Message
	if mode == Synth {
		msg = zero.ProtoReflect().New().Interface()
		s.curRole, s.curType, s.curMsg = role, md.FullName(), msg
		applyTo(s.core, Synth, msg.ProtoReflect(), "", cs)
		if err := s.t.Emit(role, msg); err != nil {
			s.abort(err)
		}
	} else {
		got, err := s.t.Recv(role.Direction, md.FullName())
		if err != nil {
			s.abort(err)
		}
		s.curRole, s.curType, s.curMsg = role, md.FullName(), got
		if name := got.ProtoReflect().Descriptor().FullName(); name != md.FullName() {
			s.fail("", fmt.Sprintf("Send[%s]", md.Name()),
				&problem{detail: fmt.Sprintf("expected a %s next, observed a %s", md.Name(), name.Name())})
		}
		msg = got
		applyTo(s.core, Check, msg.ProtoReflect(), "", cs)
	}

	e := Entry{Seq: s.seq, Role: role, Mode: mode, Msg: msg}
	s.transcript = append(s.transcript, e)
	s.trace("%s", e)
	return msg.(T)
}

// applyTo runs a constraint list against one message. Nested is handled here
// rather than through the Constraint interface because it re-enters the
// specification rather than touching a single field.
func applyTo(co *core, mode Mode, m protoreflect.Message, cursor string, cs []Constraint) {
	g := &genctx{seed: &co.seed}
	for _, c := range cs {
		var p *problem
		if n, ok := c.(*nestedC); ok {
			p = n.run(co, mode, m, cursor)
		} else if mode == Check {
			p = c.check(m)
		} else {
			p = c.solve(m, g)
		}
		if p != nil {
			co.fail(cursor, c.describe(), p)
		}
	}
}

// Maybe is a branch the runner resolves. In synthesizer mode it is a choice; in
// checker mode it must be derivable from what was observed, which is why it
// takes a lookahead rather than being a bare coin flip.
func (co *core) Maybe(label string, p Peek) bool {
	if co.t.Mode(p.dir()) == Check {
		m, err := co.t.Peek(p.dir(), p.msgTypes())
		if err != nil {
			co.abort(fmt.Errorf("looking ahead for %q: %w", label, err))
		}
		taken := p.eval(m)
		co.trace("    branch %-20s %v (from lookahead: %s)", label, taken, p.describe())
		co.record(label, taken, true, p.describe())
		return taken
	}
	taken := co.policy(label)
	co.trace("    branch %-20s %v (from policy)", label, taken)
	// describe() is recorded even though the Peek never ran: what the lookahead would
	// have asked is the best available name for the branch, and a renderer that draws
	// the specification's structure rather than one run of it has nothing else to use.
	co.record(label, taken, false, p.describe())
	return taken
}

// record notes a branch decision against the number of messages that had already
// crossed the wire, which is what places it among them.
func (co *core) record(label string, taken, fromLookahead bool, describe string) {
	// callSite walks out to the specification's own frame, so this is the line the
	// branch is written on rather than anything in the runner.
	file, line, _ := callSite()
	co.choices = append(co.choices, Choice{
		AfterSeq:      co.seq,
		Label:         label,
		Taken:         taken,
		FromLookahead: fromLookahead,
		Describe:      describe,
		Site:          fmt.Sprintf("%s:%d", file, line),
	})
}

// Peek is a lookahead used to discriminate a branch from an observed message.
type Peek interface {
	dir() Direction
	// msgTypes lists every message type the lookahead is prepared to see, so the
	// transport can hand back whichever of them arrives first.
	msgTypes() []protoreflect.FullName
	eval(m proto.Message) bool
	describe() string
}

// PeekEmpty takes a branch when the next message of the given type has no value
// in the named field. This is how "the long poll returned nothing" is recognized
// in checker mode.
func PeekEmpty[T proto.Message](path string) Peek {
	var zero T
	md := zero.ProtoReflect().Descriptor()
	role, err := roleOf(md.FullName())
	if err != nil {
		panic(err)
	}
	return &peekEmpty{role: role, name: md.FullName(), path: path}
}

type peekEmpty struct {
	role Role
	name protoreflect.FullName
	path string
}

func (p *peekEmpty) dir() Direction { return p.role.Direction }

func (p *peekEmpty) msgTypes() []protoreflect.FullName { return []protoreflect.FullName{p.name} }

func (p *peekEmpty) describe() string {
	return fmt.Sprintf("next %s has empty %q", p.name.Name(), p.path)
}

func (p *peekEmpty) eval(m proto.Message) bool {
	if m == nil || m.ProtoReflect().Descriptor().FullName() != p.name {
		return false
	}
	r, err := resolve(m.ProtoReflect(), p.path, false)
	if err != nil {
		return false
	}
	return !(r.reached && r.parent.Has(r.fd))
}

// PeekEq takes a branch when the next message of the given type carries a
// particular value in the named field.
func PeekEq[T proto.Message](path string, want any) Peek {
	var zero T
	md := zero.ProtoReflect().Descriptor()
	role, err := roleOf(md.FullName())
	if err != nil {
		panic(err)
	}
	return &peekEq{role: role, name: md.FullName(), path: path, want: want}
}

type peekEq struct {
	role Role
	name protoreflect.FullName
	path string
	want any
}

func (p *peekEq) dir() Direction { return p.role.Direction }

func (p *peekEq) msgTypes() []protoreflect.FullName { return []protoreflect.FullName{p.name} }

func (p *peekEq) describe() string {
	return fmt.Sprintf("next %s has %s == %v", p.name.Name(), p.path, rawval(p.want))
}

func (p *peekEq) eval(m proto.Message) bool {
	if m == nil || m.ProtoReflect().Descriptor().FullName() != p.name {
		return false
	}
	return (&eqC{path: p.path, want: p.want}).check(m.ProtoReflect()) == nil
}

// PeekIsType takes a branch when the next message the peer sends is a T rather
// than one of the listed alternatives. Not every branch is discriminated by a
// field: sometimes the peer's choice of reply *is* the observation, because the
// choice depends on the peer's own build rather than on anything in the
// conversation so far. A specification uses this to permit either reaction
// without presupposing which one a given peer will have.
//
// Every alternative must travel the same direction as T, and the alternatives
// must be exhaustive: the lookahead blocks until one of the listed types arrives.
// An alternative is named by any instance of it, e.g. &wsv1.FooRequest{}.
func PeekIsType[T proto.Message](alternatives ...proto.Message) Peek {
	var zero T
	md := zero.ProtoReflect().Descriptor()
	role, err := roleOf(md.FullName())
	if err != nil {
		panic(err)
	}
	p := &peekIsType{role: role, name: md.FullName(), names: []protoreflect.FullName{md.FullName()}}
	for _, alt := range alternatives {
		amd := alt.ProtoReflect().Descriptor()
		arole, err := roleOf(amd.FullName())
		if err != nil {
			panic(err)
		}
		if arole.Direction != role.Direction {
			panic(fmt.Sprintf("PeekIsType: %s travels %v but %s travels %v, so they cannot be alternatives",
				md.Name(), role.Direction, amd.Name(), arole.Direction))
		}
		p.names = append(p.names, amd.FullName())
	}
	return p
}

type peekIsType struct {
	role  Role
	name  protoreflect.FullName
	names []protoreflect.FullName
}

func (p *peekIsType) dir() Direction { return p.role.Direction }

func (p *peekIsType) msgTypes() []protoreflect.FullName { return p.names }

func (p *peekIsType) describe() string {
	others := make([]string, 0, len(p.names)-1)
	for _, n := range p.names[1:] {
		others = append(others, string(n.Name()))
	}
	if len(others) == 0 {
		return fmt.Sprintf("next message is a %s", p.name.Name())
	}
	return fmt.Sprintf("next message is a %s rather than a %s",
		p.name.Name(), strings.Join(others, " or a "))
}

func (p *peekIsType) eval(m proto.Message) bool {
	return m != nil && m.ProtoReflect().Descriptor().FullName() == p.name
}
