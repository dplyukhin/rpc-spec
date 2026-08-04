package spec

import (
	"fmt"

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
	// Peek is Recv without consuming the message.
	Peek(dir Direction, want protoreflect.FullName) (proto.Message, error)
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

// Result is the conversation a run produced.
type Result struct {
	Transcript []Entry
}

func (r *Result) String() string {
	s := ""
	for _, e := range r.Transcript {
		s += e.String() + "\n"
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
		res = &Result{Transcript: co.transcript}
		switch r := recover().(type) {
		case nil:
		case *specFailure:
			r.v.Transcript = co.transcript
			err = r.v
		case *abortErr:
			err = r.err
		default:
			panic(r)
		}
	}()

	body(s)
	return &Result{Transcript: co.transcript}, nil
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
		m, err := co.t.Peek(p.dir(), p.msgType())
		if err != nil {
			co.abort(fmt.Errorf("looking ahead for %q: %w", label, err))
		}
		taken := p.eval(m)
		co.trace("    branch %-20s %v (from lookahead: %s)", label, taken, p.describe())
		return taken
	}
	taken := co.policy(label)
	co.trace("    branch %-20s %v (from policy)", label, taken)
	return taken
}

// Peek is a lookahead used to discriminate a branch from an observed message.
type Peek interface {
	dir() Direction
	msgType() protoreflect.FullName
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

func (p *peekEmpty) msgType() protoreflect.FullName { return p.name }

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

func (p *peekEq) msgType() protoreflect.FullName { return p.name }

func (p *peekEq) describe() string {
	return fmt.Sprintf("next %s has %s == %v", p.name.Name(), p.path, rawval(p.want))
}

func (p *peekEq) eval(m proto.Message) bool {
	if m == nil || m.ProtoReflect().Descriptor().FullName() != p.name {
		return false
	}
	return (&eqC{path: p.path, want: p.want}).check(m.ProtoReflect()) == nil
}
