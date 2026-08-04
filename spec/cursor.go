package spec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The contents of a message are described with the same primitives, applied to a
// cursor over a field rather than to the wire. A specification is a program over
// a cursor; the wire is just the outermost cursor.

// Cursor is a position on a message nested inside another.
type Cursor struct {
	co   *core
	mode Mode
	msg  protoreflect.Message
	path string
}

// List is a cursor over a repeated field. It has reference semantics, so it can
// be passed by value into a nested specification function and still track how
// much of the list has been consumed.
type List struct{ st *listState }

type listState struct {
	co     *core
	mode   Mode
	fd     protoreflect.FieldDescriptor
	list   protoreflect.List
	path   string
	idx    int
	bounds bool
}

// More reports whether the list has another element to describe, and bounds its
// length. In checker mode it walks the observed elements; in synthesizer mode it
// produces the minimum permitted number, which is enough to drive a mock but is
// not a search over lengths.
func (l List) More(min, max int) bool {
	st := l.st
	if !st.bounds {
		st.bounds = true
		if st.mode == Check {
			if n := st.list.Len(); n < min || n > max {
				st.co.fail(st.path, fmt.Sprintf("LengthBetween(%q, %d, %d)", st.path, min, max),
					probf("", "expected between %d and %d element(s), observed %d", min, max, n))
			}
		}
	}
	if st.mode == Check {
		return st.idx < st.list.Len()
	}
	return st.idx < min
}

// Len returns the number of elements, bounding it as More does.
func (l List) Len(min, max int) int {
	if !l.st.bounds {
		l.More(min, max)
	}
	if l.st.mode == Check {
		return l.st.list.Len()
	}
	return min
}

// Append moves to the next element: in checker mode the next observed one, in
// synthesizer mode a freshly created one.
func (l List) Append() Cursor {
	st := l.st
	path := fmt.Sprintf("%s[%d]", st.path, st.idx)
	if st.mode == Synth {
		el := st.list.NewElement()
		st.list.Append(el)
		st.idx++
		return Cursor{co: st.co, mode: st.mode, msg: el.Message(), path: path}
	}
	if st.idx >= st.list.Len() {
		st.co.fail(st.path, fmt.Sprintf("element %d of %q", st.idx, st.path),
			probf("", "expected another element, but the list has only %d", st.list.Len()))
	}
	el := st.list.Get(st.idx)
	st.idx++
	return Cursor{co: st.co, mode: st.mode, msg: el.Message(), path: path}
}

// At returns a cursor on an existing element without consuming it.
func (l List) At(i int) Cursor {
	st := l.st
	if i >= st.list.Len() {
		st.co.fail(st.path, fmt.Sprintf("element %d of %q", i, st.path),
			probf("", "index %d is out of range; the list has %d element(s)", i, st.list.Len()))
	}
	return Cursor{co: st.co, mode: st.mode, msg: st.list.Get(i).Message(),
		path: fmt.Sprintf("%s[%d]", st.path, i)}
}

// Nested descends into a repeated field and hands it to another part of the
// specification. In checker mode the nested function must consume every observed
// element; leftovers are a violation, so a specification cannot pass by
// describing only a prefix of what arrived.
func Nested[C any](path string, fn func(*Session[C], List)) Constraint {
	return &nestedC{
		path: path,
		desc: fmt.Sprintf("Nested(%q)", path),
		run: func(co *core, mode Mode, m protoreflect.Message, cursor string) *problem {
			r, err := resolve(m, path, mode == Synth)
			if err != nil {
				return probf(path, "%v", err)
			}
			if !r.fd.IsList() {
				return probf(path, "%s is not repeated, so Nested does not apply", r.fd.FullName())
			}
			full := path
			if cursor != "" {
				full = cursor + "." + path
			}
			var pl protoreflect.List
			if mode == Synth {
				pl = r.parent.Mutable(r.fd).List()
			} else {
				if !r.reached {
					return probf(path, "expected a list, but an enclosing message is absent")
				}
				pl = r.parent.Get(r.fd).List()
			}
			st := &listState{co: co, mode: mode, fd: r.fd, list: pl, path: full}
			fn(co.self.(*Session[C]), List{st: st})
			if mode == Check && st.idx < pl.Len() {
				return probf(path, "%d element(s) at the end of the list were not described by the specification",
					pl.Len()-st.idx)
			}
			return nil
		},
	}
}

type nestedC struct {
	path string
	desc string
	run  func(co *core, mode Mode, m protoreflect.Message, cursor string) *problem
}

func (c *nestedC) describe() string                    { return c.desc }
func (c *nestedC) check(protoreflect.Message) *problem { panic("nested is applied by applyTo") }
func (c *nestedC) solve(protoreflect.Message, *genctx) *problem {
	panic("nested is applied by applyTo")
}

// Satisfy applies constraints to the message under a cursor and returns it, so
// everything downstream is plain Go over a typed message.
func Satisfy[T proto.Message, C any](s *Session[C], c Cursor, cs ...Constraint) T {
	applyTo(s.core, c.mode, c.msg, c.path, cs)
	m, ok := c.msg.Interface().(T)
	if !ok {
		var zero T
		s.abort(fmt.Errorf("cursor at %s is a %s, not a %s", c.path,
			c.msg.Descriptor().FullName(), zero.ProtoReflect().Descriptor().FullName()))
	}
	return m
}

// Field constrains a field to one of the listed values and returns it. In checker
// mode it reads and verifies; in synthesizer mode it chooses. A branch written
// with Field is therefore always discriminated by an observable field, which is
// what keeps checking deterministic.
func Field[V any, C any](s *Session[C], c Cursor, path string, vals ...V) V {
	if len(vals) == 0 {
		s.abort(fmt.Errorf("Field(%q) needs at least one permitted value", path))
	}
	desc := fmt.Sprintf("Field(%q, %d permitted value(s))", path, len(vals))

	if c.mode == Synth {
		if p := (&eqC{path: path, want: vals[0]}).solve(c.msg, &genctx{seed: &s.seed}); p != nil {
			s.fail(c.path, desc, p)
		}
		return vals[0]
	}

	r, err := resolve(c.msg, path, false)
	if err != nil {
		s.fail(c.path, desc, probf(path, "%v", err))
	}
	if !r.reached {
		s.fail(c.path, desc, probf(path, "path is not reachable: an enclosing message is absent"))
	}
	got := r.parent.Get(r.fd)
	for _, v := range vals {
		want, err := toValue(r.fd, v)
		if err != nil {
			s.fail(c.path, desc, probf(path, "%v", err))
		}
		if valueEqual(r.fd, got, want) {
			return v
		}
	}
	s.fail(c.path, desc, probf(path, "observed %s, which the specification does not permit here",
		fmtValue(r.fd, got)))
	panic("unreachable")
}
