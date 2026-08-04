package spec

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Constraint is one clause of a message specification. Every constraint must be
// both checkable against an observed message and solvable into a generated one;
// that is what makes a single specification usable in both directions, and it is
// why the sublanguage is a closed set rather than arbitrary Go predicates.
type Constraint interface {
	check(m protoreflect.Message) *problem
	solve(m protoreflect.Message, g *genctx) *problem
	describe() string
}

type problem struct {
	path   string
	detail string
}

func probf(path, format string, args ...any) *problem {
	return &problem{path: path, detail: fmt.Sprintf(format, args...)}
}

// genctx carries the deterministic state a solver needs to invent values.
type genctx struct{ seed *int }

func (g *genctx) next() int { *g.seed++; return *g.seed }

// ---------------------------------------------------------------- Eq

// Eq constrains a field to a specific value. Because the specification is an
// ordinary program, the expected value is written at the point of use with the
// configuration already in scope: Eq("namespace", s.C.Namespace).
func Eq(path string, want any) Constraint { return &eqC{path: path, want: want} }

type eqC struct {
	path string
	want any
}

func (c *eqC) describe() string { return fmt.Sprintf("Eq(%q, %v)", c.path, rawval(c.want)) }

func (c *eqC) check(m protoreflect.Message) *problem {
	r, err := resolve(m, c.path, false)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	want, err := toValue(r.fd, c.want)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	if !r.reached {
		return probf(c.path, "path is not reachable: an enclosing message is absent, so the field cannot equal %s",
			fmtValue(r.fd, want))
	}
	got := r.parent.Get(r.fd)
	if !valueEqual(r.fd, got, want) {
		return probf(c.path, "expected %s, observed %s", fmtValue(r.fd, want), fmtValue(r.fd, got))
	}
	return nil
}

func (c *eqC) solve(m protoreflect.Message, _ *genctx) *problem {
	r, err := resolve(m, c.path, true)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	want, err := toValue(r.fd, c.want)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	r.parent.Set(r.fd, want)
	return nil
}

// ---------------------------------------------------------------- OneOfValues

// OneOfValues constrains a field to one of a set of values. In checker mode the
// observed value must be a member; in synthesizer mode the first is chosen.
func OneOfValues(path string, want ...any) Constraint {
	return &oneOfC{path: path, want: want}
}

type oneOfC struct {
	path string
	want []any
}

func (c *oneOfC) describe() string {
	parts := make([]string, len(c.want))
	for i, w := range c.want {
		parts[i] = fmt.Sprint(rawval(w))
	}
	return fmt.Sprintf("OneOfValues(%q, %s)", c.path, strings.Join(parts, ", "))
}

func (c *oneOfC) check(m protoreflect.Message) *problem {
	r, err := resolve(m, c.path, false)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	if !r.reached {
		return probf(c.path, "path is not reachable: an enclosing message is absent")
	}
	got := r.parent.Get(r.fd)
	for _, w := range c.want {
		wv, err := toValue(r.fd, w)
		if err != nil {
			return probf(c.path, "%v", err)
		}
		if valueEqual(r.fd, got, wv) {
			return nil
		}
	}
	return probf(c.path, "observed %s, which is not one of the permitted values", fmtValue(r.fd, got))
}

func (c *oneOfC) solve(m protoreflect.Message, g *genctx) *problem {
	if len(c.want) == 0 {
		return probf(c.path, "OneOfValues needs at least one value to generate")
	}
	return (&eqC{path: c.path, want: c.want[0]}).solve(m, g)
}

// ---------------------------------------------------------------- presence

// Empty requires a field to hold no value: an empty string or byte string, an
// empty list, a zero number, or an absent message.
func Empty(path string) Constraint { return &presenceC{path: path, want: false, name: "Empty"} }

// Absent is Empty, named for message and optional fields.
func Absent(path string) Constraint { return &presenceC{path: path, want: false, name: "Absent"} }

// NonEmpty requires a field to hold some value. In synthesizer mode a
// deterministic value is invented.
func NonEmpty(path string) Constraint { return &presenceC{path: path, want: true, name: "NonEmpty"} }

// Present is NonEmpty, named for message and optional fields.
func Present(path string) Constraint { return &presenceC{path: path, want: true, name: "Present"} }

// EmptyList requires a repeated field to have no elements.
func EmptyList(path string) Constraint {
	return &presenceC{path: path, want: false, name: "EmptyList"}
}

type presenceC struct {
	path string
	want bool
	name string
}

func (c *presenceC) describe() string { return fmt.Sprintf("%s(%q)", c.name, c.path) }

func (c *presenceC) check(m protoreflect.Message) *problem {
	r, err := resolve(m, c.path, false)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	has := r.reached && r.parent.Has(r.fd)
	if has == c.want {
		return nil
	}
	if c.want {
		return probf(c.path, "expected a value, observed none")
	}
	return probf(c.path, "expected no value, observed %s", fmtValue(r.fd, r.parent.Get(r.fd)))
}

func (c *presenceC) solve(m protoreflect.Message, g *genctx) *problem {
	r, err := resolve(m, c.path, true)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	if !c.want {
		r.parent.Clear(r.fd)
		return nil
	}
	v, err := inventValue(r.parent, r.fd, g)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	r.parent.Set(r.fd, v)
	return nil
}

// ---------------------------------------------------------------- LengthBetween

// LengthBetween bounds the length of a repeated, string, or bytes field.
func LengthBetween(path string, min, max int) Constraint {
	return &lenC{path: path, min: min, max: max}
}

type lenC struct {
	path     string
	min, max int
}

func (c *lenC) describe() string {
	return fmt.Sprintf("LengthBetween(%q, %d, %d)", c.path, c.min, c.max)
}

func (c *lenC) check(m protoreflect.Message) *problem {
	r, err := resolve(m, c.path, false)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	n := 0
	if r.reached {
		n = valueLen(r.fd, r.parent.Get(r.fd))
	}
	if n < c.min || n > c.max {
		return probf(c.path, "expected length in [%d, %d], observed %d", c.min, c.max, n)
	}
	return nil
}

func (c *lenC) solve(protoreflect.Message, *genctx) *problem {
	// Length is a consequence of what the nested specification appends, so there
	// is nothing to solve here; the check side still applies when generating is
	// not in play.
	return nil
}

// ---------------------------------------------------------------- Counter

// Counter constrains a monotone field such as event_id: in checker mode the
// observed value must equal the counter, in synthesizer mode the counter's value
// is written. Either way the counter advances, so contiguity is enforced by
// construction. The counter lives in the specification's own configuration,
// which is ordinary mutable Go state.
func Counter(path string, next *int64) Constraint { return &counterC{path: path, next: next} }

type counterC struct {
	path string
	next *int64
}

func (c *counterC) describe() string {
	return fmt.Sprintf("Counter(%q) = %d", c.path, *c.next)
}

func (c *counterC) check(m protoreflect.Message) *problem {
	r, err := resolve(m, c.path, false)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	if !r.reached {
		return probf(c.path, "path is not reachable: an enclosing message is absent")
	}
	got := r.parent.Get(r.fd).Int()
	if got != *c.next {
		return probf(c.path, "expected the next value %d, observed %d (values must be contiguous)", *c.next, got)
	}
	*c.next++
	return nil
}

func (c *counterC) solve(m protoreflect.Message, _ *genctx) *problem {
	r, err := resolve(m, c.path, true)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	r.parent.Set(r.fd, protoreflect.ValueOfInt64(*c.next))
	*c.next++
	return nil
}

// ---------------------------------------------------------------- PositiveDuration

// PositiveDuration requires a google.protobuf.Duration field to be greater than
// zero. It is the constraint to reach for when the contract cares that a timeout
// was set but not what it was set to, so the same clause can check a real Server
// and drive a mock.
func PositiveDuration(path string) Constraint { return &posDurC{path: path} }

type posDurC struct{ path string }

func (c *posDurC) describe() string { return fmt.Sprintf("PositiveDuration(%q)", c.path) }

func (c *posDurC) check(m protoreflect.Message) *problem {
	r, err := resolve(m, c.path, false)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	if !r.reached || !r.parent.Has(r.fd) {
		return probf(c.path, "expected a positive duration, observed none")
	}
	d, ok := r.parent.Get(r.fd).Message().Interface().(*durationpb.Duration)
	if !ok {
		return probf(c.path, "%s is not a google.protobuf.Duration", r.fd.FullName())
	}
	if d.AsDuration() <= 0 {
		return probf(c.path, "expected a positive duration, observed %v", d.AsDuration())
	}
	return nil
}

func (c *posDurC) solve(m protoreflect.Message, _ *genctx) *problem {
	return (&eqC{path: c.path, want: 10 * time.Second}).solve(m, nil)
}

// ---------------------------------------------------------------- Opaque

// Opaque declares that a field's value is not part of the contract: it is
// checked for presence only and generated as an arbitrary well-typed value. This
// is the escape hatch that keeps the closed constraint set from becoming a
// straitjacket.
func Opaque(path string) Constraint { return &opaqueC{path: path} }

type opaqueC struct{ path string }

func (c *opaqueC) describe() string { return fmt.Sprintf("Opaque(%q)", c.path) }

func (c *opaqueC) check(m protoreflect.Message) *problem {
	return (&presenceC{path: c.path, want: true, name: "Opaque"}).check(m)
}

func (c *opaqueC) solve(m protoreflect.Message, g *genctx) *problem {
	return (&presenceC{path: c.path, want: true, name: "Opaque"}).solve(m, g)
}

// ---------------------------------------------------------------- value plumbing

func toValue(fd protoreflect.FieldDescriptor, v any) (protoreflect.Value, error) {
	if fd.IsList() || fd.IsMap() {
		return protoreflect.Value{}, fmt.Errorf("cannot assign a scalar to repeated field %s", fd.FullName())
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		b, ok := v.(bool)
		if !ok {
			return protoreflect.Value{}, typeErr(fd, v, "bool")
		}
		return protoreflect.ValueOfBool(b), nil

	case protoreflect.StringKind:
		s, ok := v.(string)
		if !ok {
			return protoreflect.Value{}, typeErr(fd, v, "string")
		}
		return protoreflect.ValueOfString(s), nil

	case protoreflect.BytesKind:
		switch t := v.(type) {
		case []byte:
			return protoreflect.ValueOfBytes(t), nil
		case string:
			return protoreflect.ValueOfBytes([]byte(t)), nil
		}
		return protoreflect.Value{}, typeErr(fd, v, "[]byte")

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, ok := asInt(v)
		if !ok {
			return protoreflect.Value{}, typeErr(fd, v, "int32")
		}
		return protoreflect.ValueOfInt32(int32(n)), nil

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, ok := asInt(v)
		if !ok {
			return protoreflect.Value{}, typeErr(fd, v, "int64")
		}
		return protoreflect.ValueOfInt64(n), nil

	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, ok := asInt(v)
		if !ok {
			return protoreflect.Value{}, typeErr(fd, v, "uint32")
		}
		return protoreflect.ValueOfUint32(uint32(n)), nil

	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, ok := asInt(v)
		if !ok {
			return protoreflect.Value{}, typeErr(fd, v, "uint64")
		}
		return protoreflect.ValueOfUint64(uint64(n)), nil

	case protoreflect.EnumKind:
		switch t := v.(type) {
		case protoreflect.Enum:
			return protoreflect.ValueOfEnum(t.Number()), nil
		case protoreflect.EnumNumber:
			return protoreflect.ValueOfEnum(t), nil
		case int:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(t)), nil
		case int32:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(t)), nil
		}
		return protoreflect.Value{}, typeErr(fd, v, "enum")

	case protoreflect.MessageKind, protoreflect.GroupKind:
		switch t := v.(type) {
		case time.Duration:
			if fd.Message().FullName() != "google.protobuf.Duration" {
				return protoreflect.Value{}, typeErr(fd, v, string(fd.Message().FullName()))
			}
			return protoreflect.ValueOfMessage(durationpb.New(t).ProtoReflect()), nil
		case time.Time:
			if fd.Message().FullName() != "google.protobuf.Timestamp" {
				return protoreflect.Value{}, typeErr(fd, v, string(fd.Message().FullName()))
			}
			return protoreflect.ValueOfMessage(timestamppb.New(t).ProtoReflect()), nil
		case proto.Message:
			return protoreflect.ValueOfMessage(t.ProtoReflect()), nil
		}
		return protoreflect.Value{}, typeErr(fd, v, string(fd.Message().FullName()))
	}
	return protoreflect.Value{}, fmt.Errorf("unsupported field kind %v for %s", fd.Kind(), fd.FullName())
}

func typeErr(fd protoreflect.FieldDescriptor, v any, want string) error {
	return fmt.Errorf("%s is %v, so it needs a %s, not %T", fd.FullName(), fd.Kind(), want, v)
}

func asInt(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	}
	return 0, false
}

func valueEqual(fd protoreflect.FieldDescriptor, a, b protoreflect.Value) bool {
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		return proto.Equal(a.Message().Interface(), b.Message().Interface())
	}
	return a.Equal(b)
}

func valueLen(fd protoreflect.FieldDescriptor, v protoreflect.Value) int {
	switch {
	case fd.IsList():
		return v.List().Len()
	case fd.IsMap():
		return v.Map().Len()
	case fd.Kind() == protoreflect.StringKind:
		return len(v.String())
	case fd.Kind() == protoreflect.BytesKind:
		return len(v.Bytes())
	}
	return 0
}

// inventValue produces a deterministic, well-typed value for a field the runner
// must fill in but the specification did not pin down.
func inventValue(parent protoreflect.Message, fd protoreflect.FieldDescriptor, g *genctx) (protoreflect.Value, error) {
	n := g.next()
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(fmt.Sprintf("%s-%d", fd.Name(), n)), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte(fmt.Sprintf("%s-%d", fd.Name(), n))), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(int32(n)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(int64(n)), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(uint32(n)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(uint64(n)), nil
	case protoreflect.EnumKind:
		// The first named value other than the zero (UNSPECIFIED) one.
		vals := fd.Enum().Values()
		for i := 0; i < vals.Len(); i++ {
			if vals.Get(i).Number() != 0 {
				return protoreflect.ValueOfEnum(vals.Get(i).Number()), nil
			}
		}
		return protoreflect.ValueOfEnum(0), nil
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protoreflect.ValueOfMessage(parent.NewField(fd).Message()), nil
	}
	return protoreflect.Value{}, fmt.Errorf("cannot invent a value for %s of kind %v", fd.FullName(), fd.Kind())
}

func fmtValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) string {
	switch {
	case fd.IsList():
		return fmt.Sprintf("a list of %d element(s)", v.List().Len())
	case fd.Kind() == protoreflect.BytesKind:
		b := v.Bytes()
		if len(b) == 0 {
			return `""`
		}
		return fmt.Sprintf("%q (%d bytes)", string(b), len(b))
	case fd.Kind() == protoreflect.EnumKind:
		n := v.Enum()
		if ev := fd.Enum().Values().ByNumber(n); ev != nil {
			return fmt.Sprintf("%s (%d)", ev.Name(), n)
		}
		return fmt.Sprintf("<unknown enum number %d>", n)
	case fd.Kind() == protoreflect.StringKind:
		return fmt.Sprintf("%q", v.String())
	case fd.Kind() == protoreflect.MessageKind, fd.Kind() == protoreflect.GroupKind:
		return "{" + strings.TrimSpace(strings.ReplaceAll(renderMsg(v.Message().Interface()), "\n", " ")) + "}"
	}
	return fmt.Sprint(v.Interface())
}

// rawval renders an expected value for a constraint description.
func rawval(v any) any {
	switch t := v.(type) {
	case []byte:
		return fmt.Sprintf("%q", string(t))
	case string:
		return fmt.Sprintf("%q", t)
	case protoreflect.Enum:
		return fmt.Sprintf("%v(%d)", t, t.Number())
	}
	return v
}
