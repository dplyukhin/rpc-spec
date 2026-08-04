package spec

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Constructing values that do not exist yet.
//
// Testing an old SDK against a future Server means emitting an enum number, a
// field number, or a oneof alternative that the peer's generated code does not
// know. Go's protoreflect supports this directly: an enum field accepts any
// protoreflect.EnumNumber, and Message.SetUnknown writes raw wire bytes. Both
// constraints below are thin wrappers over first-party API.

// UnknownEnum constrains an enum field to a numeric value with no name in the
// current descriptor. In synthesizer mode it writes that number; in checker mode
// it requires the observed number to match, which is how you assert that a peer
// echoed a value it could not have understood.
func UnknownEnum(path string, number int32) Constraint {
	return &unknownEnumC{path: path, number: protoreflect.EnumNumber(number)}
}

type unknownEnumC struct {
	path   string
	number protoreflect.EnumNumber
}

func (c *unknownEnumC) describe() string {
	return fmt.Sprintf("UnknownEnum(%q, %d)", c.path, c.number)
}

func (c *unknownEnumC) check(m protoreflect.Message) *problem {
	r, err := resolve(m, c.path, false)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	if r.fd.Kind() != protoreflect.EnumKind {
		return probf(c.path, "%s is %v, not an enum", r.fd.FullName(), r.fd.Kind())
	}
	if !r.reached {
		return probf(c.path, "path is not reachable: an enclosing message is absent")
	}
	if got := r.parent.Get(r.fd).Enum(); got != c.number {
		return probf(c.path, "expected unknown enum number %d, observed %s",
			c.number, fmtValue(r.fd, r.parent.Get(r.fd)))
	}
	return nil
}

func (c *unknownEnumC) solve(m protoreflect.Message, _ *genctx) *problem {
	r, err := resolve(m, c.path, true)
	if err != nil {
		return probf(c.path, "%v", err)
	}
	if r.fd.Kind() != protoreflect.EnumKind {
		return probf(c.path, "%s is %v, not an enum", r.fd.FullName(), r.fd.Kind())
	}
	if ev := r.fd.Enum().Values().ByNumber(c.number); ev != nil {
		return probf(c.path, "%d is %s in the current descriptor, so it is not an unknown value; "+
			"pick a number outside the enum", c.number, ev.Name())
	}
	r.parent.Set(r.fd, protoreflect.ValueOfEnum(c.number))
	return nil
}

// UnknownField attaches a field number that does not exist in the current
// descriptor, carrying a length-delimited payload. It applies to the message the
// cursor is on rather than to a named field.
func UnknownField(number int32, payload []byte) Constraint {
	return &unknownFieldC{number: protowire.Number(number), payload: payload}
}

type unknownFieldC struct {
	number  protowire.Number
	payload []byte
}

func (c *unknownFieldC) describe() string {
	return fmt.Sprintf("UnknownField(%d, %d bytes)", c.number, len(c.payload))
}

func (c *unknownFieldC) check(m protoreflect.Message) *problem {
	if m.Descriptor().Fields().ByNumber(c.number) != nil {
		return probf("", "field number %d is known in %s, so it cannot be an unknown field",
			c.number, m.Descriptor().FullName())
	}
	raw := m.GetUnknown()
	for len(raw) > 0 {
		num, _, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return probf("", "unknown fields are malformed")
		}
		if num == c.number {
			return nil
		}
		skip := protowire.ConsumeFieldValue(num, protowire.Type(raw[0]&7), raw[n:])
		if skip < 0 {
			return probf("", "unknown fields are malformed")
		}
		raw = raw[n+skip:]
	}
	return probf("", "expected unknown field %d to be present", c.number)
}

func (c *unknownFieldC) solve(m protoreflect.Message, _ *genctx) *problem {
	if m.Descriptor().Fields().ByNumber(c.number) != nil {
		return probf("", "field number %d already exists in %s; pick a number outside the message",
			c.number, m.Descriptor().FullName())
	}
	raw := protowire.AppendTag(nil, c.number, protowire.BytesType)
	raw = protowire.AppendBytes(raw, c.payload)
	m.SetUnknown(append(m.GetUnknown(), raw...))
	return nil
}
