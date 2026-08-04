package spec

import (
	"fmt"
	"runtime"
	"strings"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Entry is one message in the observed-or-generated conversation.
type Entry struct {
	Seq  int
	Role Role
	Mode Mode
	Msg  proto.Message
}

func (e Entry) String() string {
	verb := "checked"
	if e.Mode == Synth {
		verb = "generated"
	}
	return fmt.Sprintf("#%-2d %-11s %-32s %s", e.Seq, e.Role.Direction, e.Role.Method, verb)
}

// Violation is a message that the specification does not permit.
type Violation struct {
	// Where in the specification the failing constraint was written.
	SpecFile string
	SpecLine int
	SpecFunc string

	Seq        int
	Role       Role
	MsgType    protoreflect.FullName
	Cursor     string // e.g. "history.events[2]", empty for the whole message
	Constraint string
	Detail     string

	Msg        proto.Message
	Transcript []Entry
}

func (v *Violation) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "spec violation at %s:%d (%s)\n", v.SpecFile, v.SpecLine, v.SpecFunc)
	fmt.Fprintf(&b, "  message    #%d %s %s.%s\n", v.Seq, v.Role.Direction, v.Role.Method, v.MsgType.Name())
	if v.Cursor != "" {
		fmt.Fprintf(&b, "  at         %s\n", v.Cursor)
	}
	fmt.Fprintf(&b, "  constraint %s\n", v.Constraint)
	fmt.Fprintf(&b, "  problem    %s\n", v.Detail)
	if v.Msg != nil {
		fmt.Fprintf(&b, "  observed   %s\n", indent(renderMsg(v.Msg), "             "))
	}
	if len(v.Transcript) > 0 {
		b.WriteString("  conversation so far\n")
		for _, e := range v.Transcript {
			fmt.Fprintf(&b, "    %s\n", e)
		}
	}
	return b.String()
}

func renderMsg(m proto.Message) string {
	s := prototext.MarshalOptions{Multiline: true, Indent: "  "}.Format(m)
	return strings.TrimRight(s, "\n")
}

func indent(s, pad string) string {
	lines := strings.Split(s, "\n")
	for i := 1; i < len(lines); i++ {
		lines[i] = pad + lines[i]
	}
	return strings.Join(lines, "\n")
}

// specFailure carries a Violation out of a running specification. The
// specification is ordinary Go with no error returns, so control leaves it by
// panicking; Run recovers at the process boundary.
type specFailure struct{ v *Violation }

// callSite walks up the stack to the first frame outside this package, so a
// diagnostic points at the line of specification that wrote the constraint
// rather than at the runner's internals.
func callSite() (file string, line int, fn string) {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if !strings.HasSuffix(pkgOf(f.Function), "/spec") && pkgOf(f.Function) != "spec" {
			return trimPath(f.File), f.Line, shortFunc(f.Function)
		}
		if !more {
			return trimPath(f.File), f.Line, shortFunc(f.Function)
		}
	}
}

func pkgOf(fullFunc string) string {
	i := strings.LastIndex(fullFunc, "/")
	rest := fullFunc[i+1:]
	if j := strings.Index(rest, "."); j >= 0 {
		return fullFunc[:i+1] + rest[:j]
	}
	return fullFunc
}

func shortFunc(fullFunc string) string {
	if i := strings.LastIndex(fullFunc, "/"); i >= 0 {
		return fullFunc[i+1:]
	}
	return fullFunc
}

func trimPath(p string) string {
	// Keep the last two path elements; absolute paths add noise to test output.
	parts := strings.Split(p, "/")
	if len(parts) <= 2 {
		return p
	}
	return strings.Join(parts[len(parts)-2:], "/")
}
