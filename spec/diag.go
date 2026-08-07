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

// Choice is one branch decision a run made. Maybe is the only place a run consumes
// nondeterminism, so the choices are the whole of what distinguishes one run of a
// specification from another — which is why they are recorded rather than only
// traced, and why they are what a renderer of the specification's own structure
// has to work from.
type Choice struct {
	// AfterSeq is how many messages had crossed the wire when the decision was made,
	// which is what places it among them. Two choices with the same AfterSeq are
	// ordered by their position in Result.Choices.
	AfterSeq int
	Label    string
	Taken    bool
	// FromLookahead separates a decision read off the peer's next message from one
	// the policy made. In checker mode the conversation decides; in synthesizer mode
	// nothing is on the wire yet and the runner chooses.
	FromLookahead bool
	// Describe is the lookahead's own account of what discriminates this branch, kept
	// whether or not the lookahead ran — in synthesizer mode the Peek is never
	// evaluated, but what it would have asked is still the clearest name for the
	// branch. "next message is a RespondWorkflowTaskFailedRequest rather than a
	// RespondWorkflowTaskCompletedRequest" says far more than the label
	// "worker-cannot-handle-event".
	Describe string
}

func (c Choice) String() string {
	src := "policy"
	if c.FromLookahead {
		src = "lookahead: " + c.Describe
	}
	return fmt.Sprintf("branch %-24s %-5v (%s)", c.Label, c.Taken, src)
}

// Step is one thing that happened in a run: a message or a branch decision.
// Exactly one field is non-nil.
type Step struct {
	Entry  *Entry
	Choice *Choice
}

// Steps interleaves the messages and the branch decisions back into the order they
// happened. Choices are stored apart from the transcript so that Entry stays "one
// message" and the transcript stays a record of the wire — but every consumer that
// shows a run as it unfolded wants them merged, so the merge lives here instead of
// in each of them.
func (r *Result) Steps() []Step {
	steps := make([]Step, 0, len(r.Transcript)+len(r.Choices))
	ci := 0
	for i := range r.Transcript {
		e := &r.Transcript[i]
		// A decision taken while fewer than e.Seq messages had been sent came first.
		for ci < len(r.Choices) && r.Choices[ci].AfterSeq < e.Seq {
			steps = append(steps, Step{Choice: &r.Choices[ci]})
			ci++
		}
		steps = append(steps, Step{Entry: e})
	}
	for ; ci < len(r.Choices); ci++ {
		steps = append(steps, Step{Choice: &r.Choices[ci]})
	}
	return steps
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
	Choices    []Choice
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
		// The branches are listed alongside the messages: which way the conversation
		// went is often the reason the message that failed was the one expected.
		b.WriteString("  conversation so far\n")
		for _, st := range (&Result{Transcript: v.Transcript, Choices: v.Choices}).Steps() {
			if st.Entry != nil {
				fmt.Fprintf(&b, "    %s\n", *st.Entry)
			} else {
				fmt.Fprintf(&b, "        %s\n", *st.Choice)
			}
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
