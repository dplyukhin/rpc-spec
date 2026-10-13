package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/rpcspec/spec"
	"go.temporal.io/rpcspec/specs/polling"
)

// DiagramDirEnv names a directory to write one .mmd file per run into. Unset, the
// diagrams only go to the test log, so an ordinary `go test` writes nothing.
const DiagramDirEnv = "RPCSPEC_DIAGRAM_DIR"

// emitDiagram renders a finished run as a mermaid sequence diagram. Both
// directions produce one: the diagram of a mock-Server run and the diagram of a
// mock-Worker run differ only in which lifeline is labelled as the mock, which is
// the clearest statement available that it really is one specification.
func emitDiagram(t *testing.T, res *spec.Result) {
	t.Helper()
	if res == nil || len(res.Transcript) == 0 {
		return
	}

	diagram := spec.MermaidOptions{
		Title:      diagramTitle(t.Name()),
		Autonumber: true,
		Detail:     polling.Detail,
	}.Render(res)

	t.Logf("sequence diagram:\n%s", diagram)

	dir := os.Getenv(DiagramDirEnv)
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Errorf("creating the diagram directory: %v", err)
		return
	}
	path := filepath.Join(dir, diagramFileName(t.Name(), nthRun(t.Name())))
	if err := os.WriteFile(path, []byte(diagram), 0o644); err != nil {
		t.Errorf("writing the diagram: %v", err)
		return
	}
	t.Logf("wrote %s", path)
}

// diagramTitle turns a test name into a readable one. Mermaid clips a title that
// is wider than the drawing rather than wrapping it, and these subtests are named
// in whole sentences, so the parent test's name is dropped — the file name still
// carries it — and the underscores become spaces.
func diagramTitle(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return strings.ReplaceAll(name, "_", " ")
}

// runCounts numbers the runs within one test. A test may run more than one
// specification against the same peer — the rejected-task cases follow up with a
// second, one-message specification asking whether the Worker is still polling —
// and each is its own conversation, so each gets its own diagram rather than
// overwriting the last.
var (
	runCountsMu sync.Mutex
	runCounts   = map[string]int{}
)

func nthRun(name string) int {
	runCountsMu.Lock()
	defer runCountsMu.Unlock()
	runCounts[name]++
	return runCounts[name]
}

// diagramFileName turns a test name into one path element; subtests put slashes
// and spaces in it.
func diagramFileName(name string, nth int) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
	for strings.Contains(safe, "--") {
		safe = strings.ReplaceAll(safe, "--", "-")
	}
	safe = strings.Trim(safe, "-")
	if nth > 1 {
		return fmt.Sprintf("%s.%d.mmd", safe, nth)
	}
	return safe + ".mmd"
}
