package test

import (
	"bytes"
	"log/slog"
	"testing"
)

// testLogger routes SDK logging into the test log, so a failing run shows what
// the Worker thought was happening alongside the specification's verdict.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(&testWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("sdk: %s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}
