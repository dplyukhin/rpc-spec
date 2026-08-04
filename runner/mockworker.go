package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.temporal.io/rpcspec/spec"
)

// MockWorker drives a specification as the SDK side of the conversation against a
// real Server. Messages the SDK produces are generated; messages the Server
// produces are checked. This is how a Server author learns that a new response
// violates the contract.
type MockWorker struct {
	Conn *grpc.ClientConn
	// CallTimeout bounds each RPC. Long polls need this to be generous.
	CallTimeout time.Duration
	// Log, if set, receives a line per RPC.
	Log func(format string, args ...any)

	pending []*clientCall
}

type clientCall struct {
	method string
	// respType is recorded up front: the response message itself is being
	// unmarshalled by gRPC until done closes, so it must not be inspected before
	// then.
	respType protoreflect.FullName
	done     chan struct{}
	resp     proto.Message
	err      error
}

// NewMockWorker wraps an established connection.
func NewMockWorker(conn *grpc.ClientConn) *MockWorker {
	return &MockWorker{Conn: conn, CallTimeout: 80 * time.Second, Log: func(string, ...any) {}}
}

// Mode makes the SDK the mocked producer: what the SDK sends is generated, what
// the Server sends is checked. This is the same specification code path as the
// mock Server, with the flag flipped.
func (w *MockWorker) Mode(dir spec.Direction) spec.Mode {
	if dir == spec.ToServer {
		return spec.Synth
	}
	return spec.Check
}

// Emit issues the generated request. gRPC pairs the request with its response by
// call identity, so the specification never has to match them up by inspecting
// fields; the response is picked up by the next Recv.
func (w *MockWorker) Emit(role spec.Role, m proto.Message) error {
	if role.Direction != spec.ToServer {
		return errors.New("the mock Worker checks Server messages; it never generates them")
	}
	resp, err := newResponse(role.Method)
	if err != nil {
		return err
	}
	c := &clientCall{
		method:   role.Method,
		respType: resp.ProtoReflect().Descriptor().FullName(),
		done:     make(chan struct{}),
		resp:     resp,
	}
	w.pending = append(w.pending, c)
	w.Log("     -> %s", role.Method)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), w.CallTimeout)
		defer cancel()
		c.err = w.Conn.Invoke(ctx, fullMethod(role.Method), m, c.resp)
		close(c.done)
	}()
	return nil
}

func (w *MockWorker) Recv(dir spec.Direction, want protoreflect.FullName) (proto.Message, error) {
	c, i, err := w.await(dir, want)
	if err != nil {
		return nil, err
	}
	w.pending = append(w.pending[:i:i], w.pending[i+1:]...)
	return c.resp, nil
}

func (w *MockWorker) Peek(dir spec.Direction, want protoreflect.FullName) (proto.Message, error) {
	c, _, err := w.await(dir, want)
	if err != nil {
		return nil, err
	}
	return c.resp, nil
}

// await waits for the oldest outstanding call whose response has the wanted type.
// gRPC already pairs a request with its response by call identity, so this never
// has to match them up by inspecting fields.
func (w *MockWorker) await(dir spec.Direction, want protoreflect.FullName) (*clientCall, int, error) {
	if dir != spec.ToSDK {
		return nil, 0, errors.New("the mock Worker generates SDK messages; it never observes them")
	}
	for i, c := range w.pending {
		if c.respType != want {
			continue
		}
		<-c.done
		if c.err != nil {
			// A gRPC error is a Server response this prototype does not model. Failing
			// loudly is better than silently treating it as an empty message.
			return nil, 0, fmt.Errorf("%s returned an error rather than a response: %w", c.method, c.err)
		}
		return c, i, nil
	}
	return nil, 0, fmt.Errorf("the specification expected a %s with no matching request outstanding", want.Name())
}
