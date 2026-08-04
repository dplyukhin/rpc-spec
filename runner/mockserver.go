package runner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.temporal.io/rpcspec/spec"
)

// MockServer serves WorkflowService on a local port and drives a specification
// as the Server side of the conversation. Messages the SDK produces are checked;
// messages the Server produces are generated. A real Worker needs no adapter:
// it is simply pointed at a different address.
type MockServer struct {
	// Modeled lists the methods the specification describes. Any other method is
	// answered permissively, because a contract should permit calls it does not
	// model rather than rejecting them.
	Modeled []string
	// RecvTimeout bounds how long the specification waits for the next SDK
	// message before giving up, so a stuck run fails with a diagnostic instead of
	// hanging.
	RecvTimeout time.Duration
	// EmptyPollHold is how long a poll response that found no work is held before
	// being returned. A real Server holds such a poll until the long-poll timeout;
	// answering instantly instead invites the Worker into a hot loop. Timing is
	// deliberately absent from the specification, so the transport supplies it.
	EmptyPollHold time.Duration
	// Log, if set, receives a line per RPC, including unmodeled ones.
	Log func(format string, args ...any)

	modeled map[string]bool
	srv     *grpc.Server
	addr    string

	inbox chan *serverCall

	mu sync.Mutex
	// held is what the SDK has sent that the specification has not asked for yet.
	// A Worker keeps several long polls outstanding at once, so messages do not
	// arrive in the order a sequential specification consumes them.
	held []*serverCall
	open map[string][]*serverCall

	stopped chan struct{}
}

type serverCall struct {
	method string
	req    proto.Message
	resp   chan proto.Message
}

// StartMockServer begins serving on 127.0.0.1 with an ephemeral port.
func StartMockServer(s *MockServer) (*MockServer, error) {
	if s.RecvTimeout == 0 {
		s.RecvTimeout = 20 * time.Second
	}
	if s.Log == nil {
		s.Log = func(string, ...any) {}
	}
	s.modeled = make(map[string]bool, len(s.Modeled))
	for _, m := range s.Modeled {
		if _, ok := serviceMethods()[m]; !ok {
			return nil, fmt.Errorf("modeled method %q is not part of %s", m, ServiceName)
		}
		s.modeled[m] = true
	}
	s.inbox = make(chan *serverCall)
	s.open = make(map[string][]*serverCall)
	s.stopped = make(chan struct{})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s.addr = l.Addr().String()
	s.srv = grpc.NewServer()
	s.srv.RegisterService(s.serviceDesc(), s)

	// The SDK's own health check uses the standard gRPC health service.
	hs := health.NewServer()
	hs.SetServingStatus(ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s.srv, hs)

	go func() {
		defer close(s.stopped)
		_ = s.srv.Serve(l)
	}()
	return s, nil
}

// Addr is the host:port a Worker should be pointed at.
func (s *MockServer) Addr() string { return s.addr }

// Stop shuts the server down and unblocks any outstanding call.
func (s *MockServer) Stop() {
	s.srv.Stop()
	<-s.stopped
}

// serviceDesc builds handlers for every WorkflowService method from the protobuf
// descriptors, so the mock covers the whole service without 125 hand-written
// stubs.
func (s *MockServer) serviceDesc() *grpc.ServiceDesc {
	sd := &grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*any)(nil),
	}
	for name, md := range serviceMethods() {
		if md.IsStreamingClient() || md.IsStreamingServer() {
			continue
		}
		method, input := name, md.Input().FullName()
		sd.Methods = append(sd.Methods, grpc.MethodDesc{
			MethodName: method,
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				req, err := newMessage(input)
				if err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
				if err := dec(req); err != nil {
					return nil, err
				}
				return s.dispatch(ctx, method, req)
			},
		})
	}
	return sd
}

func (s *MockServer) dispatch(ctx context.Context, method string, req proto.Message) (proto.Message, error) {
	if s.modeled[method] {
		return s.route(ctx, method, req)
	}

	// Unmodeled methods. GetSystemInfo and DescribeNamespace must succeed with an
	// empty response: the Go SDK fails to dial if GetSystemInfo errors, and fails
	// to start a Worker if DescribeNamespace errors with anything other than
	// Unimplemented. An empty response also leaves every capability false, which
	// is the state a mock wants.
	s.Log("     (unmodeled) %s", method)
	if strings.HasPrefix(method, "Poll") {
		// Behave like a long poll that never finds work, rather than returning
		// immediately and inviting a hot loop.
		<-ctx.Done()
		return nil, status.Error(codes.DeadlineExceeded, "mock: long poll timed out")
	}
	resp, err := newResponse(method)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *MockServer) route(ctx context.Context, method string, req proto.Message) (proto.Message, error) {
	c := &serverCall{method: method, req: req, resp: make(chan proto.Message, 1)}
	select {
	case s.inbox <- c:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-c.resp:
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// --- spec.Transport ---

// Mode makes the Server the mocked producer: what the SDK sends is checked, what
// the Server sends is generated.
func (s *MockServer) Mode(dir spec.Direction) spec.Mode {
	if dir == spec.ToServer {
		return spec.Check
	}
	return spec.Synth
}

func (s *MockServer) Recv(dir spec.Direction, want protoreflect.FullName) (proto.Message, error) {
	c, err := s.await(dir, want, true)
	if err != nil {
		return nil, err
	}
	// Register the call so the response the specification generates next for this
	// method reaches the right RPC. Correlation comes from the transport, not the
	// payload.
	s.mu.Lock()
	s.open[c.method] = append(s.open[c.method], c)
	s.mu.Unlock()
	return c.req, nil
}

func (s *MockServer) Peek(dir spec.Direction, want protoreflect.FullName) (proto.Message, error) {
	c, err := s.await(dir, want, false)
	if err != nil {
		return nil, err
	}
	return c.req, nil
}

// await returns the first observed message of the wanted type, holding back any
// others until a later Send asks for them.
func (s *MockServer) await(dir spec.Direction, want protoreflect.FullName, consume bool) (*serverCall, error) {
	if dir != spec.ToServer {
		return nil, errors.New("the mock Server generates Server messages; it never observes them")
	}
	deadline := time.After(s.RecvTimeout)
	for {
		if c := s.findHeld(want, consume); c != nil {
			return c, nil
		}
		select {
		case c := <-s.inbox:
			s.mu.Lock()
			s.held = append(s.held, c)
			s.mu.Unlock()
		case <-deadline:
			return nil, fmt.Errorf("waited %s for the SDK to send a %s and none arrived%s",
				s.RecvTimeout, want.Name(), s.describeHeld())
		case <-s.stopped:
			return nil, errors.New("the mock Server was stopped")
		}
	}
}

func (s *MockServer) findHeld(want protoreflect.FullName, consume bool) *serverCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, c := range s.held {
		if c.req.ProtoReflect().Descriptor().FullName() != want {
			continue
		}
		if consume {
			s.held = append(s.held[:i:i], s.held[i+1:]...)
		}
		return c
	}
	return nil
}

// describeHeld reports what did arrive, which is the useful half of a timeout: a
// specification that expected the wrong reaction sees the reaction it got.
func (s *MockServer) describeHeld() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.held) == 0 {
		return "; nothing else arrived either"
	}
	names := make([]string, 0, len(s.held))
	for _, c := range s.held {
		names = append(names, string(c.req.ProtoReflect().Descriptor().FullName().Name()))
	}
	return "; meanwhile these arrived and the specification did not ask for them: " +
		strings.Join(names, ", ")
}

func (s *MockServer) Emit(role spec.Role, m proto.Message) error {
	if role.Direction != spec.ToSDK {
		return errors.New("the mock Server checks SDK messages; it never generates them")
	}
	s.mu.Lock()
	q := s.open[role.Method]
	if len(q) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("the specification generated a %s response with no %s call outstanding",
			role.Method, role.Method)
	}
	c := q[0]
	s.open[role.Method] = q[1:]
	s.mu.Unlock()

	// A wholly empty poll response means the poll found no work. Hold it open so
	// the Worker behaves as it would against a real Server; the specification is
	// free to move on to the next message meanwhile.
	if s.EmptyPollHold > 0 && strings.HasPrefix(role.Method, "Poll") && proto.Size(m) == 0 {
		s.Log("     holding an empty %s response open for %s", role.Method, s.EmptyPollHold)
		go func() {
			select {
			case <-time.After(s.EmptyPollHold):
			case <-s.stopped:
			}
			c.resp <- m
		}()
		return nil
	}

	c.resp <- m
	return nil
}
