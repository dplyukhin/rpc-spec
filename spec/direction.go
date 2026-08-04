package spec

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"

	"go.temporal.io/api/workflowservice/v1"
)

// Direction is which way a message travels.
type Direction int

const (
	// ToServer is a message produced by the SDK and consumed by the Server.
	ToServer Direction = iota
	// ToSDK is a message produced by the Server and consumed by the SDK.
	ToSDK
)

func (d Direction) String() string {
	if d == ToServer {
		return "SDK->Server"
	}
	return "Server->SDK"
}

// Role describes where a message sits in the RPC conversation: which gRPC method
// it belongs to and which way it travels.
type Role struct {
	Method    string // gRPC method short name, e.g. "PollWorkflowTaskQueue"
	Direction Direction
}

var (
	rolesOnce sync.Once
	roles     map[protoreflect.FullName]Role
)

// roleOf derives a message's method and direction from the WorkflowService
// descriptor: a method's input type is produced by the SDK, its output type by
// the Server. Nothing is hand-maintained, so the mapping cannot drift from the
// API definition.
func roleOf(name protoreflect.FullName) (Role, error) {
	rolesOnce.Do(func() {
		roles = make(map[protoreflect.FullName]Role)
		svcs := workflowservice.File_temporal_api_workflowservice_v1_service_proto.Services()
		for i := 0; i < svcs.Len(); i++ {
			svc := svcs.Get(i)
			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				m := methods.Get(j)
				roles[m.Input().FullName()] = Role{Method: string(m.Name()), Direction: ToServer}
				roles[m.Output().FullName()] = Role{Method: string(m.Name()), Direction: ToSDK}
			}
		}
	})
	r, ok := roles[name]
	if !ok {
		return Role{}, fmt.Errorf("%s is not a request or response of any WorkflowService method", name)
	}
	return r, nil
}
