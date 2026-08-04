// Package runner drives a specification against a real peer. It contains the
// two transports that make checker mode and synthesizer mode the same mode: a
// mock Server that a real Worker connects to, and a mock Worker that connects to
// a real Server.
package runner

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.temporal.io/api/workflowservice/v1"
)

// ServiceName is the gRPC service both transports speak.
const ServiceName = "temporal.api.workflowservice.v1.WorkflowService"

func fullMethod(method string) string { return "/" + ServiceName + "/" + method }

var (
	methodsOnce   sync.Once
	methodsByName map[string]protoreflect.MethodDescriptor
)

// serviceMethods returns every WorkflowService method, keyed by short name. Both
// transports are generic over the whole service rather than hand-written per
// method, which is what makes "permit calls the contract does not model" cheap.
func serviceMethods() map[string]protoreflect.MethodDescriptor {
	methodsOnce.Do(func() {
		methodsByName = make(map[string]protoreflect.MethodDescriptor)
		svcs := workflowservice.File_temporal_api_workflowservice_v1_service_proto.Services()
		for i := 0; i < svcs.Len(); i++ {
			ms := svcs.Get(i).Methods()
			for j := 0; j < ms.Len(); j++ {
				m := ms.Get(j)
				methodsByName[string(m.Name())] = m
			}
		}
	})
	return methodsByName
}

func newMessage(name protoreflect.FullName) (proto.Message, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return nil, fmt.Errorf("no generated type registered for %s: %w", name, err)
	}
	return mt.New().Interface(), nil
}

// newResponse builds an empty message of a method's output type.
func newResponse(method string) (proto.Message, error) {
	md, ok := serviceMethods()[method]
	if !ok {
		return nil, fmt.Errorf("unknown WorkflowService method %q", method)
	}
	return newMessage(md.Output().FullName())
}
