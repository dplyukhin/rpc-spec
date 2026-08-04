package spec

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// A field path is a dotted sequence of protobuf field names, e.g.
// "task_queue.name" or "start_timer_command_attributes.timer_id". Paths are
// strings rather than typed accessors, which is one of the costs the proposal
// accepts; validatePath recovers some of the lost safety by checking a path
// against the message descriptor before it is used.

// validatePath reports whether path names a real field reachable from md.
func validatePath(md protoreflect.MessageDescriptor, path string) error {
	if path == "" {
		return nil // the whole message
	}
	cur := md
	parts := strings.Split(path, ".")
	for i, part := range parts {
		fd := cur.Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			return fmt.Errorf("%s has no field %q (in path %q)", cur.FullName(), part, path)
		}
		last := i == len(parts)-1
		if last {
			return nil
		}
		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			return fmt.Errorf("%s.%s is not a message, so %q cannot continue past it",
				cur.FullName(), part, path)
		}
		if fd.IsList() || fd.IsMap() {
			return fmt.Errorf("%s.%s is repeated; use Nested to descend into it (path %q)",
				cur.FullName(), part, path)
		}
		cur = fd.Message()
	}
	return nil
}

// resolved is a located field: the message that directly owns it plus its
// descriptor. reached is false when an intermediate message was absent, which
// means the leaf cannot be present either.
type resolved struct {
	parent  protoreflect.Message
	fd      protoreflect.FieldDescriptor
	reached bool
}

// resolve walks path from m. With create set, absent intermediate messages are
// materialized; otherwise an absent intermediate yields reached == false.
func resolve(m protoreflect.Message, path string, create bool) (resolved, error) {
	if err := validatePath(m.Descriptor(), path); err != nil {
		return resolved{}, err
	}
	parts := strings.Split(path, ".")
	cur := m
	for i, part := range parts {
		fd := cur.Descriptor().Fields().ByName(protoreflect.Name(part))
		if i == len(parts)-1 {
			return resolved{parent: cur, fd: fd, reached: true}, nil
		}
		if !create && !cur.Has(fd) {
			return resolved{parent: cur, fd: fd, reached: false}, nil
		}
		if create {
			cur = cur.Mutable(fd).Message()
		} else {
			cur = cur.Get(fd).Message()
		}
	}
	panic("unreachable")
}
