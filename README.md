# `rpcspec` — a prototype of the imperative RPC specification DSL

This is a working prototype of [`Proposal_Go.md`](../Proposal_Go.md), built to answer the question
[`Concept.md`](../Concept.md) leaves open: **can one executable artifact describe the SDK/Server
conversation well enough to be run in both directions, and does it stay readable when written as an
ordinary Go program?**

There is no code generation. A specification is a Go function; "generating a mock Server" means
running that function with an oracle that generates the Server-produced messages instead of checking
them.

## What it demonstrates

One specification — [`specs/polling/spec.go`](specs/polling/spec.go), 254 lines — drives all of the
following. Nothing in it mentions checking or generating.

| | Evidence |
| --- | --- |
| **Mock Server vs. a real Go SDK Worker** | An unmodified `go.temporal.io/sdk` Worker dials the runner, receives a Workflow Task the specification generated, and completes the Workflow. The only thing that changes about the Worker is the address it dials. |
| **Mock Worker vs. a real Temporal Server** | The same specification generates the SDK's side against `temporal server start-dev`, and the Server then reports the Workflow as `Completed`. |
| **A real compatibility rule** | The unknown-History-Event / `worker_may_ignore` rule from `Concept.md`, expressed as one branch in the specification and enforced in both directions. |
| **Values that do not exist yet** | An event whose `event_type` is `9999` — a number with no name in the current descriptor — written straight onto the wire and observed by a real Worker as `EventType=9999`. |
| **The checking is not vacuous** | A consumer bug and a producer bug are each detected, with a diagnostic naming the message, the field, the expected and observed values, and the line of specification that failed. |
| **The constraint sublanguage really is bidirectional** | Every clause in the closed set is solved into an empty message, marshalled, unmarshalled, and then checked against the result — `TestEveryConstraintSolvesThenChecks`. |

## Running it

```bash
cd rpcspec && go test ./... -v
```

```text
--- PASS: TestEveryConstraintSolvesThenChecks (0.00s)          # 18 subtests: solve, marshal, check
--- PASS: TestCounterSolvesThenChecks (0.00s)
--- PASS: TestUnknownEnumSurvivesTheWire (0.00s)
--- PASS: TestUnknownFieldSurvivesTheWire (0.00s)
--- PASS: TestFieldPathsAreValidatedAgainstDescriptors (0.00s)
ok  	go.temporal.io/rpcspec/spec	0.278s

--- PASS: TestMockServerAgainstRealGoSDK/trivial_workflow_completes
--- PASS: TestMockServerAgainstRealGoSDK/unknown_history_event_marked_ignorable_is_tolerated
--- PASS: TestMockServerAgainstRealGoSDK/unknown_history_event_not_marked_ignorable_fails_the_task
--- PASS: TestMockWorkerAgainstRealServer (1.72s)
--- PASS: TestConsumerBugIsDetected (3.01s)
--- PASS: TestProducerBugIsDetected (1.75s)
ok  	go.temporal.io/rpcspec/test	7.116s
```

It is clean under `go vet ./...` and `go test ./... -race`.

It builds and runs offline from the local module cache (`go.temporal.io/api v1.63.4`,
`go.temporal.io/sdk v1.47.0`). The two tests that need a real Server skip themselves with a clear
message if the `temporal` CLI is absent.

To point a Worker in any language at the mock by hand:

```bash
cd rpcspec && go run ./cmd/mockserver -unknown-event -ignorable=false
```

## The two directions, side by side

Mock Server mode. The Worker keeps two polls outstanding, one of them on its own sticky queue; the
specification recognizes the sticky one from `task_queue.kind` and answers it with nothing.

```text
     (unmodeled) GetSystemInfo
     (unmodeled) DescribeNamespace
    branch sticky-poll          true (from lookahead: next PollWorkflowTaskQueueRequest has task_queue.kind == Sticky(2))
#1  SDK->Server PollWorkflowTaskQueue            checked
     holding an empty PollWorkflowTaskQueue response open for 5s
#2  Server->SDK PollWorkflowTaskQueue            generated
    branch sticky-poll          false (from lookahead: ...)
#3  SDK->Server PollWorkflowTaskQueue            checked
    branch long-poll-timeout    false (from policy)
#4  Server->SDK PollWorkflowTaskQueue            generated
sdk: level=ERROR msg="unknown event type" ... EventID=2 EventType=9999
sdk: level=WARN  msg="Failed to process workflow task." ... Error="unknown history event"
#5  SDK->Server RespondWorkflowTaskFailed        checked
#6  Server->SDK RespondWorkflowTaskFailed        generated
```

Mock Worker mode, same specification, every `checked` and `generated` swapped:

```text
#1  SDK->Server StartWorkflowExecution           generated
#2  Server->SDK StartWorkflowExecution           checked
    branch sticky-poll          false (from policy)
#3  SDK->Server PollWorkflowTaskQueue            generated
    branch long-poll-timeout    false (from lookahead: next PollWorkflowTaskQueueResponse has empty "task_token")
#4  Server->SDK PollWorkflowTaskQueue            checked
#5  SDK->Server RespondWorkflowTaskCompleted     generated
#6  Server->SDK RespondWorkflowTaskCompleted     checked
the real Server reports rpcspec-TestMockWorkerAgainstRealServer as Completed
```

Note what happens to `spec.NonEmpty("task_token")` between the two runs. In mock Server mode it
*invents* a token, which the specification then stores in `s.C.OpenToken` and later requires the
Worker to echo. In mock Worker mode the identical clause *verifies* the real Server's token, which
lands in the same variable and is then used to build the response. One clause, both jobs.

## A diagnostic

```text
spec violation at test/negative_test.go:110 (test.TestProducerBugIsDetected.func3)
  message    #4 Server->SDK PollWorkflowTaskQueue.PollWorkflowTaskQueueResponse
  at         attempt
  constraint Eq("attempt", 2)
  problem    expected 2, observed 1
  observed   task_token: "..."
             workflow_execution: { workflow_id: "..." run_id: "..." }
             ...
  conversation so far
    #1  SDK->Server StartWorkflowExecution           generated
    #2  Server->SDK StartWorkflowExecution           checked
    #3  SDK->Server PollWorkflowTaskQueue            generated
```

Because the specification is ordinary Go, the failing clause has a file and line, and the whole run
is steppable in a debugger.

## Layout

```
spec/          the DSL: Session, Send, constraints, cursors, diagnostics   1444 lines
runner/        the two transports: mock Server and mock Worker              486 lines
specs/polling/ the specification                                            254 lines
cmd/mockserver standalone mock, for driving a Worker by hand
test/, spec/*_test.go   the evidence                                        615 lines
```

Three implementation choices are worth naming, because they are cheaper than the proposal assumes:

- **Direction is derived, not declared.** `spec/direction.go` walks the `WorkflowService` protobuf
  descriptor once: a method's input type is produced by the SDK, its output by the Server. So
  `Send[*wsv1.PollWorkflowTaskQueueRequest]` knows its own direction and there is no table to
  maintain.
- **The mock Server covers all 123 RPCs without 123 stubs.** `runner/mockserver.go` builds a
  `grpc.ServiceDesc` from the same descriptors, so every method gets a handler and the "modeled" set
  is data.
- **The mock Worker never names an RPC either.** It calls `ClientConn.Invoke` with a method name
  derived from the message type.

## Findings

### What the proposal gets right

**The imperative form works, and it reads like Go.** `if`, `for`, `switch`, early `return`, and
helper functions are exactly what they look like. `PollUntilTask` is a plain function that loops
until a poll yields a task; in a DSL that reified control flow it would be a named rule with a loop
node. `Config` is a plain struct mutated in place — no copying, no transactions, because nothing is
rolled back.

**Constraints constructed at the point of use are much lighter than bidirectional matcher objects.**
`spec.Eq("namespace", s.C.Namespace)` needs no closure over the configuration, because the
configuration is already in scope. This is the single biggest readability win over a graph DSL and it
holds up.

**Checker mode and synthesizer mode really are the same mode.** Both transports implement one
four-method interface and the specification is compiled once. The `NonEmpty("task_token")` example
above is the proof: the same clause invents a value in one deployment and verifies one in the other,
and the plain-Go code downstream of it does not change.

**Wire-level construction of not-yet-existing values is genuinely easy in Go.** `UnknownEnum` is a
thin wrapper over `protoreflect.EnumNumber`, `UnknownField` over `protowire` and `SetUnknown`, and both
survive a real marshal/unmarshal round trip (`spec/unknown_test.go`). The real Go SDK saw
`EventType=9999` and took the documented branch. This is the strongest part of the case for Go.

**Permissive handling of unmodeled RPCs is the right default and costs nothing.** `GetSystemInfo`,
`DescribeNamespace`, activity polls, and `ShutdownWorker` are all answered generically. Making the
modeled set an explicit list means a specification can start small without rejecting real traffic.

### What the proposal understates

**`Maybe` cannot be a bare choice.** The proposal writes `s.Maybe("long-poll-timeout")`. That works
in synthesizer mode, but in checker mode the branch has to be derivable from what was observed, and
by the time the branch is taken nothing has been observed yet. The prototype therefore requires a
lookahead:

```go
if s.Maybe("long-poll-timeout", spec.PeekEmpty[*wsv1.PollWorkflowTaskQueueResponse]("task_token"))
```

This is not a small syntactic wart. Every branch that is not gated on configuration or on an
already-received message needs a declarative peek, which is a second small language alongside the
constraint sublanguage. The proposal's own advice — discriminate branches by an observable field —
turns out to be a *requirement*, not a style preference.

**Constraints are evaluated eagerly, so a field determined by a nested part of the message is
awkward.** `started_event_id` depends on how many events the history carries, but all the constraint
arguments to a `Send` are evaluated before any of them run. The specification has to compute the
count up front:

```go
startedEventID := s.C.NextEventID + s.C.taskEventCount() - 1
```

That is honest about a real coupling, but it means the specification cannot always be written in the
order the message is laid out.

**Timing is not a v1 problem — it is a day-one problem.** The first version of this prototype hot
looped at ~1700 RPCs/second. A real Server holds a poll that finds no work open until the long-poll
timeout; returning it instantly changes which queue the Worker's pollers choose next, so the Worker
never issued the normal-queue poll the specification was waiting for. The fix
(`MockServer.EmptyPollHold`) belongs in the transport, not the specification, which is the right
separation — but a mock Server is not usable without it.

**A sequential specification needs type-directed delivery, and that costs diagnostic sharpness.** A
default Go Worker keeps several polls outstanding, so messages do not arrive in the order a
sequential specification consumes them. `Recv` therefore takes the wanted message type and holds
non-matching messages back — which is the proposal's own dispatch rule with the key omitted. The
price is that "the Worker did the wrong thing" surfaces as a timeout rather than an immediate
mismatch:

```text
at message #6: waited 3s for the SDK to send a RespondWorkflowTaskCompletedRequest and none arrived;
meanwhile these arrived and the specification did not ask for them:
PollWorkflowTaskQueueRequest, RespondWorkflowTaskFailedRequest
```

The information is all there, but it took 3 seconds instead of being instant, and the reader has to
infer the point. The keyed-process model the proposal describes would fix this; the prototype
deliberately does not build it.

**String field paths cost less than expected; generic call syntax costs about what was predicted.**
Paths are validated against descriptors on first use, so `"worker_may_ignor"` fails with
`temporal.api.history.v1.HistoryEvent has no field "worker_may_ignor"` rather than passing silently
— see `TestFieldPathsAreValidatedAgainstDescriptors`. That is a decent mitigation. The
`spec.Send[*wsv1.PollWorkflowTaskQueueRequest](s, ...)` form is as noisy as advertised, though Go's
partial type-argument inference at least means the configuration type never has to be written.

**The specification has to say more than you would expect to drive a mock.** A poll response the Go
SDK will accept needs `task_token`, `workflow_execution`, `workflow_type`, `attempt`,
`previous_started_event_id`, `started_event_id`, and a contiguous history whose first event is
`WORKFLOW_EXECUTION_STARTED` with a non-empty `task_queue.name`. Anything the specification leaves
unconstrained is generated as a zero value, so synthesizer mode is a good forcing function for
completeness — but it means the contract ends up pinning down fields it does not really care about.

### Scoped out, deliberately

Per the proposal's own proof-of-concept scope: no keyed spec processes or cooperative scheduler, no
backtracking or choice-sequence replay (so no minimization and no schedule exploration — synthesizer
choices come from a fixed `Policy`), no proxy-check deployment against two real peers, no modelling
of gRPC errors as protocol messages, and no time model beyond the transport-level poll hold. The
specification covers a Workflow that completes in its first task; timers, Activities, queries,
updates, and pagination each add state.

### Verdict

The design holds. One 254-line specification really did drive a real Go SDK Worker and a real
Temporal Server from opposite ends, express a live compatibility rule, and reject both a consumer and
a producer bug — with about 1900 lines of DSL and runner behind it. The imperative form stayed readable at
this size.

Two things would decide whether it stays readable at ten times the size, and neither is answered
here: whether the peek-based branching stays tolerable once version and capability gating are dense,
and whether the keyed-process model can be added without the diagnostics or the goroutine teardown
becoming a problem. Those are the questions a second iteration should target.
