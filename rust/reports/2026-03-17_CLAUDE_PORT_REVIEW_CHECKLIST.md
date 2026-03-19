# Reviewer Checklist: What Claude Code Appears To Have Gotten Wrong In The Port

This checklist is derived from the full backend audit and is intended as a
human review aid. It is not a blame document. It is a compact list of the main
porting failure modes that appear repeatedly across the Rust backend.

Reference reports:

- Full audit:
  [`2026-03-17_BACKEND_PORT_REVIEW.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW.md)
- One-page human summary:
  [`2026-03-17_BACKEND_PORT_REVIEW_EXEC_SUMMARY.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW_EXEC_SUMMARY.md)

## 1. Replaced distributed behavior with local in-memory behavior

This is one of the most common failure modes in the port.

Typical pattern:

- C++ implementation coordinates state through GCS, owners, pubsub, or remote
  RPCs.
- Rust replacement keeps the same nouns but collapses the behavior into a local
  map, queue, or cache.

Examples from the review:

- GCS internal KV uses in-memory storage instead of the configured GCS store.
- Object ownership/location tracking is reduced to local maps instead of the
  owner-driven distributed protocol.
- Core Worker ownership tracking is local rather than distributed.
- Object Manager location tracking and transfer decisions are reduced to local
  bookkeeping.

Review question:

- Did the port preserve the distributed protocol, or only preserve the data
  structure names?

## 2. Ported data structures without porting the state machine

Another repeated issue is that the Rust code often keeps a simplified queue or
map, but not the state transitions that made the C++ implementation correct.

Typical pattern:

- C++ has explicit active/inactive/pending/retrying/restarting/removed states.
- Rust keeps only the “happy path” state and loses edge-case transitions.

Examples:

- Placement groups skip important lifecycle states.
- Pull manager loses active/inactive/pullable bundle logic.
- Actor transport loses restart/backpressure/cancellation state handling.
- Task execution loses much of the actor ordering/cancellation protocol.

Review question:

- Was the full state machine ported, or just the steady-state success path?

## 3. Stubbed control-plane functionality instead of porting it

The audit found many places where the Rust implementation leaves behavior as a
stub, no-op, or reduced acknowledgment path.

Typical pattern:

- RPC exists.
- Handler compiles.
- Response is returned.
- Real behavior is missing or explicitly stubbed.

Examples:

- `ReportJobError`
- actor lineage reconstruction / out-of-scope handling
- worker info/update RPCs
- resource resizing
- object pinning paths
- cleanup RPCs
- GCS drain behavior
- Python `drain_nodes`

Review question:

- Does this RPC actually perform the work that the C++ path performs, or only
  preserve the wire shape?

## 4. Preserved API names but changed observable semantics

In several places the Rust port keeps a similar method name but changes inputs,
outputs, or behavior in user-visible ways.

Examples:

- Python `CoreWorker.get` uses raw binary IDs and different return shapes.
- `ObjectRef` loses async/future/callback semantics and ref-lifecycle behavior.
- `wait` behavior diverges from C++ semantics.
- worker lease and object transfer methods acknowledge requests without matching
  the original behavior.

Review question:

- If a caller uses the same API name, do they actually get the same contract?

## 5. Dropped failure-handling and recovery logic

A major portion of the original C++ backend is about failure, retry, restart,
owner death, node death, reconstruction, and cancellation. That logic is often
reduced in the Rust port.

Examples:

- missing-object timeout/failure behavior in object pull paths,
- weaker shutdown/drain behavior,
- weaker object recovery and lineage behavior,
- reduced actor restart/death handling,
- weaker worker/job cleanup after node death.

Review question:

- What happens when the owner dies, the node dies, the object is spilled, the
  task is retried, or the transfer partially completes?

## 6. Reimplemented the happy path but not idempotency/retry semantics

Ray depends heavily on idempotency and retry-safe behavior.

Examples:

- duplicate push requests are not handled like C++ resend behavior,
- duplicate object references inside bundles are not always canonicalized,
- cleanup/cancellation paths do not always restore correct state,
- pubsub and long-poll behavior diverge under repeated calls.

Review question:

- What happens if the same request arrives twice, arrives late, or races with
  completion?

## 7. Under-ported the Python/native boundary

The Python layer is not just a convenience wrapper. It is part of the backend
contract. The Rust port appears to under-scope that fact.

Typical pattern:

- A small compatibility shim is added.
- A few classes are exposed.
- But the real Cython `_raylet` contract is much broader.

Examples:

- `_raylet` coverage is much smaller,
- `CoreWorker` constructor/lifecycle is reduced,
- `ObjectRef` behavior is weaker,
- `GcsClient` surface is reduced,
- RDT remains hybrid rather than fully ported.

Review question:

- Was the goal to replace a module name, or to preserve the Python runtime
  contract that the rest of Ray expects?

## 8. Ported unit-testable helpers without porting subsystem integration

Some Rust components look reasonable in isolation, but they are not integrated
 into the real subsystem flow the way the C++ code is.

Examples:

- spill manager works as a standalone utility, but object-manager integration is
  weaker,
- RDT store is ported, but the overall RDT path still depends on Python helpers
  and fallback behavior,
- local object bookkeeping exists, but the distributed lifecycle around it does
  not match C++.

Review question:

- Does this component only work in unit tests, or does it preserve the real
  production control flow?

## 9. Optimized or simplified before establishing equivalence

The branch includes cases where data structures were changed for simplicity or
performance before the original semantics were fully preserved.

Examples:

- replacing richer manager logic with simpler `DashMap` / local map based flows,
- simplifying actor/task transport into generic queues,
- simplifying object transfer into local data-structure loops.

Review question:

- Did the port first establish semantic equivalence and then optimize, or did it
  simplify away required behavior?

## 10. Relied too much on “compiles + some tests pass”

The strongest overall lesson from the audit is that large backend ports can look
plausible while still being functionally wrong in important ways.

Observed pattern:

- Modules exist with familiar names.
- Types compile.
- Many local tests pass.
- But core distributed semantics are missing or reduced.

Reviewer takeaway:

- For this kind of port, do not treat compilation, API name reuse, or local
  unit tests as evidence of functional equivalence.
- The right standard is behavioral conformance under real distributed and
  failure scenarios.

## Fast Review Questions For Future Port Passes

Use these questions as a lightweight screen when reviewing future changes:

1. Does this change preserve the distributed protocol, or only local state?
2. Does it preserve the full state machine, or only the success path?
3. Are retries, duplicates, races, and shutdown handled?
4. Are owner death, node death, and recovery behavior preserved?
5. Is the Python-facing contract actually unchanged?
6. Is there a cross-language or end-to-end test proving equivalence?

## Final Takeaway

If this port had been reviewed with a stricter “protocol and failure semantics
first” checklist, many of the highest-risk issues likely would have been caught
earlier.

The main problem is not that the code is in Rust. The main problem is that too
many C++ subsystems were translated as simplified local implementations instead
of full behavioral replacements.
