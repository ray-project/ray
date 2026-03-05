# C++ Ray Backend Bugs Discovered During Rust Conversion

During the conversion of Ray's C++ backend to Rust, we analyzed 194 recent
GitHub issues (Nov 2025 - Feb 2026) for Ray Core correctness and stability
bugs. We identified **5 confirmed bugs** in the C++ backend and documented
**6 undocumented behavioral quirks** that had to be reverse-engineered for
compatibility. Reproduction scripts were written for all 5 bugs.

## Bug 1: Actor Tasks Hang on Head-of-Line Dependency Failure (Critical)

**GitHub Issue:** [#60910](https://github.com/ray-project/ray/issues/60910)
**Severity:** Critical (silent deadlock)
**Status:** Confirmed by code analysis

**Description:** When an actor processes tasks sequentially
(`max_concurrency=1`), if the head-of-line task's dependency resolution fails
(e.g., the task producing the dependency is cancelled or its worker dies), all
subsequent tasks in the actor's queue hang permanently.

**Root Cause:** In `ScheduleRequests()`, when a task's dependency fails,
`MarkDependencyFailed` is called but it does **not** call `SendPendingTasks`.
Meanwhile, subsequent tasks whose dependencies resolved successfully already
called `SendPendingTasks` (which returned nothing because the head-of-line
task was still unresolved). After the head-of-line task is removed via
`MarkDependencyFailed`, no mechanism re-triggers `SendPendingTasks` for the
remaining tasks, leaving them permanently stuck.

**Impact:** Silent deadlock in production. Actor appears alive but stops
processing tasks. No error messages or timeouts are generated.

**Fix:** `MarkDependencyFailed` should call `SendPendingTasks` after removing
the failed task from the queue, so subsequent ready tasks can proceed.

---

## Bug 2: ReferenceCounter Double-Cleanup Crash (Critical)

**GitHub Issue:** [#60494](https://github.com/ray-project/ray/issues/60494)
**Severity:** Critical (process crash, P0)
**Status:** Confirmed by code analysis

**Description:** A race condition in `ReferenceCounter::CleanupBorrowersOnRefRemoved`
causes a `RAY_CHECK` failure (assertion crash) when both `message_published_callback`
and `publisher_failed_callback` fire for the same borrower.

**Root Cause:** When a borrower publishes a ref-removed message but dies before
the subscriber's IO service processes the `message_published_callback`, both
callbacks get queued:

1. `message_published_callback` fires (processes the ref-removed message)
2. `publisher_failed_callback` fires (borrower detected dead)

Both call `CleanupBorrowersOnRefRemoved` for the same `(object_id, borrower_addr)`.
The second call hits:

```cpp
RAY_CHECK(it->second.mutable_borrow()->borrowers.erase(borrower_addr))
```

This fails because the borrower was already erased by the first callback.

**Impact:** Worker or raylet process crashes under high concurrency when
borrowers die while reference cleanup is in-flight.

**Fix:** Guard against double-cleanup by checking if the borrower exists before
attempting to erase, or use a "cleanup already done" flag.

---

## Bug 3: Wrong Error Message for num_returns Mismatch (Medium)

**GitHub Issue:** [#60251](https://github.com/ray-project/ray/issues/60251)
**Severity:** Medium (confusing error message)
**Status:** Bug reproduced

**Description:** When a `@ray.remote(num_returns=2)` task returns a single
scalar value instead of a tuple/list of 2, the user gets a confusing
`TypeError` instead of a helpful error message.

**Actual error:**
```
TypeError: object of type 'int' has no len()
```

**Expected error:**
```
ValueError: Task returned 1 object, but num_returns=2. Ensure your function returns a tuple/list of 2 values.
```

**Root Cause:** The worker tries to call `len()` on the return value to split
it into multiple return objects, without first checking if the value is iterable
or if its length matches `num_returns`.

**Impact:** Developers waste time debugging a confusing `TypeError` that gives
no indication of the actual problem (mismatched return count).

---

## Bug 4: Unbounded Memory Growth from Small Objects (High)

**GitHub Issue:** [#59914](https://github.com/ray-project/ray/issues/59914)
**Severity:** High (unbounded memory growth)
**Status:** Bug reproduced (~30 MB growth over 10K calls)

**Description:** The in-process memory store accumulates small objects
(< 100 KB) without eviction, causing the owner process's memory to grow
unboundedly when repeatedly calling `ray.get()` on actor task results.

**Root Cause:** Objects smaller than 100 KB are stored inline in the
in-process memory store (heap) rather than in Plasma. This store has **no
eviction mechanism** -- objects are only deleted when their reference count
drops to zero AND Python's garbage collector runs. In practice, internal
reference counting keeps objects alive longer than expected, especially for
actor task results where the reference chain includes the task spec, lineage,
and borrower tracking.

**Reproduction:** ~30 MB growth after 10,000 `ray.get()` calls on small
(~2 KB) actor results, despite explicitly deleting all Python references and
running `gc.collect()`.

**Impact:** Long-running services (web servers, streaming pipelines) using
actors experience steady memory growth, eventually leading to OOM kills.

---

## Bug 5: Inconsistent Context Manager Behavior (Low)

**GitHub Issue:** [#59582](https://github.com/ray-project/ray/issues/59582)
**Severity:** Low (inconsistent API behavior)
**Status:** Confirmed (possibly by design)

**Description:** `ray.init()` behaves differently as a context manager
depending on the connection mode:

- **Local mode** (`ray.init()`): `__exit__` shuts down Ray entirely. The
  context is NOT reenterable.
- **Client mode** (`ray.init("ray://...")`): `__exit__` only disconnects the
  client. The context IS reenterable via `_context_to_restore` swapping.

**Root Cause:** Local mode owns the cluster lifecycle, so `__exit__` calls
`ray.shutdown()`. Client mode only manages a connection, so `__exit__`
disconnects without shutting down the remote cluster. The API does not
document this difference.

**Impact:** Code that works with Ray Client fails when switched to local mode,
or vice versa. The behavior is arguably by design (ownership semantics differ),
but the inconsistency is undocumented and surprising.

---

## Undocumented C++ Behaviors Discovered During Conversion

During the Rust conversion (Phase 10, commit `cd30e8a3a7`), we discovered 6
undocumented behavioral quirks in the C++ backend that had to be
reverse-engineered for wire compatibility:

| Behavior | Discovery |
|----------|-----------|
| **Binary cluster IDs** | Cluster IDs are raw 28-byte binary, not hex strings. The C++ code passes these as raw bytes in protobuf messages. |
| **Base64 config encoding** | The `--config-list` CLI flag passes serialized config as base64, not JSON or plaintext. This is undocumented in the CLI help. |
| **PubSub publisher_id** | The PubSub system requires a random 28-byte `publisher_id` (NodeID format) that is not documented in the protobuf definitions. |
| **CheckAlive response counts** | `CheckAlive` RPC must return a `ray_version` string and response counts matching the number of input addresses, not a single boolean. |
| **KV NotFound status codes** | GCS KV store returns a specific `StatusCode::NotFound` (not `OK` with empty value) when a key doesn't exist. |
| **Binary KV values** | KV store values can be raw binary (pickle data), not just UTF-8 strings, despite the protobuf field being `string`. |

These are not bugs per se, but undocumented implementation details that any
alternative backend must match exactly for compatibility with the Python
frontend.

---

## How the Rust Backend Addresses These Issues

| C++ Bug | Rust Backend Status |
|---------|-------------------|
| #60910 Actor head-of-line hang | **Not applicable** -- Rust backend uses a different actor task dispatch model with per-method gRPC calls. |
| #60494 ReferenceCounter crash | **Not applicable** -- Rust uses `Arc`/`Mutex` with Rust's ownership model, eliminating the double-cleanup race condition by construction. |
| #60251 Wrong error message | **Fixed** -- Rust backend returns `RayTaskError` with the actual exception message, not a confusing `TypeError`. |
| #59914 Memory growth | **Mitigated** -- Rust's deterministic drop semantics and lack of GC dependency mean objects are freed promptly when references are dropped. |
| #59582 Context manager | **Not applicable** -- Rust backend only supports local mode via `ray.init()` / `ray.shutdown()`. |
