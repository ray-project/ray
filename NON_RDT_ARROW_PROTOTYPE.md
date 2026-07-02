# Non-RDT Arrow Prototype — working doc

> Living doc for the heap→heap Arrow data-transfer prototype.
> Branch: `karticam/arrow-prototype`. Worktree: `~/ray-worktrees/arrow-prototype`.
> (Renamed from `non-rdt-arrow-prototype` on 2026-06-11; old worktree removed.)
> Keep this updated as we go — it is our shared memory of *what we're doing, why, and what we learned*.

---

## 0. START HERE — session resume (updated 2026-06-16)
> New session: read this whole file + [`BACKGROUND.md`](./BACKGROUND.md) (the Arrow/Flight/`process_vm` primer).
> This section is the **single source of truth for where we are and what's next.** Auto-memory pointer:
> `project_arrow_flight_ray_data.md`.

**What this is.** A prototype moving `pa.Table`s producer→consumer **bypassing plasma** (heap→heap via Arrow),
for **Ray Data single-consumer pipelines**. PR #62772 is **applied in this worktree** (commit `dc2ae38801`), so
two impls are live: **native** (`RAY_USE_FLIGHT_NATIVE=1`, the `FLIGHT_TABLE` intercept — the "non-rdt" focus) and
**RDT** (`@ray.method(tensor_transport="ARROW_FLIGHT")`). Joint study with `~/ray-worktrees/plasma-move-semantics`:
compare the **2×2 {plasma | bypass-plasma} × {copy | move}** (§1). native = bypass+**move**, RDT = bypass+**copy**.

**Where we are (2026-06-16):**
- **Understanding — DONE:** Arrow IPC / Flight / `process_vm` primer (`BACKGROUND.md`); native path end-to-end
  incl. consumer deep-trace (§3.3, §3.9); RDT end-to-end trace (§3.8) + how the plugin slots in (§3.8.2);
  copy-count model + large-table same-node-gap diagnosis (§3.10).
- **Understanding — PAUSED (resume point):** RDT framework internals not yet deep-dived → re-read §3.8 Phase 1
  *"Gory detail"*, then the `serialize_rdt_objects` ⇄ deserialize **index-swap recombination** + `RDTStore` mechanics.
- **Execution — full 2×2×3 grid run (§5.3, 2026-06-17):** {same/cross-node} × {read-only/modify} ×
  {ray/native/rdt} × 1/10/100 MB, serial. **plasma wins 10/12; native wins only same-node 1 MB; RDT slowest.**
  Predicted arrow wins (modify, cross-node) **don't show** — arrow carries a ~50 ms reader-side allocation deficit
  at 100 MB (Ed's caching point, §3.10) ⇒ **the buffer pool is the gating fix.** Still serial / latency-bound;
  concurrency / peak-mem / plasma+move not yet exercised; modify-cell meaning is an open question (§4).
  (§5.2 = the earlier same-node-only run.)

**Next steps (pick up here):**
1. **Reader-side buffer-reuse pool in `fetch_via_vm` (THE gating fix, §5.3):** add it, then re-run **modify +
   cross-node** — the decisive test of whether arrow's predicted wins are real once its per-fetch
   `pa.allocate_buffer` deficit (§3.10 / Ed) is removed. (`flight_core.py:168`; mechanism `(verify)` with Ed.)
2. **Then the rest of the benchmark plan:** concurrency/throughput sweep (`--num-actor-pairs 4 --concurrency 4`,
   raise `--max-in-flight` — exposes pipelining where latency/throughput decouple; **use `--concurrency > 1`** so
   native can pipeline its fetch, §3.9 nuance 2); add a **peak-memory** metric; build the **plasma-move-semantics**
   image for the 4th 2×2 cell (`--mode ray` there); collate vs the shared `plasma+copy` baseline. (Plan + harness
   ref in §5; first full grid in §5.3.)
3. **RDT internals (when ready):** the paused resume point above.

**Run facts (don't relearn):** runs on **Anyscale / the Linux devbox ONLY — never this macOS box** (design/read
here). `/rebuild` Ray from THIS worktree on the target; sanity-check `from ray._raylet import vm_scatter_write`.
Two gotchas (both understood/fixed, see §5): set `kernel.yama.ptrace_scope=0` **cluster-wide** (same-node
`process_vm`); RDT **receiver** actors need `@ray.remote(enable_tensor_transport=True)` (already patched in the
`proto/*` Consumers).

**Map:** §1 goal + 2×2 matrix · §2 background (→`BACKGROUND.md`) · §3 architecture (3.3 native intercept, 3.8 RDT
trace, 3.8.2 plug-in, 3.9 native consumer deep-trace) · §4 open questions · §5 benchmark plan/ref/results · §6 changelog.

---

## 1. Goal

Ray transfers objects between actors/tasks through the **plasma object store** by default:
the producer serializes its result into plasma (shared memory), and a consumer on another
node **pulls** it via the object manager (push/pull RPC, chunked). We want to **bypass plasma**:
keep the produced data **live in the producer's process heap**, and move it **directly heap→heap**
using **Apache Arrow** as the data format.

Concrete scenario: a **producer actor** makes a `pyarrow.Table`; a **consumer actor** (possibly on
another node) needs it. Instead of plasma, the consumer pulls the table straight out of the
producer's heap.

**Why Arrow?** Arrow's in-memory columnar layout is identical in memory, on the wire, and on disk,
so moving it can be (near) zero-copy — no row-by-row (de)serialization.

**End goal of the prototype:** for **Ray Data** — a streaming **single-consumer** producer→consumer
pipeline (each downstream consumer uses the upstream producer's objects; **no fan-out / multi-consumer**).
This context matters: it makes the native path's *delete-on-first-read* eviction acceptable (no 2nd
consumer). Direction: benchmark vs plasma + a design proof toward a Ray Data data-plane.

**Broader evaluation — joint with `~/ray-worktrees/plasma-move-semantics`.** This worktree and the
plasma-move-semantics worktree are two halves of one investigation. **Move semantics** (per
`PLASMA_MOVE_SEMANTICS.md:16`) = *the producer frees its primary copy **eagerly** once the consumer has
the data* (to cut peak object-store memory), vs keeping it until the owner's ref-count-driven eviction.
⚠️ "move" = **reclamation timing**, NOT copy-avoidance — the producer→consumer transfer is **single-copy
in every cell**. Two axes → the full 2×2, and **all four cells already have an implementation:**

|                                 | **copy** (producer keeps primary until owner ref-count free; fan-out OK) | **move** (producer frees primary eagerly once consumer has it; single-read) |
|---------------------------------|---|---|
| **plasma**                      | baseline = today's Ray                                | `plasma-move-semantics` worktree (free-on-push-ack)            |
| **bypass-plasma (heap, Arrow)** | **RDT** (`ARROW_FLIGHT`) — owner ref-count `__ray_free__` | **native flight** — consumer delete-on-first-read              |

Key correction: **native flight is already bypass + *move*** (consumer-triggered delete = eager producer
reclamation, same idea as plasma-move's free-on-push-ack). **RDT Arrow is bypass + *copy*** (keeps the
producer copy until ref-count; supports fan-out). Questions: does **move** help (peak memory / throughput)?
does **bypassing plasma** help (latency / copies / mutable buffers)? is **bypass + move** best? **Plan:**
once the RDT flow is fully understood, design **microbenchmarks** across the four (already-implemented)
cells. Seed: `proto/bench_flight_store.py` (its `--consumer-mode modify` probes the mutable-buffer benefit).

---

## 2. Background primer (the tech this prototype relies on)

> 📖 **Full tutorial-style explanation lives in [`BACKGROUND.md`](./BACKGROUND.md)** — read that for the
> ground-up walkthrough (incl. `buf`/`open_stream` Q&A and an annotated `_RecordingSink`). The below
> is the dense quick-reference.

### 2.1 Apache Arrow in-memory format (the foundation)
- Arrow stores a table **column-major** as a set of **buffers** (raw byte arrays): e.g. a validity
  bitmap buffer (which values are null) + a values buffer per column; variable-width types
  (strings/lists) add an offsets buffer.
- The **in-memory layout is the canonical layout.** There is no separate "parsed" vs "serialized"
  form for the data values — the bytes you compute on are the bytes you ship.
- A **RecordBatch** = a schema + a list of these buffers for one chunk of rows. A **Table** = a
  schema + one or more RecordBatches ("chunks") per column.

### 2.2 Arrow IPC (Inter-Process Communication format) — the "serialization"
- Arrow IPC is the standard way to lay out Arrow data as a byte stream for moving between processes
  / machines / files. Two variants: **stream** (schema message, then record-batch messages) and
  **file/Feather** (stream + footer index for random access). The PR uses the **stream** form
  (`pyarrow.ipc.new_stream` / `open_stream`).
- An IPC record-batch message is two parts:
  1. a small **metadata header** (FlatBuffers): schema, and for each buffer its *offset + length*
     and null counts — i.e. a map of "where each column buffer lives in the body".
  2. the **body**: the raw column buffers, concatenated, 8/64-byte aligned.
- Because the body buffers are *already in Arrow's target layout*, "serializing" is mostly:
  write the metadata header, then write the buffers as-is. **Reading** (`open_stream(buf).read_all()`)
  parses the tiny header and then builds arrays that **point into `buf`** — zero-copy; it does not
  copy the column values.
- **This is the trick the PR exploits** (see `_RecordingSink`, §3.1): you can know exactly what bytes
  the IPC stream *would* contain, and where those bytes already live in the producer's heap, without
  ever materializing the stream. Then you reproduce the stream byte-for-byte in the consumer's buffer
  by copying those memory regions in order.

### 2.3 Arrow Flight — Arrow's RPC layer (the "RPC")
- **Flight = a gRPC-based client/server framework for moving Arrow data.** Control plane is
  gRPC + Protobuf; the data plane ships record batches in their **IPC encoding** (no row-by-row
  serialization), which is what makes it fast.
- Core RPC verbs (only some are used here):
  - `DoGet(ticket) -> stream<RecordBatch>`: client presents an opaque **ticket** (bytes identifying
    what it wants); server streams Arrow data back. **← used for the cross-node fetch.**
  - `DoAction(action) -> stream<result>`: generic "do a custom command" RPC; `action` = a type
    string + arbitrary body bytes. **← used as a control/doorbell channel** (`"scatter_write_vm"`,
    `"delete"`).
  - `DoPut`, `GetFlightInfo`, `ListFlights`, `DoExchange`: not central here.
- In this PR, **one Flight server runs per worker process** (`FlightCore`), and it is used **two ways**:
  1. **As real data transfer** (cross-node): `do_get` returns `RecordBatchStream(table)`, gRPC streams
     it to the consumer.
  2. **As a doorbell only** (same-node): the consumer calls `do_action("scatter_write_vm", ...)` just
     to *ask* the producer to copy its buffers into the consumer's memory via a kernel syscall — the
     actual table bytes do **not** travel through Flight/gRPC in this case.

### 2.4 `process_vm_readv` / `process_vm_writev` — direct cross-process memory copy
- Linux syscalls (kernel ≥ 3.2). They copy bytes **directly between the virtual address spaces of two
  processes on the same machine**, in a single syscall — no pipe, no socket, no shared-memory file,
  no serialization. The kernel does the page copy.
- Signatures (both identical shape):
  ```c
  ssize_t process_vm_readv (pid_t pid,
                            const struct iovec *local_iov,  unsigned long liovcnt,
                            const struct iovec *remote_iov, unsigned long riovcnt,
                            unsigned long flags);   // flags = 0
  ssize_t process_vm_writev(pid_t pid, ... same ...);
  ```
  - `struct iovec { void *iov_base; size_t iov_len; }` — just a (pointer, length) pair.
  - `local_iov[]` describes buffers in **the calling process**; `remote_iov[]` describes buffers in
    process **`pid`**. The "v" = vector = **scatter/gather**: you can name many discontiguous regions
    in one call. Sum of local lengths must equal sum of remote lengths.
  - `readv`: copies remote → local. `writev`: copies local → remote.
- **The scatter-write used here** (`ScatterWriteToRemoteProcess` → `process_vm_writev`):
  - **Caller = the producer** (inside its Flight `do_action` handler).
  - `local_iov[]` = the **scatter-list** = one entry per live column buffer (+ small metadata chunks)
    in the producer's heap. `remote_iov[]` = **one** entry = the consumer's single pre-allocated buffer.
  - One syscall lays all the producer's scattered buffers down **contiguously** into the consumer's
    buffer — which is exactly a valid Arrow IPC stream the consumer can `open_stream(...).read_all()`.
  - Net cost: **one kernel memcpy**, producer heap → consumer heap. No plasma create+copy, no gRPC
    data plane.
- **Why it beats plasma (same node):** plasma must serialize/copy the object into shared memory,
  seal it, and have the consumer map/copy it out; this is a single direct copy into a buffer the
  consumer already owns (and which is **mutable**, so the consumer can mutate in place zero-copy —
  plasma shared memory is immutable, forcing a copy on mutate).

#### ⚠️ Hard constraints of the syscall path (important for our dev loop)
- **Linux only.** macOS/Windows have no `process_vm_*` → the C++ shim returns `ENOSYS`. **Our dev box
  is macOS (darwin), so the same-node VM fast path will NOT run locally** — only plasma and the
  cross-node Flight gRPC path work on a Mac. Need a Linux box / cluster to exercise the fast path.
- **Permissions (ptrace / Yama):** writing another process's memory needs ptrace capability over it,
  gated by `/proc/sys/kernel/yama/ptrace_scope`: `0`=any same-uid process (what the prototype forces),
  `1`=parent-only (common default), `2`=admin-only, `3`=disabled. The benchmark's `_enable_ptrace()`
  does `echo 0 | sudo tee .../ptrace_scope` (needs passwordless sudo). Also requires **same uid** and
  **same PID namespace** (matters inside containers).
- **Same node only** (it's process↔process on one host) — hence the cross-node Flight fallback.

---

## 3. What we found in PR #62772 (`edoakes`, "[WIP] proto", CLOSED)

Ed prototyped exactly this idea, **two ways**, on a shared engine, with a correctness harness and a
benchmark racing both against plasma. 18 files, +1661 lines. Diff saved at `/tmp/pr62772.diff`.
**As of 2026-06-13 the PR is APPLIED in this worktree** — commit `dc2ae38801 "Apply changes from #62772"`
on top of master — so all of the below is live, readable, and runnable here (real files, not just the diff).

### 3.1 Shared engine
- **`python/ray/_private/flight_core.py` → `FlightCore`** (per-process singleton):
  - Runs **one Arrow Flight gRPC server per worker process**; holds produced tables in
    `self._tables[key] = table` (data stays in heap).
  - **Per-process, NOT per-node** (module-global `_core`, lazy). Essential: the table is a Python
    object in the *producer worker's* heap — a node-level store can't reference objects in other
    workers' heaps, and the Flight server / `process_vm_writev` handler must run *in* the producer
    process to read its own buffers. So one node runs *many* Flight servers (one per worker). The
    transfer-info dict is therefore **process-granular**: `flight_uri` (reach the specific producer
    process's server) + `pid` (target for `process_vm_writev`) + `node_id` (same-node vs cross-node).
  - **`_RecordingSink`**: a fake IPC sink. When the table is "written" to it, instead of copying
    buffer bytes it **records `(address, size)` of each live Arrow buffer** and copies only small
    metadata chunks (kept alive in `_refs`). Produces the **scatter-list** + total IPC size with no
    byte-stream materialization. Built once per object at `put()` time and cached in `self._sinks`.
  - `fetch_via_vm(uri, key, size)` (same-node): consumer `pa.allocate_buffer(size)`, sends Flight
    `DoAction("scatter_write_vm", {key, consumer_pid, consumer_addr, size})`; producer's handler runs
    `vm_scatter_write(...)` → `process_vm_writev` into the consumer buffer; consumer
    `ipc.open_stream(local_buf).read_all()`.
  - `fetch_via_flight(uri, key)` (cross-node): plain `client.do_get(ticket).read_all()`.
  - `_handle_scatter_write` (producer-side `do_action` handler), `delete`/`send_delete_rpc` (eviction).
- **`src/ray/flight_store/vm_transfer.{cc,h}`** (C++): thin wrappers over `process_vm_readv`/`writev`
  — `ReadFromRemoteProcess`, `WriteToRemoteProcess`, `ScatterWriteToRemoteProcess`. `ENOSYS` off Linux.
- **`python/ray/includes/flight_store.{pxd,pxi}`** (Cython): expose the C++ funcs to Python —
  `vm_read`, `vm_read_into_arrow_buffer`, `vm_write`, `vm_scatter_write` (the last is the one used).

### 3.2 Path A — RDT backend (`python/ray/experimental/rdt/arrow_flight_transport.py`)
- Implements the existing `TensorTransportManager` interface; registered as `ARROW_FLIGHT`,
  devices `["cpu"]`, `data_type=pyarrow.Table` (via `util.py` change). **One-sided.**
- `extract_tensor_transport_metadata` (producer): `put`s the table into `FlightCore`, returns
  `{flight_uri, key, pid, node_id, ipc_size}` (stuffs `(num_rows, schema)` into the `tensor_meta` field).
- `recv_multiple_tensors` (consumer): same-node→`fetch_via_vm`, else `fetch_via_flight`.
- `garbage_collect`→`delete`. **Opt-in: `@ray.method(tensor_transport="ARROW_FLIGHT")`.**
- Rides on ALL the RDT plumbing (ref interception, out-of-band metadata through C++
  `ReturnObject.direct_transport_metadata`, RDT store, GC, system-task send/recv orchestration).
  Ed's noted cost: "the owner/system-task overhead of the RDT path."

### 3.3 Path B — native intercept (**the "non-RDT" path** ← our likely focus)
- **Opt-in: env var `RAY_USE_FLIGHT_NATIVE=1`.** Bypasses RDT entirely.
- **`python/ray/_raylet.pyx`** hook in `store_task_outputs`: when a task return
  `isinstance(output, pa.Table)` (and no `tensor_transport`), instead of plasma-serializing it:
  1. `put`s the table in the local `FlightCore`, keyed by the return object's hex id;
  2. builds a tiny msgpack dict `{flight_uri, key, pid, ipc_size, node_id}`;
  3. stores **only that dict** into the normal object store, tagged with new metadata type
     `OBJECT_METADATA_TYPE_FLIGHT_TABLE = b"FLIGHT_TABLE"` (`ray_constants.py`).
  - **Reuses the normal storage primitive:** step 3 calls the existing `CoreWorker.store_task_output`
    (singular) helper — *allocate return buffer (inline/plasma) → write serialized bytes → seal*, or
    *pin if it already exists in plasma*; returns a bool, hence the `while not ...: pass` retry. The
    native path invents no new storage: it just feeds this primitive the tiny coordinate dict instead
    of the serialized table. The big table stays in the producer heap.
  - **The coord dict always inlines → plasma fully bypassed.** `AllocateReturnObject`
    (`core_worker.cc:2686`) inlines a return iff `data_size < max_direct_call_object_size` (**100 KiB**)
    *and* the per-task total stays `≤ task_rpc_inlined_bytes_limit` (**10 MiB**) → allocates a
    `LocalMemoryBuffer` (in-proc heap), returned to the owner **inline in the task RPC reply**, never
    touching plasma. The ~few-hundred-byte coord dict is always under 100 KiB → always inlined. So the
    native path bypasses plasma end-to-end: big table heap→heap (Flight/`vm`), coords inline in the reply.
  - **Worker-side copy path (inline return):** `write_to` is copy #1 (serialized bytes → the
    `LocalMemoryBuffer`); then `SerializeReturnObject` (`common.cc:57`) does copy #2 via `set_data(...)`
    into the `PushTaskReply` proto → gRPC → caller deserializes into its in-mem store. (Plasma return:
    `write_to` writes into the plasma buffer, and `SerializeReturnObject` sees `IsPlasmaBuffer()` →
    sets `in_plasma=true` with **no data** in the reply; caller pulls from plasma.) **RDT coord channel
    is also here:** `common.cc:85-88` packs `ReturnObject.direct_transport_metadata` into the same reply
    (RDT path only; native ships its dict as the inlined `set_data` payload).
  - **Why 2 copies (not serialize straight into the reply proto)?** Deliberate uniformity tradeoff:
    one `write_to(RayObject buffer)` path serves *both* inline (`LocalMemoryBuffer`) and plasma
    (shared-mem) — `store_task_output` doesn't branch on destination; the single late decision is
    `IsPlasmaBuffer()` in `SerializeReturnObject`. Plasma fundamentally can't serialize into the reply
    (bytes go to shared memory), and the serializer runs at a different layer/time than reply-building.
    Crucially the 2nd copy only hits **inline = `<100 KiB`** objects (cheap); **large objects take the
    single-copy plasma path with `in_plasma=true` and NO reply-data copy** — so Ray already avoids the
    double copy exactly where it would be expensive.
- **`python/ray/_private/flight_object_store.py` → `FlightObjectStore`**: thin adapter over
  `FlightCore` exposing `put_and_get_transfer_info(key, table)` and `fetch(info)` (dispatches
  same-node-vm vs cross-node-flight, then sends a delete RPC).
  - **"Adapter" = mechanism in `FlightCore`, policy here.** It owns no transfer logic; it delegates
    to `_core` and only adds native-specific *policy*: the coordinate-**dict** format
    (`{flight_uri,key,pid,ipc_size,node_id}`), the same-vs-cross-node decision, and eviction
    (delete-after-pull). It is the **native counterpart to `ArrowFlightTransport`** (RDT) — both are
    parallel adapters over the *same* per-process `FlightCore` (via `get_flight_core()`).

```
                    ┌─────────────────────────┐
                    │   FlightCore  (engine)   │  put / fetch_via_vm / fetch_via_flight
                    │  per-process singleton   │  (the only place transfer logic lives)
                    └─────────────────────────┘
                       ▲ delegates           ▲ delegates
       ┌───────────────┴────────┐   ┌─────────┴───────────────────┐
       │  FlightObjectStore      │   │  ArrowFlightTransport        │
       │  (NATIVE adapter)       │   │  (RDT adapter)               │
       │  put_and_get_transfer_  │   │  extract_..._metadata /      │
       │  info / fetch(dict)     │   │  recv_multiple_tensors(meta) │
       └─────────────────────────┘   └──────────────────────────────┘
```
  - Per method — *delegates* vs *adds*: `put_and_get_transfer_info` delegates `ensure_server`+`put`,
    adds the dict packaging; `fetch` delegates `fetch_via_vm`/`fetch_via_flight`/`send_delete_rpc`,
    adds the dispatch decision + eviction; `ensure_server`/`get_uri`/`delete` are pass-throughs.
    Interface diff vs RDT: `fetch(dict)→table` here vs
    `recv_multiple_tensors(TensorTransportMetadata)→[table]` in RDT — same `fetch_via_*` underneath.
- **`python/ray/_private/serialization.py`**: `_deserialize_object` detects the `FLIGHT_TABLE` marker
  → `_fetch_flight_table(data)` → msgpack-decode → `get_flight_store().fetch(info)`.
- **Net effect:** the `ObjectRef` is a **normal Ray ref** whose plasma payload is just **coordinates**
  to the real data in the producer's heap. `ray.get`, actor-arg passing, chaining all work
  **transparently** because the swap happens at (de)serialize time. Lighter than RDT (no system
  tasks), at the cost of a custom metadata type + a direct core-worker hook.
- **How the consumer learns `pid`/`flight_uri`:** they *are* the ref's value. The tiny coordinate
  dict rides Ray's **normal object store / ref-resolution** path (only the big table bypasses plasma).
  Two-hop fetch: (1) normal plasma resolve of the small dict object, then (2) out-of-band Arrow fetch
  using those coordinates. Passing the ref *is* the channel — the producer never explicitly tells the
  consumer anything. (RDT path delivers the same dict via C++ `ReturnObject.direct_transport_metadata`
  through the owner instead.)

### 3.4 Harnesses (in the PR)
- **`test_arrow_flight.py`** — "hello world": a `DataServer` actor runs a Flight server, a `DataClient`
  actor pulls via `DoGet`. Pure Flight, no Ray-store integration.
- **`proto/verify_flight_store.py`** — correctness: ~10 diverse tables (empty, ints, 1/10/100MB floats,
  many-columns, strings, nulls, nested lists, dictionary) × 4 scenarios (driver `ray.get`,
  actor→actor, chained, `put`/`get`), checked via `pa.Table.equals`. `FLIGHT_MODE` ∈ {plasma, native, rdt}.
- **`proto/bench_flight_store.py`** — throughput/latency: streaming producer→consumer pairs, placement
  {same/cross/mixed-node}, consumer mode {read-only, in-place modify}, size sweep, modes {`ray`,
  `arrow-native`, `arrow-rdt`}. `_enable_ptrace()` sets `yama/ptrace_scope=0`.

### 3.5 Design tensions Ed is probing
1. **Same-node vs cross-node**: `process_vm_writev` (1 syscall, no serialize, no gRPC) vs Flight gRPC.
2. **Zero-materialization producer**: `_RecordingSink` never builds the IPC byte stream.
3. **RDT vs native = reuse-vs-overhead**: RDT gives lifecycle plumbing for free but adds per-object
   system-task overhead; native is leaner + transparent but pokes the core worker + adds a metadata type.
4. **Mutability bonus**: flight path hands the consumer a *mutable* buffer → in-place mutation is
   zero-copy; plasma shared memory is immutable → mutation copies.

### 3.6 File map (PR #62772)
| File | Role |
|---|---|
| `python/ray/_private/flight_core.py` | Shared engine: Flight server, table store, `_RecordingSink`, fetch paths |
| `python/ray/_private/flight_object_store.py` | Native-path adapter over `FlightCore` |
| `python/ray/_private/serialization.py` | Consumer-side `FLIGHT_TABLE` detect + fetch |
| `python/ray/_private/ray_constants.py` | `OBJECT_METADATA_TYPE_FLIGHT_TABLE` |
| `python/ray/_raylet.pyx` | `store_task_outputs` intercept (native) + Cython VM bindings include |
| `python/ray/includes/flight_store.{pxd,pxi}` | Cython ↔ C++ VM-syscall bindings |
| `src/ray/flight_store/vm_transfer.{cc,h}` + `BUILD.bazel` | C++ `process_vm_*` wrappers |
| `python/ray/experimental/rdt/arrow_flight_transport.py` | RDT backend (Path A) |
| `python/ray/experimental/rdt/util.py` | Register `ARROW_FLIGHT` transport |
| `proto/verify_flight_store.py`, `proto/bench_flight_store.py`, `test_arrow_flight.py` | Harnesses |

### 3.7 Reading guide — native vs RDT (which files for which path)
Three buckets, not two: a **shared engine** both paths sit on, plus path-specific hooks. Both fetch
data identically (`flight_core`); they differ only in the *hook* into Ray.

**Shared engine (read first, bottom-up):**
1. `src/ray/flight_store/vm_transfer.{h,cc}` — the `process_vm_*` syscalls (smallest; start here)
2. `python/ray/includes/flight_store.{pxi,pxd}` — Cython exposes them (`vm_scatter_write`)
3. `python/ray/_private/flight_core.py` — **the heart**: `_RecordingSink`, `put`, `fetch_via_vm`,
   `fetch_via_flight`, in-process server
4. `python/ray/_raylet.pyx` *(the `cimport` + `include "includes/flight_store.pxi"` part only)*

**NATIVE path (Path B)** = shared engine **+**:
- `python/ray/_private/ray_constants.py` — `OBJECT_METADATA_TYPE_FLIGHT_TABLE` marker
- `python/ray/_raylet.pyx` — `store_task_outputs` intercept = **producer side**
- `python/ray/_private/flight_object_store.py` — `put_and_get_transfer_info` + `fetch`
- `python/ray/_private/serialization.py` — `_fetch_flight_table` = **consumer side**
- Run: `verify_flight_store.py` `FLIGHT_MODE="native"`; `bench_flight_store.py --mode arrow-native`
- *Ignore for native:* `arrow_flight_transport.py`, `rdt/util.py`.

**RDT path (Path A)** = shared engine **+**:
- *Prereq (NOT in PR — the framework it plugs into):* `rdt/tensor_transport_manager.py` (interface),
  `rdt/rdt_store.py`, `rdt/rdt_manager.py`, `serialization.serialize_rdt_objects`,
  `worker.get_objects` RDT branch, C++ `ReturnObject.direct_transport_metadata`.
- `python/ray/experimental/rdt/arrow_flight_transport.py` — implements interface for `pa.Table`
- `python/ray/experimental/rdt/util.py` — registers `ARROW_FLIGHT`
- Run: `verify_flight_store.py` `FLIGHT_MODE="rdt"`; `bench_flight_store.py --mode arrow-rdt`
- *Ignore for RDT:* `flight_object_store.py`, `FLIGHT_TABLE` const, `serialization._fetch_flight_table`,
  the `_raylet.pyx` intercept.

**`_raylet.pyx` straddles buckets:** VM-binding include = shared; `store_task_outputs` intercept =
native-only. **One-sentence contrast:** native hooks at the core-worker serialize boundary and ships
coordinates as a **plasma object value**; RDT plugs into `TensorTransportManager` and ships
coordinates via **RDT's out-of-band metadata channel**.

(View the code: `gh pr diff 62772 --repo ray-project/ray`, or check out the PR branch.)

### 3.8 RDT path — end-to-end trace (one `@ray.method(tensor_transport="ARROW_FLIGHT")` call)
`ARROW_FLIGHT` is **one-sided**, `data_type=pa.Table` — both matter at the transport-call points.
The RDT *framework* + the `ArrowFlightTransport` plugin are all present in this worktree (PR applied,
commit `dc2ae38801`). Core idea: **CPU/structural data rides the normal object store; the
table rides heap→heap via `FlightCore`; they're recombined by index on deserialize.**

**Terms (short):** *one-sided* = receiver pulls, sender passive (no scheduled send) — `ARROW_FLIGHT`, NIXL;
*two-sided* = sender+receiver post matching send/recv that rendezvous — NCCL, GLOO (can deadlock → kill on
timeout). `actor.__ray_call__(fn, …)` = run arbitrary `fn(self, …)` on an actor, on the `_ray_system`
background thread. `__ray_send__`/`__ray_recv__` (`rdt_store.py:19`/`:75`) = per-actor helpers `__ray_call__`
invokes → they call the transport's `send_multiple_tensors`/`recv_multiple_tensors`. One-sided fires only `__ray_recv__`.

**Phase 1 — Submit (on caller = owner).** `producer.make_table.remote()` → `ActorMethod.remote`
(`actor.py:916`) → `_remote` (`:1024`) → the `invocation` closure (`:1082`): (1)
`queue_or_trigger_out_of_band_tensor_transfer(producer, args)` (`:1091`, **before** the C++ submit;
no-op here — no RDT args); (2) `_actor_method_call` (`:2324`) → `core_worker.submit_actor_task` (`:2411`,
Cython `_raylet.pyx:3788`) → **`CoreWorker::SubmitActorTask`** (`:3847`), shipping the task with
`tensor_transport` in the spec; (3) **after** submit, `rdt_manager.add_rdt_ref` (`:1120` →
`rdt_manager.py:393`) registers `RDTMeta{src_actor, backend, tensor_transport_meta=None}` on the owner
(needs the returned ref; `meta=None` until the reply). So the two RDT touch-points **straddle**
`SubmitActorTask` (arg-transfer trigger before, output-ref registration after); `SubmitActorTask` itself
is RDT-unaware — it just carries the `tensor_transport` spec field.

  *Gory detail.* `tensor_transport` rides as an **opaque `optional<string>`** through C++:
  `ActorMethod._tensor_transport` → `submit_actor_task` (Cython `optional[c_string]`, default
  `NULL_TENSOR_TRANSPORT`, `emplace` if set, `_raylet.pyx:3820`) → `CTaskOptions` → `SubmitActorTask` →
  `TaskSpecBuilder`: `message_->set_tensor_transport(*tensor_transport)` (`task_util.h:326`) → TaskSpec field
  `44` (`common.proto:622`) → executor reads it into `store_task_outputs` (`_raylet.pyx:2108`) → the
  `tensor_transport is not None` branch. The C++ never interprets it — pure courier. *(Distinct from the
  **arg-ref** `tensor_transport` — `task_util.h:77`, proto 728/771, set by `InlineDependencies` so a
  consumer knows an input is RDT: task-field = "my returns use RDT", arg-field = "this input is RDT".)*
  `add_rdt_ref` (`rdt_manager.py:393` → `set_rdt_metadata:244`, under `_lock` — the dict is touched by both
  the user thread and the CoreWorker io thread) stores `RDTMeta` keyed by **`obj_ref.hex()`**:
  `{src_actor=producer, backend, tensor_transport_meta=None, sent_dest_actors=set(), warned=False,
  target_buffers=None}`. `meta=None` until Phase 3's callback `_replace`s it (`ray.put` precomputes it,
  `put_object:935`); it runs **after** submit because the dict key *is* the submit's returned ref.

**Phase 2 — Produce (on producer actor).** `store_task_outputs` `tensor_transport is not None` branch
(`_raylet.pyx:4344`):
- `serialize_rdt_objects(output, backend)` (`serialization.py:689`) — registers a `pa.Table` custom
  serializer; with `use_external_transport=True`, each table hits the `serialize` closure (`:704`) →
  **appended to `thread_local.rdt_tensors`, replaced by its integer index** in the msgpack blob
  (`:709-710`). Returns `(cpu_blob_with_indices, [table])` — heavy table pulled OUT of the bytes.
- `store_rdt_objects(return_id, [table], backend)` (`serialization.py:761`) → `rdt_store.add_object_primary`
  (`rdt_store.py:240`): stores `[table]` in the **producer's `RDTStore`** AND calls
  `ArrowFlightTransport.extract_tensor_transport_metadata` → `FlightCore.put` (builds `_RecordingSink`
  scatter-list) + returns `{flight_uri,key,pid,node_id,ipc_size}`; returns `pickle.dumps(meta)`.
- `store_task_output` stores the tiny CPU blob as the ref's object-store value (inline).
- `return_ptr.get().SetDirectTransportMetadata(pickled_meta)` (`_raylet.pyx:4398`) → rides the
  `PushTaskReply` via `SerializeReturnObject` (`common.cc:85-88`).

**Phase 3 — Owner learns metadata.** Reply → `task_manager.cc:607` calls the `set_direct_transport_metadata_`
cb → `set_direct_transport_metadata` (`_raylet.pyx:2261`) → unpickle →
`set_tensor_transport_metadata_and_trigger_queued_operations` (`rdt_manager.py:423`) fills
`RDTMeta.tensor_transport_meta` and fires queued transfers/frees.

**Phase 4 — Consume (two paths).**
- **A) `ray.get(ref)` on owner** (`worker.py:1025`): `_get_rdt_ids` (`:907`) → `fetch_and_get_rdt_objects`
  (`rdt_manager.py:775`) → `_trigger_fetch` (`:465`). One-sided → `fetch_multiple_tensors` (default) →
  **`ArrowFlightTransport.recv_multiple_tensors` runs IN THE OWNER PROCESS** → same-node `fetch_via_vm`
  / cross-node `fetch_via_flight` from the producer's `FlightCore` → `[table]`. `deserialize_objects`
  loads it into `thread_local.rdt_tensors`; the `deserialize` closure (`serialization.py:720`) swaps
  index→table.
- **B) `ref` → consumer actor arg**: at submit the caller runs `queue_or_trigger_out_of_band_tensor_transfer`
  (`actor.py:1091`) → `trigger_out_of_band_tensor_transfer` (`rdt_manager.py:622`). One-sided ⇒
  `send_ref=None` (no `__ray_send__`); launches **`__ray_recv__` on the consumer** via a `_ray_system`
  task (`:720`) → `ArrowFlightTransport.recv` → stores `[table]` in the **consumer's `RDTStore`**. When
  the consumer's task deserializes its args, `get_rdt_objects` (`rdt_manager.py:744`) just **waits on the
  local `RDTStore`** → recombine. A `_monitor_failures` thread (`:265`) watches `recv_ref`, aborts/kills on
  timeout.

**Phase 5 — GC.** Ref out of scope → C++ cb → `queue_or_free_object_primary_copy` → `free_object_primary_copy`
(`rdt_manager.py:897`) launches `__ray_free__` on the producer (`rdt_store.py:118`) →
`ArrowFlightTransport.garbage_collect` → `FlightCore.delete`.

```
PRODUCER                               OWNER (caller)              CONSUMER
serialize_rdt_objects (pull table out)
store_rdt_objects → RDTStore + FlightCore.put → meta
SetDirectTransportMetadata ──reply──▶ set_..._metadata cb (meta filled)
                                        │
                           ray.get? ────┤── recv in-process ◀────── FlightCore (vm/flight)
                           arg-pass? ───┴── __ray_recv__ system task ─▶ recv on consumer ◀── FlightCore
```

**vs native (same `FlightCore` underneath):** native hooks the *same* `store_task_outputs` point but
ships coords as the object **value** (`FLIGHT_TABLE` metadata) and fetches **inline in the deserializer**
— no `RDTMeta`, no owner callback, no `__ray_recv__` system tasks, no monitor thread. Phases 1/3 and the
system-task half of 4B = the "owner/system-task overhead" Ed flagged the RDT path as carrying.

#### 3.8.1 RDT vs traditional args — *same control plane, added data plane*
A common misread: that RDT bypasses normal dependency resolution. It does **not**. An RDT ref is still
a normal `ObjectRef` whose object-store value is the small **CPU blob** (msgpack with a tensor *index*).
So when `consumer.process.remote(ref)` is submitted:
- The owner still waits for the producer's `HandlePushTaskReply` → `Put`(CPU blob), which resolves the
  consumer's dependency the **normal** way (`GetAsync` parked callback → `InlineDependencies` → ship).
  The consumer task is gated on this, exactly like a traditional arg.
- **Carve-out inside `InlineDependencies`** (`task_submission/dependency_resolver.cc`): for a
  non-`OBJECT_STORE` `tensor_transport` arg it inlines the CPU blob (`set_data` + `is_inlined=true`) but
  **keeps the `object_ref`** (skips `clear_object_ref`) and stamps `set_tensor_transport`, and **does not**
  push the id to `inlined_dependency_ids` (so the refcount isn't decremented — keeps the producer's
  tensors alive until transferred). The retained ref+transport is how the consumer keys into its `RDTStore`.

**What's actually added: a parallel out-of-band data plane for the bulk tensors.** `queue_or_trigger_…`
runs on the **owner** (the submitter that *manages* the ref — owner or a borrower via `_rdt_ref_deserializer`),
and the owner acts as a **conductor**: it fires `__ray_recv__` (one-sided) / `__ray_send__`+`__ray_recv__`
(two-sided) **system tasks** to the actors; the bulk tensors flow **producer→consumer heap-to-heap**,
never through the owner or the object store. Both channels are driven by the **same producer reply** —
the return value (CPU blob → `Put` → resolves dep → ships task) *and* `direct_transport_metadata`
(→ callback → fills `RDTMeta` + fires the queued recv) ride it together; the tensor transfer is kicked off
**eagerly, in parallel** with consumer-task dispatch (pipelining win). The CPU+tensor **join happens on
the consumer** at arg-deser (`get_rdt_objects` waits on its local `RDTStore`), not at the owner.
(Contrast: in the `ray.get` path the owner runs `recv` *in-process* — there it *is* a data-path participant.)

| | traditional arg | RDT arg |
|---|---|---|
| ref is a dep; owner waits for producer reply | ✅ | ✅ same |
| dep resolved + inlined into consumer spec | ✅ clears ref | ✅ but **keeps ref** + stamps transport, no decref |
| consumer task gated on dep resolution | ✅ | ✅ same |
| bulk payload | inlined spec / plasma | **out-of-band producer→consumer heap**, parallel, same reply |
| final assembly | at owner before ship | **on the consumer** at arg-deser |

#### 3.8.2 How `ArrowFlightTransport` plugs into the framework
The RDT framework is transport-agnostic — `rdt_store.py` / `rdt_manager.py` / `__ray_recv__` speak only
`TensorTransportManager` + `TensorTransportMetadata`, never Flight. The plugin connects via **two seams**:
1. **Registry (by name):** `util.py` → `register_tensor_transport("ARROW_FLIGHT", ["cpu"], ArrowFlightTransport,
   pyarrow.Table)`. Generic `__ray_recv__` (`rdt_store.py:75`) calls
   `get_tensor_transport_manager(backend).recv_multiple_tensors(obj_id, meta, comm_meta)` → for `"ARROW_FLIGHT"`
   that resolves to the `ArrowFlightTransport` singleton.
2. **Metadata subclass (the coordinates):** `ArrowFlightTransportMetadata(TensorTransportMetadata)` carries
   `{flight_uri, key, pid, node_id, ipc_size}` — produced on the put side, consumed on the recv side.

Both ends bottom out in the shared `FlightCore` (`self._core = get_flight_core()` in `__init__`):
```
RDT generic              ArrowFlightTransport (plugin)            FlightCore (engine)
extract_..._metadata ──▶ extract_tensor_transport_metadata ────▶ put() → scatter-list, returns {uri,key,pid,…}
__ray_recv__         ──▶ recv_multiple_tensors(meta)        ────▶ fetch_via_vm / fetch_via_flight (by meta.node_id)
```
- **put side:** `store_rdt_objects` → `add_object_primary` → `extract_tensor_transport_metadata` → `FlightCore.put`.
- **recv side:** `recv_multiple_tensors(meta)` → same-node `fetch_via_vm` / cross-node `fetch_via_flight`.

Same seam any custom transport uses (NCCL/GLOO/NIXL = their own metadata + send/recv). `send_multiple_tensors`
raises `NotImplementedError` (one-sided).

### 3.9 NATIVE path — consumer-side fetch (deep trace)
Native is much simpler than RDT: **no system tasks, no `rdt_manager`, no `_get_rdt_ids`, no metadata
callback, no monitor thread.** The coord dict (tagged `FLIGHT_TABLE`) is inlined into the consumer
task spec as a normal by-value arg (native sets no `tensor_transport`, so `InlineDependencies` takes the
ordinary `clear_object_ref` + `set_data` + `set_metadata` branch — `FLIGHT_TABLE` metadata travels). So
the raylet has **nothing heavy to pull** (table was never in plasma); the task dispatches immediately and
the **fetch happens lazily in the worker at arg-deserialization**.

**Entry — `execute_task` (`_raylet.pyx:1714`), `task:deserialize_arguments` block (`:1829`)** (runs on
the consumer worker, *before* the user fn, *synchronous* on the critical path):
1. args arrive as C++ `RayObject`s; the inlined coord-dict arg = `RayObject{data=msgpack, metadata=FLIGHT_TABLE}`.
2. `RayObjectsToSerializedRayObjects` (`:1836`) → `(data, metadata)` pairs.
3. `worker.deserialize_objects` (`:1858` → `worker.py:935`): `_get_rdt_ids` (`worker.py:947`) returns `[]`
   (metadata is `FLIGHT_TABLE` not `PYTHON`, `tensor_transport=None`) → **RDT machinery skipped** →
   `context.deserialize_objects` (`serialization.py:577`) → `_deserialize_object` (`:433`).
4. `_deserialize_object` `metadata[0]==FLIGHT_TABLE` (`:450`) → `_fetch_flight_table(data)` (`:419`):
   `msgpack.unpackb` → `{flight_uri,key,pid,ipc_size,node_id}` → `get_flight_store().fetch(info)`.

**`fetch(info)` (`flight_object_store.py`):** `same_node = producer node_id == my node_id and linux` →
`fetch_via_vm` else `fetch_via_flight`; then `send_delete_rpc`; return table.
- **`fetch_via_vm` (`flight_core.py:159`)** — same-node: `pa.allocate_buffer(size)` (consumer heap) →
  pack `{key,consumer_pid,buf.address,size}` → cached `_get_client(flight_uri)` (`:204`) →
  `do_action("scatter_write_vm")`. Producer side: `_Server.do_action` (`:256`) → `_handle_scatter_write`
  (`:214`) → cached `_sinks[key]` scatter-list → `vm_scatter_write` → `process_vm_writev` gathers the
  producer's scattered column buffers into the consumer's `local_buf` (1 syscall). Then
  `ipc.open_stream(local_buf).read_all()` (`:181`) → `pa.Table` (zero-copy views into `local_buf`).
- **`fetch_via_flight` (`flight_core.py:183`)** — cross-node: `do_get(Ticket(key))` → producer `do_get`
  (`:249`) returns `RecordBatchStream(table)` → gRPC streams IPC → `read_all()`.

Returns the table → deserialized arg → `recover_args` (`:1864`) → user fn gets the `pa.Table`.

**Nuances / design decisions (from discussion):**
- **(1) Deletion is CONSUMER-triggered; the producer is passive.** `fetch()` calls `send_delete_rpc`
  *after* it has pulled the table; the producer never auto-deletes after pushing (`_handle_scatter_write`
  only writes). **Who triggers deletion, across transports:** native = *consumer read-driven*; RDT =
  *owner ref-counting* (`__ray_free__` on out-of-scope, `rdt_manager.py:897`); normal plasma = *owner
  ref-counting* + LRU. So native is the odd one out — read-driven, not ownership-driven. **Design axis to
  evaluate** (see §4): consumer-signal (retry-safe; but leaks if the consumer dies post-fetch-pre-delete)
  vs producer-delete-after-push (eager, *move*-like; unsafe for retries). Relates to the **plasma
  move-semantics** work (who owns/frees after a hand-off). ✅ For our **Ray Data single-consumer** case
  delete-on-first-read is fine — no fan-out.
- **(2) No pipelining.** The fetch is synchronous inside `task:deserialize_arguments` (vs RDT pre-firing
  `__ray_recv__` in parallel with dispatch), so the bulk transfer does **not** overlap with consumer-task
  scheduling. A perf axis to revisit.
  - **Dormant at max-in-flight=1 (§5.3 config):** with one pair in flight and the driver blocking per pair,
    everything is strictly serial — there's no queued/concurrent work for *any* transport to overlap the transfer
    with, so plasma's prefetch and native's lack of it are both inert. Pipelining changes nothing in those numbers.
  - **Structural deficit for the throughput sweep:** plasma overlaps arg-transfer with compute via the raylet
    pulling queued tasks' args ahead of the worker — **even at concurrency 1** (a Kartica PR sharpens this). Native
    can only overlap via **`max_concurrency > 1`** (another task thread runs while one blocks on its synchronous
    fetch); at concurrency 1 it can't pipeline at all, regardless of in-flight depth. So the sweep must run
    **concurrency > 1** to give native a fair shot, else it measures "plasma pipelines, native doesn't." This is a
    **separate** deficit from the allocation `a` (the buffer pool doesn't fix it).
- **(3) Scatter-list is built at producer `put()`, not per fetch-request.** `FlightCore.put` walks the
  table once (`_RecordingSink`) and caches the scatter-list in `_sinks[key]`; each `_handle_scatter_write`
  *replays* it — never re-serializes. Design choice of **when to pay the walk cost**: eager-at-produce
  (amortized over N fetches, but paid even if never fetched) vs lazy-per-request. Current = eager-at-put.
- **Transport split:** consumer = Flight client, producer = Flight server; `flight_uri` selects the
  producer *process*, `pid` is the `process_vm_writev` target. Same-node: table bytes bypass gRPC (only the
  `DoAction` control msg rides it); cross-node: table rides gRPC (`DoGet`).

### 3.10 Copy-count model (analytical) + why large-table same-node arrow lags
Counting **bulk memcpys** (operations that touch every byte — produce / transfer / mutate copies; NOT the
metadata walk), for a pipeline of **X actors** each emitting one table (⇒ **X** produces, **X-1** transfer
edges, mutating consumers A₂…Aₓ):

| case | plasma | arrow native |
|---|---|---|
| same-node, read-only | X | X-1 |
| same-node, read-mod-write | 2X-1 | X-1 |
| cross-node, read-only | 2X-1 | X-1 |
| cross-node, read-mod-write | **3X-2** | X-1 |

**Per-primitive cost (the engine of the table):**
- **plasma** = #produces (heap→plasma, 1 each ⇒ **X**) **+ 1 per mutating consumer** (plasma is immutable →
  copy-out to a private heap buffer, ⇒ X-1) **+ 1 per edge cross-node** (object-manager network hop, ⇒ X-1).
- **arrow** = #transfers only (**X-1**; vm same-node / Flight cross-node). **Produce is free** (table stays in
  the producer heap — `_RecordingSink` records `(addr,size)`, no bulk copy) and **mutate is free** (vm/Flight
  lands the data in a *mutable* buffer → in-place). So arrow = X-1 in every cell.
- ⚠️ **cross-node RMW plasma = 3X-2, NOT 2X-1** (corrected): within plasma, RMW must cost (X-1) more than RO
  (the mutate copy-out) — same-node already shows that gap (X→2X-1), so cross-node must too (2X-1→3X-2).
- Convention/assumptions: "network hop = 1 copy" applied uniformly (counting send+recv as 2 adds X-1 to *both*
  columns → arrow's per-edge advantage is unchanged); tables large (>100 KiB) so plasma returns take the
  single-copy `in_plasma` path; coord dict tiny (negligible). The ±1 (X vs X-1 in same-node-RO) is the terminal
  output Aₓ that plasma materializes to plasma but arrow leaves in heap.

**The `put`-time walk is NOT an extra arrow cost (per Ed, 2026-06-17).** Worry: `FlightCore.put`'s
`_RecordingSink` walk (`flight_core.py:63-78`) is an extra pass vs plasma → blamed for large-table slowness.
It isn't — **plasma walks the table too**, in its serialization codepath, *fused with the bulk memcpy*
("before/while memcpy'ing the chunks"). plasma-produce = walk **+ copy**; arrow-produce = walk **only**. The
walk is O(#buffers) metadata, NOT O(#bytes) — it does not scale with table size, so it is not the cause of the
gap. (Confirms §3.9 / the analysis above: a memcpy ≠ the recording walk in time.)

**Why same-node large-table arrow still lags plasma — reader-side memory allocation (per Ed):** not copies,
not the walk. Ed's claim (his words): arrow *"won't perfectly cache the memory allocations from the kernel,
which plasma is effectively doing by mmap'ing the shared memory region on startup"*; he got **~exact parity
with plasma** by adding a **reader-side buffer-reuse cache** (a pool to "copy into"). Code fact: arrow allocates
a fresh buffer **per fetch** (`pa.allocate_buffer(size)`, `flight_core.py:168`) rather than reusing one.
- **(verify — my hypothesis for the underlying mechanism, NOT yet confirmed with Ed):** the cost is demand-zero
  **page faults** — a fresh per-fetch allocation's pages aren't resident, so `process_vm_writev` faults every
  destination page in on first touch, every transfer, scaling with size; plasma's once-`mmap`'d region keeps
  pages warm so repeated puts/reads don't re-fault. **Confirm the exact mechanism with Ed.**
- **Multiple vs single reader:** plasma stores 1 shared copy → N same-node readers ≈ 1 copy (zero-copy maps);
  arrow copies per reader → N readers = N copies. So arrow loses under **fan-out**, ~matches for a **single
  reader**. Ray Data single-consumer (§1) is the single-reader case → parity expected (with the cache); the
  *wins* remain **mutate** + **cross-node** + **peak-memory**.
- **Harness TODO:** add a reader-side buffer-reuse pool to `fetch_via_vm` (+ bench consumer) **before** any
  same-node verdict — else we're partly benchmarking `malloc`/page-faults, not the transport.

**Does deeper-pipeline (larger X) help arrow? Only after `a < c` — it's a per-edge contest, not accumulation.**
The copy *savings* grow with X (case 2: X, case 3: X, case 4: 2X-1), which suggests "deeper pipeline ⇒ bigger
arrow win." But arrow's per-transfer overhead `a` (allocate + page-fault + Flight round-trip + IPC parse) is paid
on **every edge** too, so it scales with the X-1 edges identically. Let `c` = one bulk-memcpy cost. Net arrow
advantage (drop common compute):
- case 2/3: `Δ = Xc − (X−1)a`,  slope `dΔ/dX = c − a`
- case 4:   `Δ = (2X−1)c − (X−1)a`,  slope `dΔ/dX = 2c − a`

So "benefit grows with X" iff **`a < c`** (cases 2/3) / **`a < 2c`** (case 4). The savings are linear and the
overhead is linear → it reduces to a **per-edge sign test**; X only multiplies whatever that per-edge verdict is.
- **Empirics (§5.3, 100 MB):** same-node read-only is a copy-count tie, so its whole gap *is* `a` → arrow 410 vs
  plasma 356 ⇒ **`a ≈ 54 ms`**, while a 100 MB memcpy `c ≈ 10 ms` ⇒ **`a ≈ 5c`**. Cross-node-modify (needs only
  `a < 2c`) still loses ⇒ confirms `a > 2c`. With `a > 2c` all slopes are **negative** → larger X makes arrow
  **worse** (the intuition is inverted). `Xc − (X−1)a > 0` with `a > c` ⇒ `X < a/(a−c) ≈ 1.25` → no depth ≥2 wins.
- **So the crossover is `a < c`, NOT some magic X** — and the buffer-reuse pool is what drives `a` down (amortize
  the per-fetch allocation to ~0, leaving just the control round-trip). *Then* the per-edge net flips positive and
  deeper pipelines scale the win linearly (case 4 flips first, needing only `a < 2c`). The pool is the precondition
  for the X-scaling regime to exist at all.
- **Caveat — what §5.3 measured:** a **single edge** (1 producer → 1 consumer returning an int, not a downstream
  table), streaming many tables = *throughput* scaling, not *depth* scaling. A true depth-X test needs a chained
  A→B→C… pipeline; but the per-edge sign test already settles it, so `a < c` must come first regardless.

---

## 4. Open questions / decisions to make
- [ ] **Does `--consumer-mode modify` actually measure the mutability benefit? (Kartica to revisit.)** The
  consumer's mutation-cost mechanics aren't pinned down yet — treat the modify cells as data only, draw no
  mutable-buffer conclusions, until confirmed. (Independent of §3.10's `a≈54 ms`, which came from read-only.)
- [ ] **Which path do we carry forward?** Native intercept (Path B) matches the "non-rdt" name. Confirm.
- [ ] **Transport scope:** keep both same-node (VM) + cross-node (Flight)? Or focus one first?
- [ ] **Data scope:** `pyarrow.Table` only, or also RecordBatch / general objects containing tables?
- [x] **End goal:** for **Ray Data** single-consumer producer→consumer pipelines (see §1) — benchmark vs
  plasma + design proof toward a Ray Data data-plane.
- [ ] **Object lifetime / who triggers deletion?** Native = consumer-signal (`send_delete_rpc`); RDT &
  plasma = owner ref-counting. Evaluate **producer-delete-after-push (move-like)** vs **consumer-signal
  (borrow-then-release)** — fault tolerance, leaks, retries. Cross-ref the **plasma move-semantics** worktree.
- [ ] **Microbenchmark matrix (the big one):** design benchmarks comparing the 2×2 — {plasma | bypass-plasma}
  × {copy | move} — for a Ray Data single-consumer pipeline; evaluate after the RDT flow is understood.
  Joint with `~/ray-worktrees/plasma-move-semantics`. Seed: `proto/bench_flight_store.py`.
- [ ] **Dev/test environment:** macOS can't run the VM fast path — do we need a Linux cluster set up?

---

## 5. Benchmark plan (design — run deferred)
**Decisions (2026-06-15):** design now, **run later**; **extend `proto/bench_flight_store.py`** (don't
reinvent); first metrics = **throughput + latency** (peak-memory + mutate-cost deferred).

**Harness is essentially ready for throughput+latency.** `proto/bench_flight_store.py` already streams
producer→consumer pairs with `--mode {ray|arrow-native|arrow-rdt}` × `--placement {same-node|cross-node|
mixed}` × `--consumer-mode {read-only|modify}` × `--sizes-mb` sweep (+ `--num-actor-pairs`, `--concurrency`,
`--max-in-flight`, `--duration`), reporting tables/s, MB/s, avg/p50/p99. So the work is the **cell→build
mapping + run matrix**, not new measurement code.

**Cell → (build, mode) mapping.** Full 2×2 can't run from one Ray build (`plasma+move` is the *other*
worktree); stitch via the shared `plasma+copy` baseline:

| cell | build (worktree) | `--mode` |
|---|---|---|
| plasma + copy (baseline) | arrow-prototype (or stock) | `ray` |
| bypass + copy (RDT) | arrow-prototype | `arrow-rdt` |
| bypass + move (native) | arrow-prototype | `arrow-native` (sets `RAY_USE_FLIGHT_NATIVE=1` via runtime_env) |
| plasma + move | **plasma-move-semantics** (move-sem ON by default) | `ray` |

For throughput+latency the meaningful knob is the **transport** (plasma vs native vs RDT); the move/copy
*eviction* distinction mostly surfaces in **peak memory** (deferred). So plasma+move's throughput/latency is
mainly a "does move-sem cost throughput?" check from the other build.

**Image / flag matrix (one image per cell on Anyscale — runs NEVER on macOS).** Key asymmetry: native &
move are **image/build-level** flags (hardcodable); RDT is **user-code** (a decorator), NOT an image flag.

| image | build (branch) | image-level flag | script producer |
|---|---|---|---|
| default ray (plasma+copy) | master / arrow-prototype | none | plain `make_table` |
| ray + move (plasma+move) | `plasma-move-semantics` | `enable_plasma_move_semantics` — **already default `true`** (`ray_config_def.h:374`) | plain |
| native (bypass+move) | arrow-prototype (PR) | **`RAY_USE_FLIGHT_NATIVE=1`** (env var, read at `_raylet.pyx:284`; bake as image/cluster ENV, or flip the gate) | plain (intercepts plain `pa.Table` returns) |
| rdt (bypass+copy) | arrow-prototype (PR) | **none — not an image flag** | producer method `@ray.method(tensor_transport="ARROW_FLIGHT")` (auto-enables `enable_tensor_transport`, `actor.py:1418`) |

Consequence: **default/move/native run the SAME plain script** (image differentiates); **RDT needs the
*decorated* script**. `bench_flight_store.py` already encodes this per `--mode` (`arrow-rdt`→decorated,
`arrow-native`→plain+env, `ray`→plain). Efficiency note: the PR build alone covers `{ray, arrow-native,
arrow-rdt}` via `--mode`, so **2 images + `--mode` suffice** (PR build for 3 cells; plasma-move build for the 4th)
— the 4-image plan is fine too (more isolation), as long as the RDT image carries the decorated script.

**✅ CHOSEN run approach (2026-06-15): ONE PR image + `--mode`** (built from arrow-prototype). Covers **3 cells,
no script changes:** `--mode ray` (plasma+copy), `--mode arrow-native` (bypass+move; script injects
`RAY_USE_FLIGHT_NATIVE=1` via `runtime_env`, `bench_flight_store.py:294`), `--mode arrow-rdt` (bypass+copy,
decorated producer). **`plasma+move` deferred** → needs the `plasma-move-semantics` branch image (no flag
enables move-sem in the PR build); add later together with the peak-memory metric. Prereqs (environment, not
code): build the image from THIS branch (verify `from ray._raylet import vm_scatter_write`); **passwordless
sudo / writable `ptrace_scope`** (see below); **≥2 worker nodes** for `--placement cross-node`/`mixed`.

**`_enable_ptrace()` (why it's in the script):** sets the node's Yama `/proc/sys/kernel/yama/ptrace_scope = 0`
so a producer actor can `process_vm_writev` into a **sibling** consumer actor's memory. Default `=1` allows
only parent→child ptrace; Ray workers are siblings (both raylet children) → the same-node fast path would get
`EPERM` without it; `=0` = any same-uid process. Runs in **every actor `__init__`, all modes** (the knob is
per-node and actors spread across nodes; idempotent) and shells out `sudo tee` → **needs passwordless sudo +
writable knob, Linux-only**; if unavailable, *every* actor fails at init. (Only the same-node native/RDT path
truly needs it.)

**⚠️ Gotchas hit on Anyscale (2026-06-15) — both are HARNESS gaps, not transport bugs:**
- **Same-node `process_vm` needs `ptrace_scope=0` — and `verify_flight_store.py` does NOT call `_enable_ptrace()`
  (only `bench` does).** So native-mode `verify` fails the same-node cases (`actor_actor`/`chained`) with
  `EPERM` while `driver_get`/`put_get` pass (driver on the head node → cross-node Flight `DoGet`). Fix: set
  `kernel.yama.ptrace_scope=0` **cluster-wide** via an Anyscale node startup command
  (`sudo bash -c 'echo 0 > /proc/sys/kernel/yama/ptrace_scope'`), OR add `_enable_ptrace()` to verify's actors.
- **RDT receiver actor MUST have `@ray.remote(enable_tensor_transport=True)`.** That flag creates the
  `_ray_system` concurrency group (`actor.py:1490-1494`) that `__ray_recv__` is dispatched onto. Without it the
  recv never runs → `wait_and_pop_object` 60s `TimeoutError` → the monitor **kills both actors** (`ARROW_FLIGHT`
  can't abort) → every later case cascades to `ActorDiedError`. The **producer** auto-enables it via the
  `@ray.method(tensor_transport=...)` decorator; the **receiver** must set it explicitly. **Unrelated to
  `max_concurrency`** (`_ray_system` is a separate thread pool; sync actors are fine). **Native consumers need
  nothing** (they fetch inline in the arg-deserializer). Fixed in `proto/verify_flight_store.py` +
  `proto/bench_flight_store.py` Consumers.

**Config grid (proposed):** `--sizes-mb 1 10 100` (add 1000 on a big box); `--consumer-mode read-only`;
`--placement same-node` AND `cross-node`; `--num-actor-pairs 4 --concurrency 4 --duration 10` (raise duration
for stable p99).

**Environment caveats (critical):**
- **Run target = Anyscale or the Linux devbox — NEVER this macOS box** (macOS is code-reading/design only;
  we will not run benchmarks locally). Both targets are Linux, so the same-node `process_vm_writev` fast path
  **and** true cross-node are available (needs `yama/ptrace_scope=0`, which `_enable_ptrace()` sets via sudo).
  The macOS `sys.platform!="linux"` → fallback-to-gRPC behavior is therefore moot for our runs.
- **Rebuild required on the run target** — must `/rebuild` Ray from THIS worktree (and from plasma-move for
  that cell) before running. Verify `from ray._raylet import vm_scatter_write`.
- **Cross-worktree comparability** — run the SAME `bench_flight_store.py` on both builds (copy it into the
  plasma-move worktree), identical config, shared `--mode ray` baseline as the anchor.

**Minimal harness changes (later, if needed):** copy `bench_flight_store.py` into plasma-move worktree;
optional CSV/markdown output for cross-build collation; optional `--label` to disambiguate plasma-copy vs
plasma-move. (Deferred metrics: peak object-store memory — cf. plasma-move's bench `c34ec3f187`; mutate cost
via `--consumer-mode modify`.)
- **Reader-side buffer-reuse pool** in `fetch_via_vm` (`flight_core.py:168`, per-fetch `pa.allocate_buffer`) —
  Ed matched plasma perf with one; needed for a fair same-node comparison (else measuring per-fetch allocation,
  not the transport). See §3.10. (Underlying page-fault mechanism `(verify)` with Ed.)

**Deferred-run checklist:** (1) `/rebuild` arrow-prototype, verify `vm_scatter_write` imports; (2) run the 3
arrow-prototype cells across the grid (macOS = gRPC variants); (3) on **Linux**, rerun for the `process_vm`
fast path + true cross-node; (4) `/rebuild` + run `--mode ray` on plasma-move for the plasma+move cell;
(5) collate against the shared `plasma+copy` baseline.

### 5.1 Harness reference — full walkthrough of `bench_flight_store.py`
**What it is:** a **steady-state streaming benchmark** — producer→consumer actor pairs hand `pa.Table`s across
Ray; the driver keeps `--max-in-flight` pairs outstanding for `--duration` s and reports per-pair latency +
throughput. Purpose: race the 3 transports (`ray`/plasma, `arrow-native`, `arrow-rdt`) under identical work.

**Params (CLI, `:40-59`):**
| param | meaning |
|---|---|
| `--mode` | `ray`=plasma, `arrow-native`=`FLIGHT_TABLE` intercept (env `RAY_USE_FLIGHT_NATIVE=1`), `arrow-rdt`=`ARROW_FLIGHT` RDT |
| `--placement` | `same-node` (→ process_vm path for bypass) / `cross-node` (→ Flight DoGet) / `mixed`; cross/mixed need ≥2 nodes |
| `--consumer-mode` | `read-only` (return num_rows) / `modify` (`arr+=1` per numeric col; whether it measures a real mutability difference is open — §4) |
| `--consumer-output` | `num-rows` (return the row count — **no consumer produce**) / `table` (republish the table — **adds a produce copy on the consumer**, needed for the read-modify-**write** / diff-2 case). Orthogonal to `--consumer-mode`; default `num-rows`. |
| `--num-actor-pairs` | # parallel producer/consumer actor pairs |
| `--concurrency` | `max_concurrency` per actor (task threads per actor) |
| `--max-in-flight` | outstanding pairs cap = pipeline depth (default pairs×concurrency). **`1` = fully serial → latency-bound** |
| `--duration` | steady-state window (s) after warmup |
| `--sizes-mb` | table sizes to sweep |

**Component walk (top→bottom):**
- **Modes (`:33-37`)** — `MODE_LABELS` names the 3 transports; actor classes are built *per mode* (below).
- **Actors built dynamically so the mode shapes the class:**
  - `_enable_ptrace()` (`:65-76`) runs in **every actor `__init__`** — `echo 0 | sudo tee ptrace_scope` so a
    producer can `process_vm_writev` into a sibling consumer (needs passwordless sudo).
  - **Producer** (`_make_producer_cls`, `:79-103`): `make_table(size_mb)` → one float64 column of
    `size_mb*1024*1024//8` rows (`pa.table({"data": np.random.randn(n)})`). For `arrow-rdt` the method is
    `@ray.method(tensor_transport="ARROW_FLIGHT")`; for `ray`/`arrow-native` it's plain (native = env var, not code).
  - **Consumer** (`_make_consumer_cls`, `:106-143`): `read-only` → `return table.num_rows`; `modify` →
    `arr += 1` over each numeric column, then `return table.num_rows` (whether the modify cell measures a real
    mutability difference is an open question — §4). Both are `@ray.remote(..., enable_tensor_transport=True)`
    (the RDT-receiver fix).
- **Placement (`:146-181`):** `_worker_nodes()` = alive ≥1-CPU nodes; `_plan_placement` maps `same-node`
  (all on `nodes[0]`) / `cross-node` (producers `nodes[0]`, consumers `nodes[1]`) / `mixed` (round-robin);
  `_create_actors` pins each actor with `label_selector={"ray.io/node-id": …}`.
- **Streaming engine `_Stream` (`:187-246`) — the core:**
  - `fill(target)` (`:200-212`): submit pairs until `target` outstanding. Round-robins the producer,
    **load-balances** the consumer (`argmin` outstanding), **stamps submit time BEFORE the producer submit**
    (`submit_t`, fixed 2026-06-17), then `make_table.remote()` → `process.remote(ref)`.
  - `wait_available()` (`:214-230`): `ray.wait(pending, num_returns=all, timeout=1ms, fetch_local=False)` —
    harvests finished refs, `latency = now − submit_t`. **`fetch_local=False`** ⇒ driver never pulls the table
    (not on the measured path; driver isn't a bottleneck).
  - `drain()` (`:232-242`): flush outstanding without recording (clean boundary between sizes).
- **`bench()` per size (`:249-286`):** (1) **warmup** `fill`+drain, *untimed* (first-time alloc/server-boot out of
  window); (2) **steady-state** — for `duration` s keep refilling to `max_in_flight`, collect latencies;
  (3) compute `avg/p50/p99`, `tables/s = done/elapsed`, `MB/s = size × tables/s`; print; `drain()`.
- **`main()` (`:289-329`):** `arrow-native` injects `RAY_USE_FLIGHT_NATIVE=1` via `runtime_env` (`:293-294`);
  `ray.init`; plan placement; build mode-specific actor classes; create pinned actors; `max_in_flight` defaults to
  `pairs × concurrency`; loop `bench()` over sizes; `ray.shutdown()`.

**Output per size:** `done` (pairs completed in window) · `avg/p50/p99` latency (ms) · `tables/s` · `throughput MB/s`.

**Methodology choices that matter:**
- **Latency = submit→consumer-done** (`fill():submit_t` → `wait_available()`): includes producer compute
  (`np.random.randn`) + transfer + consumer compute; **excludes** pulling the result to the driver.
- **⚠️ Latency is `observe − submit`, an upper bound (inflation-only) — potential mismatch, NOT yet addressed
  (decide later).** The driver stamps completion when its `wait_available` loop *observes* the ref ready, not when
  the consumer actually finished. If the consumer finishes at T but the driver is mid-`fill()`/bookkeeping or
  OS-descheduled and doesn't return from `ray.wait` until T+δ, it records `T+δ − submit`. **Small for
  `max_in_flight=1`** (driver is parked in `ray.wait` almost the whole time → mostly a p99 effect; doesn't move the
  §5.3 means or the ~54 ms 100 MB gap); **material for the throughput sweep** (`max_in_flight>1` ⇒ real
  per-iteration submit work + a loaded node ⇒ bigger δ). Roughly uniform across modes (won't flip rankings) but
  inflates absolute + p99, worst at small sizes. **Distinct from the `ray.wait` 1 ms timeout.** Fix options
  (blocking `num_returns=1`, a dedicated observer thread, source-side timestamps) **deferred**.
- **`--max-in-flight` = pipeline depth.** `1` ⇒ fully serial / latency-bound, **no overlap** so pipelining is inert
  for all transports (§5.3 config). `>1` ⇒ throughput regime where pipelining/concurrency matter (see §3.9 nuance 2:
  native needs `concurrency>1` to overlap its synchronous fetch). It **continuously tops up** to the target depth
  (sliding window), it does **not** drain the whole batch before refilling.
- **Work is identical across modes** → cross-mode deltas isolate the transport (absolute ms includes the common
  produce/consume compute).
- **What it does NOT do:** no peak-memory metric; no plasma+move cell; consumer never republishes a table
  (single edge, not a depth-X pipeline).

### 5.2 First results (2026-06-15) — REVISIT (don't over-read yet)
Config: `same-node, --num-actor-pairs 1 --concurrency 1` (⇒ max-in-flight 1, **fully serial / latency-bound**),
`read-only`, `--duration 5`, sizes 1/10/100 MB. (Cluster had 2 worker nodes but placement=same-node.)

| size | ray (plasma) | arrow-native | arrow-rdt |
|---|---|---|---|
| 1 MB | 152 t/s, 6.2 ms | **178 t/s, 5.3 ms** | 120 t/s, 7.7 ms |
| 10 MB | **27 t/s, 36.9 ms** | 26 t/s, 37.5 ms | 25 t/s, 40.0 ms |
| 100 MB | **2.8 t/s, 356 ms** | 2.4 t/s, 408 ms | 2.4 t/s, 412 ms |

Headline: **RDT consistently slowest** (most orchestration overhead); native wins only at 1 MB; **plasma wins at
10/100 MB**. ⚠️ Caveat for revisiting: this config is **serial/latency-bound, same-node** — same-node plasma is
shared-memory near-zero-copy, hard to beat, and the bypass paths' likely wins (**concurrency/throughput,
cross-node, `modify` mutate cost, peak memory**) are NOT exercised here. So treat as a latency-overhead snapshot,
not a verdict.
**Likely cause of the 10/100 MB same-node gap (per Ed — see §3.10): reader-side `pa.allocate_buffer` per-fetch
allocation, not copies/walk; Ed matched plasma with a reader-side buffer-reuse cache. Add the pool before any
same-node verdict.** (The page-fault *mechanism* for why is `(verify)` pending Ed.)

### 5.3 Full 2×2×3 grid (2026-06-17) — first complete matrix
**Run:** Anyscale staging workspace `expwrk_2w7vjic28c8m3rdc1ug7vlib4y`, 2 worker nodes (8 CPU each), Ray built
from this branch (`vm_scatter_write` imports; passwordless sudo → `_enable_ptrace()` sets `ptrace_scope=0` per
node). Grid = {same-node, cross-node} × {read-only, modify} × {ray, arrow-native, arrow-rdt} × sizes 1/10/100 MB,
`--num-actor-pairs 1 --concurrency 1 --duration 5` (⇒ max-in-flight 1, **serial / latency-bound**). Runner:
`proto/run_2x2_bench.sh`.
- **Cell = `avg · p99 (ms) · throughput (MB/s)`.** "avg" = mean **per-pair end-to-end latency, submit→consume**
  (driver submits `make_table.remote()`+`process.remote(ref)`, stops when the consumer ref is done via
  `ray.wait(fetch_local=False)`). Includes producer compute (`np.random.randn`, identical across modes) +
  transfer + consumer compute. Serial ⇒ MB/s ≈ size/avg, reciprocal of latency → same winner per row.
- ⚠️ **Timer caveat (this run):** collected with the pre-fix timer stamped *after* both `.remote()` submits, which
  excludes driver submit overhead and slightly favors RDT. **Fixed 2026-06-17** (stamp before the producer
  submit). Same-node read-only reproduces §5.2 within noise → harness stable. (winner per row by avg = **bold**.)
- ⚠️ **The consumer did NOT produce a copy here (`--consumer-output num-rows`, added later), so the §3.10 formula
  is NOT directly applicable to these numbers.** §3.10's `2X-1` / `3X-2` assume **X produces** — i.e. *every*
  actor (including the terminal consumer) emits a table. This consumer returned `num_rows`, so it's one produce
  short: plasma modify is `2X-2` (=2 at X=2), not `2X-1` (=3), ⇒ the arrow-vs-plasma copy gap here is **1, not
  2**. The formula's full gap (diff = X, e.g. 2 at X=2) only holds when the consumer **republishes**
  (`--consumer-output table`). So read these cells as the *read-modify* (diff-1) case; the *read-modify-write*
  (diff-2) case needs `--consumer-output table`.

**Same-node**
| avg · p99 (ms) · MB/s | ray (plasma) | arrow-native | arrow-rdt |
|---|---|---|---|
| read-only 1 MB | 6.7 · 9.5 · 141 | **5.7 · 7.0 · 166** | 8.3 · 10.6 · 111 |
| read-only 10 MB | **37.1 · 40.4 · 264** | 38.6 · 41.7 · 256 | 40.7 · 45.2 · 240 |
| read-only 100 MB | **356.1 · 367.2 · 280** | 409.9 · 418.9 · 240 | 410.6 · 412.6 · 240 |
| modify 1 MB | 6.9 · 9.3 · 136 | **6.3 · 8.6 · 150** | 8.3 · 11.4 · 111 |
| modify 10 MB | **40.4 · 47.0 · 244** | 41.6 · 45.1 · 236 | 43.9 · 48.4 · 222 |
| modify 100 MB | **411.0 · 417.5 · 240** | 457.9 · 461.2 · 200 | 461.8 · 472.0 · 200 |

**Cross-node**
| avg · p99 (ms) · MB/s | ray (plasma) | arrow-native | arrow-rdt |
|---|---|---|---|
| read-only 1 MB | **10.5 · 12.6 · 91** | 13.4 · 16.6 · 72 | 24.7 · 28.8 · 39 |
| read-only 10 MB | **53.2 · 61.8 · 184** | 54.9 · 62.9 · 180 | 67.8 · 78.8 · 144 |
| read-only 100 MB | **452.7 · 461.9 · 220** | 598.3 · 599.6 · 160 | 611.9 · 619.0 · 160 |
| modify 1 MB | **10.9 · 12.6 · 87** | 16.2 · 18.1 · 60 | 27.7 · 30.6 · 35 |
| modify 10 MB | **56.8 · 64.3 · 174** | 64.9 · 70.1 · 152 | 73.1 · 87.2 · 134 |
| modify 100 MB | **505.0 · 514.1 · 180** | 648.8 · 652.9 · 140 | 666.3 · 673.7 · 140 |

**Headline:** **plasma (ray) wins 10/12 cells.** arrow-native wins only same-node 1 MB (RO + modify); RDT is
slowest everywhere. **The two predicted arrow wins — `modify` and `cross-node` — do NOT materialize** in this
serial / no-buffer-pool config.

**Conclusion (gating result):** with **no reader-side buffer-reuse pool**, arrow's per-fetch `pa.allocate_buffer`
overhead (§3.10 / Ed) costs **more than every copy it saves**, in every cell — so arrow can't even reach parity,
let alone show its modify/cross-node/peak-mem advantages. **The buffer pool is the gating fix; implementing it and
re-running modify + cross-node is the decisive test of the whole hypothesis.**

**Caveats (don't over-read):** serial / single-pair / max-in-flight 1 → latency-bound (no concurrency/pipelining
or RDT parallel-recv exercised); the `modify` cells are reported as data only (what the consumer's mutation step
costs per transport is a separate open question, §4); peak-memory not measured; plasma+move (4th 2×2 cell) not
run; pre-fix timer (above); latency is an **upper bound** (driver-observation lag — see §5.1; small here at
max-in-flight 1, mostly p99, but flagged for the throughput sweep).

**Next:** (1) implement reader-side buffer pool in `fetch_via_vm`, re-run modify + cross-node (decisive);
(2) concurrency/throughput sweep (`--num-actor-pairs 4 --concurrency 4`, raise `--max-in-flight`) to expose
pipelining where latency/throughput decouple — **must use `--concurrency > 1`** or native can't pipeline its
synchronous fetch and the sweep just measures plasma's raylet prefetch advantage (see §3.9 nuance 2).

### 5.4 Profiling plan — where does arrow-native's time go vs plasma? (design; markers not placed yet)
**Question.** NOT "reconcile the ~54 ms gap." Instead: **measure, symmetrically for both `arrow-native` and `ray`,
how long each stage of the pipeline takes**, so we see where each spends its time. **Native-first; RDT deferred.**

**Focus case (start here, nothing else):** 100 MB · `modify` · same-node · `--num-actor-pairs 1 --concurrency 1`
— the simplest cell and arrow's *best theoretical case* (fewest copies). Run **both** `--consumer-output`:
- `num-rows` → read-modify (copy gap **1**: plasma's mutate copy-out).
- `table` → read-modify-write (copy gap **2**: + the republish produce copy).
Run each as the streaming loop for `--duration` (many samples → mean/p50/p99), not a single transfer.

**Expected copy ledger** (what the timers should confirm or refute), X=2, as `num-rows / table`:
| stage | plasma | arrow-native |
|---|---|---|
| produce→store | serialize→plasma: 1 / 1 | `put` (records addrs): 0 / 0 |
| transfer | map from plasma: 0 / 0 | vm write: 1 / 1 |
| mutate | copy-out (immutable): 1 / 1 | in-place: 0 / 0 *(§4 unverified)* |
| republish | — / serialize→plasma: 0 / 1 | — / stays in heap: 0 / 0 |
| **total copies** | **2 / 3** | **1 / 1** |
⇒ expected arrow copy-advantage = **1** (`num-rows`) or **2** (`table`). §5.3 shows arrow *slower* despite this ⇒
the timers must reveal where arrow gives it back (candidates: `alloc` page-faults, Flight RPC overhead).

**Clock.** `time.clock_gettime(time.CLOCK_MONOTONIC)` — system-wide on Linux ⇒ timestamps comparable across
producer / consumer / driver **on one node** (same-node only). Stamping events *in-process* **sidesteps the
driver-observation lag** (§5.1 caveat) for the segment breakdown.

**Markers — symmetric event stamps (BOTH modes):**
- Producer: `t_make_start` / `t_make_end` around `make_table` → **produce-compute**.
- Consumer `process`: `t_proc_start`; sub-time `to_numpy(...)` and `arr += 1` separately (+ `np.shares_memory`
  check → copy-vs-view, which also **resolves §4**); `t_proc_end`. This is the **differentiating mutate step,
  measured identically in both**.

**Markers — arrow-only fine transfer segments (`FlightCore`, all Python):** `put` (producer store; expect ~0),
`alloc` (`allocate_buffer`), `rpc` (`do_action`), `parse` (`open_stream().read_all()`), `delete`
(`send_delete_rpc`). **Split `rpc` → rpc-overhead vs `syscall`:** `_handle_scatter_write` returns its
`vm_scatter_write` ns in the `do_action` result; consumer computes `rpc_overhead = rpc − syscall`.

**Plasma transfer = coarse (acknowledged asymmetry).** Plasma's produce-serialize copy lives in Ray C++
(`store_task_outputs`), not Python-timeable → it sits inside the `t_proc_start − t_make_end` gap (serialize +
schedule + resolve), a black box. Acceptable because the *differentiating* copy (mutate) is measured precisely in
both, and the transfer is ~1 copy either way.

**Output / activation.** Env-gated `RAY_FLIGHT_TIMING=1`, read once at import (hot path untouched when off).
Accumulate per-segment count/mean/min/max/p50/p99 in a `FlightCore` module global + the bench actors' own stamps;
log a per-run summary.

**Staged.** (1) fetch + mutate breakdown for the focus case; if the summed stages account for the
arrow-vs-plasma total, done. (2) Only if they don't, widen to the macro produce/put/consume split. Then sweep
100 → 10 → 1 MB: `alloc` should scale with size if it's page-faults; fixed RPC overhead shouldn't — that's the tell.

**Still-open decisions:** output = running aggregates [default] vs raw per-fetch file; do the rpc/syscall
producer-return split now [recommended]. Everything else above is settled.

---

## 6. Status / changelog

**📌 Current state & resume points → see [§0 START HERE](#0-start-here--session-resume-updated-2026-06-16) at the top of this file.**

- **2026-06-10** — Created this doc. Read PR #62772 end-to-end; recorded architecture + background
  primer (Arrow IPC, Arrow Flight, `process_vm_readv/writev`). Branch is clean (none of the PR code
  present). Next: decide direction (see §4).
- **2026-06-10** — Added [`BACKGROUND.md`](./BACKGROUND.md): tutorial-style primer with the
  `buf`/`open_stream`-who-calls-when Q&A and an annotated `_RecordingSink` walkthrough.
- **2026-06-11** — Worktree renamed `non-rdt-arrow-prototype` → `arrow-prototype` (old worktree
  removed; both docs carried over to the new one). Deep-dived the shared engine: Arrow in-mem vs IPC
  layout, position-independent (relative-offset) metadata, `_RecordingSink` no-bulk-copy + `_refs`
  pinning, `process_vm_*` single-copy semantics + readv/writev push-vs-pull, per-process `FlightCore`,
  how the consumer learns `pid`/`flight_uri`, `FlightObjectStore` adapter, native-vs-RDT file reading
  guide (§3.7), and `CoreWorker.store_task_output` reuse.
- **2026-06-12** — Added §3.8: full RDT end-to-end trace (submit → produce → owner-metadata-callback →
  consume via ray.get or actor-arg → GC), anchored to the framework + `ArrowFlightTransport` plugin,
  with the vs-native contrast.
- **2026-06-13** — §3.8 Phase 1: traced `actor.py` → `SubmitActorTask` and the RDT touch-points that
  straddle it. Added §3.8.1: RDT vs traditional args = *same control plane, added data plane* (RDT ref
  still resolves normally; `InlineDependencies` carve-out keeps ref + stamps transport; bulk tensors are
  a parallel out-of-band producer→consumer channel off the same reply; join on the consumer).
- **2026-06-13** — Discovered the PR is APPLIED in this worktree (commit `dc2ae38801`) — corrected the
  stale "not on branch" notes (§3 intro, §3.8). Added §3.9: NATIVE consumer-side fetch deep trace
  (`execute_task` arg-deser → `_deserialize_object` FLIGHT_TABLE branch → `fetch_via_vm`/`fetch_via_flight`),
  incl. synchronous-not-pipelined and the delete-on-first-read (no-fan-out) limitation.
- **2026-06-14** — Recorded 3 design points in §3.9: (1) deletion is consumer-triggered / producer passive
  (vs RDT & plasma owner-ref-counting) → new §4 lifetime question + plasma-move-semantics cross-ref;
  (2) no pipelining; (3) scatter-list built eager-at-`put` (cost-timing choice). Pinned the end goal:
  **Ray Data single-consumer pipeline** (§1), which makes delete-on-first-read acceptable.
- **2026-06-14** — Framed the broader investigation (§1 + §4): this worktree + `plasma-move-semantics` are
  two halves of one study — compare the full 2×2 {plasma | bypass-plasma} × {copy | move} for a Ray Data
  single-consumer pipeline; microbenchmark + evaluate after the RDT flow is understood.
- **2026-06-14** — Corrected the 2×2 (§1): native flight is bypass+**move** (consumer delete-on-first-read =
  eager producer free, = plasma-move's free-on-push-ack idea), RDT Arrow is bypass+**copy** (owner ref-count
  free, fan-out). Grounded "move" in `plasma-move-semantics/PLASMA_MOVE_SEMANTICS.md:16` (eager reclamation,
  not copy-avoidance — transfer is single-copy in all cells). All 4 cells already have an implementation.
- **2026-06-14** — §3.8: added a short one-sided/two-sided + `__ray_call__`/`__ray_send__`/`__ray_recv__`
  primer, and §3.8.2 "how `ArrowFlightTransport` plugs in" (registry-by-name + `TensorTransportMetadata`
  subclass; both ends bottom out in `FlightCore`).
- **2026-06-15** — §3.8 Phase 1 "Gory detail": `tensor_transport` as an opaque `optional<string>` through
  C++ (Cython `optional[c_string]` → `CTaskOptions` → `TaskSpecBuilder set_tensor_transport` `task_util.h:326`
  → TaskSpec field 44 → executor `store_task_outputs`); task-field vs arg-ref-field distinction; `add_rdt_ref`
  `RDTMeta` field breakdown (keyed by `obj_ref.hex()`, `meta=None` until the Phase-3 callback).
- **2026-06-15** — Paused RDT learning (resume marker at top of §5/§6). Pivoted to benchmarks: added §5
  "Benchmark plan" (extend `bench_flight_store.py`; throughput+latency; cell→build mapping; macOS/Linux +
  rebuild caveats; deferred-run checklist). Established runtime reality: active `ray` ≠ this worktree → rebuild
  needed; macOS = Flight-gRPC variants only (no `process_vm` fast path).
- **2026-06-15** — Chose run approach: **one PR image + `--mode`** (covers plasma/native/RDT, no script
  changes); **`plasma+move` deferred** to a 2nd image (+ peak-memory) later. Added the image/flag matrix
  (native = `RAY_USE_FLIGHT_NATIVE=1` env; RDT = `@ray.method(tensor_transport="ARROW_FLIGHT")` decorator,
  not an image flag) and a `_enable_ptrace()` explainer (Yama `ptrace_scope=0` for sibling `process_vm_writev`).
- **2026-06-15** — First Anyscale runs (image built from branch; PR build confirmed). Native `verify`: same-node
  cases EPERM'd → `verify` lacks `_enable_ptrace()` → set `ptrace_scope=0` cluster-wide → native green. RDT
  `verify`: same-node transfers timed out (60s) → monitor killed actors → cascade; root cause = the **receiver
  Consumer lacked `enable_tensor_transport=True`** (no `_ray_system` group for `__ray_recv__`). Patched the
  Consumers in `proto/verify_flight_store.py` + `proto/bench_flight_store.py`; recorded both gotchas in §5.
- **2026-06-16** — First `bench` runs (same-node, 1 pair / concurrency 1, serial). Added §5.1 (harness reference +
  all params) and §5.2 (first results: RDT slowest, plasma strong same-node — flagged REVISIT, not a verdict).
  **Added §0 "START HERE" resume anchor** (single source of truth for state + next steps) and collapsed the
  mid-file state block into a pointer to it. **Session paused for resume** — next session: read this file + `BACKGROUND.md`.
- **2026-06-17** — Added §3.10: analytical **copy-count model** (plasma vs arrow × same/cross-node ×
  read-only/read-mod-write; arrow = X-1 all cells; plasma = X / 2X-1 / 2X-1 / **3X-2** — corrected cross-node
  RMW from 2X-1, since within plasma RMW must cost X-1 more than RO) + **large-table same-node-gap diagnosis**
  from a chat with Ed: the `put` walk is a wash (plasma walks+copies fused, arrow walks-only, O(#buffers) not
  O(#bytes)); the real gap is **reader-side per-fetch `pa.allocate_buffer`** (plasma amortizes via startup
  `mmap`), which a **reader-side buffer-reuse cache** closes to ~parity (Ed verified); arrow loses only under
  fan-out (multi-reader), ~matches single-reader (our Ray Data case). The page-fault *mechanism* for the gap is
  tagged `(verify)` — to confirm with Ed. Flagged the buffer-pool harness fix as a prerequisite for any
  same-node verdict (§0/§5/§5.2).
- **2026-06-17** — Ran the **full 2×2×3 grid** on Anyscale (§5.3): {same/cross-node} × {read-only/modify} ×
  {ray/native/rdt} × 1/10/100 MB, serial. **plasma wins 10/12**; native wins only same-node 1 MB; RDT slowest.
  Predicted arrow wins (modify, cross-node) **don't materialize** — arrow carries a ~50 ms reader-side allocation
  deficit at 100 MB (Ed's caching point) ⇒ **buffer pool is the gating fix** (decisive re-run = pool +
  modify/cross-node; modify-cell meaning itself is an open question, §4). Also fixed the latency timer in
  `bench_flight_store.py` (`fill()`): stamp moved **before** the producer submit (was after both `.remote()`s —
  excluded submit overhead, slightly favored RDT). Same-node RO reproduced §5.2 → harness stable.
- **2026-06-17** — Extended §3.10 with the **X-scaling / per-edge `a` vs `c`** analysis: copy savings grow with X
  but arrow's per-transfer overhead `a` scales with the X-1 edges too ⇒ a per-edge sign test (`a<c` cases 2/3,
  `a<2c` case 4), not accumulation. §5.3 ⇒ `a≈54 ms ≈ 5c` at 100 MB ⇒ deeper pipeline currently *hurts* arrow;
  the buffer pool (drives `a`→~0) is the precondition for the X-scaling win. (`a≈54 ms` is from same-node
  **read-only** — a copy-count tie — independent of any modify-consumer behavior.) Expanded §5.1 into a full
  component walkthrough; added the §3.9 pipelining note (dormant at max-in-flight=1; native needs concurrency>1
  to overlap → sweep must use it). The `--consumer-mode modify` two-liner is **flagged to revisit** (§4); §5.3
  modify cells are data only, uninterpreted.
- **2026-06-18** — Added `proto/run_2x2_bench.sh` (the grid runner used for §5.3; launched detached on Anyscale,
  polled for `ALL_DONE`).
- **2026-07-01** — Added `--consumer-output {num-rows, table}` to `bench_flight_store.py` (orthogonal to
  `--consumer-mode`): `table` republishes the consumer's output = the extra produce copy, enabling the
  read-modify-**write** / diff-2 case; `num-rows` = the diff-1 read-modify case. Documented in §5.1. Flagged in
  §5.3 that the §5.3 grid used `num-rows` (**no consumer produce**), so the §3.10 `2X-1`/`3X-2` formula (which
  assumes X produces, incl. the terminal consumer) is **not directly applicable** — the copy gap there is 1, not
  2; the formula's full gap needs `--consumer-output table`.
- **2026-07-01** — Added **§5.4 profiling plan** (design; markers not placed): symmetric per-stage timing of
  `arrow-native` vs `ray` (measure both, don't reconcile the 54 ms), focus case 100 MB modify same-node ×
  {`--consumer-output num-rows`, `table`}, `CLOCK_MONOTONIC` in-process stamps (sidesteps §5.1 driver-lag),
  FlightCore fine segments + rpc/syscall split, plasma transfer acknowledged coarse, env-gated aggregates,
  staged fetch-first-then-macro.
- **2026-06-30** — Noted a **latency-measurement caveat** in §5.1/§5.3 (not fixed, decide later): measured latency
  is `observe − submit` (driver stamps when its `wait_available` loop notices the ref ready, not when the consumer
  finished) → an **upper bound**. If the driver is busy submitting/bookkeeping or OS-descheduled at completion T, it
  records `T+δ − submit`. Small at `max_in_flight=1` (driver mostly parked in `ray.wait` → p99 only; §5.3 means/gap
  unaffected), material for the throughput sweep. Distinct from the `ray.wait` 1 ms timeout; fixes deferred.

---

## TEMP SCRATCH — avg latency only (remove after copy; data = §5.3, 2026-06-17)
<!-- TEMP: avg-only view of the §5.3 grid for copy/paste. Delete this whole section when done. -->

**Same-node — avg latency (ms)**
| | ray (plasma) | arrow-native | arrow-rdt |
|---|---|---|---|
| read-only 1 MB | 6.7 | **5.7** | 8.3 |
| read-only 10 MB | **37.1** | 38.6 | 40.7 |
| read-only 100 MB | **356.1** | 409.9 | 410.6 |
| modify 1 MB | 6.9 | **6.3** | 8.3 |
| modify 10 MB | **40.4** | 41.6 | 43.9 |
| modify 100 MB | **411.0** | 457.9 | 461.8 |

**Cross-node — avg latency (ms)**
| | ray (plasma) | arrow-native | arrow-rdt |
|---|---|---|---|
| read-only 1 MB | **10.5** | 13.4 | 24.7 |
| read-only 10 MB | **53.2** | 54.9 | 67.8 |
| read-only 100 MB | **452.7** | 598.3 | 611.9 |
| modify 1 MB | **10.9** | 16.2 | 27.7 |
| modify 10 MB | **56.8** | 64.9 | 73.1 |
| modify 100 MB | **505.0** | 648.8 | 666.3 |
