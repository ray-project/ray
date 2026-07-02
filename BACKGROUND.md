# Background primer — Arrow IPC, Arrow Flight, and `process_vm_*`

> Tutorial-style companion to `NON_RDT_ARROW_PROTOTYPE.md`. Read top-to-bottom.
> Goal: understand the three technologies the heap→heap Arrow prototype is built on, and how
> PR #62772 stitches them together. Each section ends with "how the PR uses it".

---

## Part 1 — How Arrow stores a table in memory

One fact underlies everything else: **Arrow stores a table column-by-column, as raw byte buffers.**

A table `{"id": [1,2,3], "value": [1.1, 2.2, 3.3]}` is **not** stored as rows. It's stored as
separate contiguous byte arrays ("buffers"):
- a buffer holding `1,2,3` (8 bytes each → 24 bytes),
- a buffer holding `1.1,2.2,3.3`,
- tiny "validity" bitmap buffers tracking which entries are null,
- (for strings/lists) an extra "offsets" buffer.

Vocabulary:
- **Buffer** — one contiguous raw byte array (e.g. one column's values).
- **RecordBatch** — a schema + the buffers for one chunk of rows.
- **Table** — a schema + one or more RecordBatches ("chunks") per column.

**The crucial property:** the bytes Arrow computes on in memory are *already* in the exact layout it
would send over a wire or write to disk. There is no "object → text → object" round trip like JSON
or pickle. **The in-memory form IS the serialized form.** This is what makes everything below cheap.

---

## Part 2 — Arrow IPC: the "serialization" format

**Arrow IPC** (Inter-Process Communication) is the standard way to turn Arrow data into a byte
stream for moving between processes/machines/files. Two variants: **stream** (what the PR uses) and
**file/Feather** (stream + a footer index for random access).

Because of Part 1, "serializing" is almost trivial. The IPC **stream** looks like:

```
[schema message]                          ← tiny: column names + types
[record-batch message: metadata header]   ← tiny (FlatBuffers): "buffer 0 @ offset 0 len 24,
                                                                  buffer 1 @ offset 24 len 24, ..."
[record-batch message: body]              ← the raw column buffers, back-to-back, padded
... (more batches) ...
```

- The **header** is just a *map* of where each buffer sits in the body. Small.
- The **body** is the column buffers copied verbatim. Big.
- **Writing**: `pyarrow.ipc.new_stream(sink, schema)`, then `writer.write_table(table)`.
- **Reading**: `pyarrow.ipc.open_stream(buf).read_all()` → reads the tiny header, then builds arrays
  that **point directly into `buf`** (zero-copy — it does not copy/parse the values).

### Q: What is `buf`? Who calls `open_stream`, and when?
- `buf` = **whatever holds the IPC stream bytes.** In general it can be a file, a stream, or an
  in-memory `pa.Buffer`. The PR uses an in-memory buffer.
- **It is NOT the end-user's buffer.** In the PR same-node path, `buf` is a buffer the **consumer's
  fetch code allocates for itself**: `local_buf = pa.allocate_buffer(size)` in `fetch_via_vm`
  (an empty buffer of exactly `ipc_size` bytes in the consumer process's heap).
- Sequence: consumer allocates `local_buf` → producer writes the IPC bytes into it (via syscall,
  Part 5) → **the consumer** calls `ipc.open_stream(local_buf).read_all()` → gets a `pa.Table`.
  So **who = the consumer process; when = right after the bytes land in its buffer.**
- Because the read is zero-copy, the returned table's columns *point into* `local_buf`, so the
  buffer must stay alive as long as the table is used — and it does (the table references it).
  The end-user (consumer actor code) never sees `local_buf`; they just get the table.
- **Cross-node path:** you won't *see* an explicit `open_stream` — `client.do_get(ticket).read_all()`
  reads the IPC stream off the gRPC connection internally and returns the table. Same idea; Flight
  handles the buffer part.

### Q: Is the in-memory table already stored in this IPC layout, or does serialize create it?
**Serialize (`new_stream`/`write_table`) creates the IPC structure; it does not pre-exist in memory.**
Split "the table" into three things:
| Aspect | in-memory `pa.Table` | IPC stream | same? |
|---|---|---|---|
| column **value bytes** | the raw buffers | the body | ✅ identical (no transcoding) |
| **contiguity** | separately-malloc'd, **scattered** (heap gaps) | **one contiguous** blob | ❌ made at serialize |
| **offset/length metadata** | none as a blob — just **pointers in the object tree** (ArrayData) | **FlatBuffers header** of relative offsets | ❌ **synthesized** by `write_table` |

In memory a table is a tree (Table → ChunkedArray → Array → ArrayData) whose buffers are independent
allocations scattered around the heap, located by actual pointers — there is no `[header][body]`
layout and no relative-offset map. `write_table` (1) synthesizes the FlatBuffers metadata and
(2) defines a contiguous body order (+padding), passing the value bytes through untouched.
Evidence (`/tmp/arrow_inmem_vs_ipc.py`): the table's column buffers sit at scattered addresses with
128/64-byte gaps; the IPC stream is one contiguous blob.

**Why this drives the prototype's design:**
- Scattered-in-memory → contiguous-IPC means you must **gather** the buffers. That's exactly the
  scatter-list + one `process_vm_writev` (name scattered sources, gather contiguously into the
  consumer's buffer).
- It explains `_RecordingSink`'s two branches: big column buffers **pre-exist** → arrive as
  `pa.Buffer` → record address (no copy); the small metadata/padding is **synthesized at serialize
  time** → arrives as `bytes` → must be materialized/copied (no pre-existing address to point at).

### Q: The IPC header says "where the buffers are" — doesn't copying it elsewhere break it?
**No — IPC metadata stores RELATIVE offsets, not absolute pointers.** For each buffer the header
records an `(offset, length)` *relative to the start of the message body* (the body = all buffers
concatenated right after the metadata). It never stores a producer address. So the stream is
**position-independent**: correct at whatever base address it lands at. On read, the consumer
computes `actual_addr = local_buf_body_base + relative_offset`, i.e. pointers are recomputed locally
against the consumer's own buffer.

Two different "where the buffers are" — only the relative one travels:
| | stores | lives | travels? |
|---|---|---|---|
| **IPC header** (in stream) | each buffer's `(offset,length)` **relative to body** | in the serialized bytes | **yes**, position-independent |
| **scatter-list** (`_RecordingSink`) | each buffer's **absolute producer address** | producer only, transient | **no** — only the *source* for the `writev` copy, then discarded |

Proof (`/tmp/arrow_relocate_demo.py`): copy the IPC bytes to a brand-new buffer at a different
address → `open_stream(buf2).read_all()` still returns the correct table, with the `id` buffer at the
**same relative offset (376B)** but a new base. The scatter-write is just "reproduce the same byte
sequence at the consumer's base"; resolution against that base happens on read. (This is also why
Arrow IPC works to files and across machines.)

### Q: How does the PR exploit this? (the `_RecordingSink`)
Since "serializing" is just "write a tiny header, then copy the buffers as-is," you can record
*where the buffers already are* instead of copying them. See Part 3.

### Worked example (runnable, real output)
```python
import pyarrow as pa
import pyarrow.ipc as ipc

table = pa.table({"id": [1, 2, 3], "value": [1.1, 2.2, 3.3]})
print("table.nbytes:", table.nbytes)            # 48  = 24B (id) + 24B (value)

# WRITE: new_stream(sink, schema) + write_table. Normal sink copies bytes into buf.
# (The prototype swaps THIS sink for _RecordingSink, which copies nothing — see Part 3.)
sink = pa.BufferOutputStream()
w = ipc.new_stream(sink, table.schema); w.write_table(table); w.close()
buf = sink.getvalue()                            # buf = pa.Buffer of IPC bytes
print("IPC size:", buf.size)                     # 432 = framing/header + 48B body + padding

# READ: open_stream(buf) parses header; read_all() wraps buf's memory (zero-copy).
table2 = ipc.open_stream(buf).read_all()
print("equals?", table2.equals(table))           # True

# Zero-copy proof: the read-back column's bytes live INSIDE buf.
vb = table2.column("id").chunk(0).buffers()[1]    # [0]=validity, [1]=values
print(buf.address <= vb.address < buf.address + buf.size)   # True
```
```
table.nbytes: 48
IPC size: 432            # 432 >> 48: for a tiny table, fixed framing overhead dominates;
equals? True             #   for a 100MB table it's a rounding error (body dominates).
True                     # id column points at an offset inside buf -> read_all did NOT copy.
```
**Takeaways:** (1) IPC = tiny header + raw column buffers (+ padding). (2) `read_all()` is
zero-copy — columns point into `buf`, so `buf` must outlive the table. (3) The prototype changes
exactly **one line** — the *write* sink (`BufferOutputStream` → `_RecordingSink`); the consumer's
*read* (`open_stream(local_buf).read_all()`) is identical to this demo, just with `local_buf` filled
by `process_vm_writev` instead of by `BufferOutputStream`.

**`write_table` copies (and why `_RecordingSink` exists):** with `BufferOutputStream`, `write_table`
*memcpy*s the column bytes into `buf`. Proof — three distinct addresses for the same `id` data:
```
original table  id-values buffer @ 0x3ace00300c0     # the table's own memory  (NOT in buf)
buf range                          [0x3ace0040080, 0x3ace0040230)
read-back table id-values buffer @ 0x3ace00401f8     # a COPY, living inside buf
```
Counting copies of the bulk column data:
- `BufferOutputStream`: **copy #1** table buffer → `buf` (serialize), then **copy #2** `buf` →
  shared-mem/socket → consumer (transport).
- `_RecordingSink` + `process_vm_writev` (same-node): **no serialize copy** — the sink records the
  address `0x3ace00300c0` (size 24) into the scatter-list and never builds `buf`; the kernel does
  **one** copy, producer's live buffer → consumer's buffer. (Tiny IPC headers are still copied into
  owned buffers — only the big column bodies are spared.)

---

## Part 3 — `_RecordingSink` (the zero-materialization trick)

A **sink** is a file-like object you write bytes to (has `.write()`, `.tell()`, `.flush()`…).
PyArrow's IPC writer doesn't care if it's a real file or a fake one — it just calls `.write(chunk)`
repeatedly. `_RecordingSink` is a **fake sink that records pointers instead of copying bytes.**

How it's driven (`flight_core.py`):
```python
def _serialize_to_recording_sink(table):
    sink   = _RecordingSink()
    pf     = pa.PythonFile(sink, mode="w")     # wrap fake sink as an Arrow output stream
    writer = ipc.new_stream(pf, table.schema)  # the normal IPC stream writer
    writer.write_table(table)                  # writer emits schema + headers + column buffers,
    writer.close()                             #   each as a sink.write(...) call
    return sink
```

The sink, annotated:
```python
class _RecordingSink:
    def __init__(self):
        self._chunks = []   # the scatter-list: ordered (address, size) pairs
        self._refs   = []   # keep underlying memory alive so addresses stay valid
        self._offset = 0    # running total = final IPC stream size

    def write(self, data):
        if isinstance(data, pa.Buffer):
            # Big column body buffer. PyArrow hands us the REAL buffer object (zero-copy),
            # so we just note where it already lives. NO COPY.
            self._chunks.append((data.address, data.size))
            self._refs.append(data)               # pin it alive
            self._offset += data.size
            return data.size
        # Small chunk (IPC metadata header / padding) arrives as plain bytes.
        b   = bytes(data)
        buf = pa.py_buffer(b)                      # copy these FEW bytes into an owned buffer
        self._chunks.append((buf.address, len(b)))
        self._refs.append(buf)
        self._offset += len(b)
        return len(b)

    def tell(self): return self._offset            # total IPC size
    @property
    def scatter_list(self): return self._chunks     # the recipe handed to process_vm_writev
```

**The key move:** when the IPC writer emits a **large column buffer**, PyArrow passes the actual
`pa.Buffer` (already in the producer's heap) to `.write()` rather than pre-copying it to bytes. The
sink seizes this and records only `(address, size)` — **the big data is never copied or touched.**
Only the **tiny** metadata headers (a few hundred bytes/batch) get copied into small owned buffers.

Result of one `put()`:
- `scatter_list` = ordered `(addr, size)` regions which, **concatenated in order, equal the exact IPC
  stream byte-for-byte.**
- `tell()` = total size the consumer must allocate.
- `_refs` pins everything alive so addresses stay valid while the table is stored.

#### What `_refs` is for (and why `append` doesn't copy)
The two fields are a **pair**:
- `_chunks` (scatter-list) stores `(data.address, data.size)` — and `data.address` is a **bare Python
  int** (e.g. `0x460180300c0`). An integer keeps *nothing* alive.
- `_refs` stores the actual **buffer objects**.

Python frees an object when its refcount hits 0. If only the addresses were kept, the `pa.Buffer`
objects would be GC'd, their memory freed/reused, and the recorded addresses would **dangle** →
`process_vm_writev` later reads garbage. `_refs` holds a live reference to each buffer so refcount
stays > 0 and the **memory (hence the addresses) stays valid** for the sink's lifetime. That's
"pinning" — not locking pages, just *keeping a Python reference so GC won't free the memory the
addresses point into*. Pairing: **`_chunks` = what to copy; `_refs` = keep those addresses valid.**

`self._refs.append(data)` does **not** copy — Python `list.append` stores a *reference* to the same
object and bumps its refcount (verified: `refs[0] is buf` → True, address unchanged,
`sys.getrefcount` rises). The only copy in the sink is `b = bytes(data)` in the *bytes* branch — a
deliberate snapshot of the small metadata; the append is still just pinning.

**Why `_refs` is essential (not redundant):** the table's own column buffers are also kept alive by
`FlightCore._tables[key]`, but the bytes-branch **metadata** buffers (`pa.py_buffer(b)`) are *not*
part of the table — `_refs` is the *only* thing keeping them alive. So `_refs` is the uniform
guarantee covering *all* scatter-list entries. Lifecycle: sink cached in `_sinks[key]` at `put()`,
released on `delete(key)`/eviction → refcounts fall → memory freed.

#### Q: Isn't there a copy when recording (the table is scattered, the IPC stream is contiguous)?
**No — recording does NOT copy the bulk data, and the sink never holds a contiguous table.** "The
table in the sink" doesn't exist: the sink holds only `_chunks` (addresses of the **original
scattered** column buffers) + `_refs`. The bulk data stays scattered, in place. Contiguity is:
1. **logical** on the producer — the scatter-list is just the *order* to concatenate; nothing is
   concatenated, and
2. **physical only on the consumer** — `process_vm_writev` gathers the scattered sources into the
   consumer's contiguous buffer. That is the single producer→consumer copy of the bulk data.

| copied at record time? | |
|---|---|
| **bulk column buffers** | **No** — sink records existing addresses; verified identical to the table's own scattered buffers, with heap gaps between them (`/tmp/arrow_sink_probe.py`) |
| **IPC metadata/headers** | **Yes (tiny)** — synthesized during serialization, doesn't pre-exist, so materialized via the bytes branch |

The claim *"Arrow makes a contiguous copy of the table"* is true **only for a normal
`BufferOutputStream`** (it assembles scattered buffers into one contiguous `buf`, on the producer).
`_RecordingSink` is the swap that **avoids that producer-side copy** — "make it contiguous" is
deferred to the consumer's scatter-gather `writev`, the only bulk copy in the path.

**The metadata offsets assume the contiguous body — same as `BufferOutputStream`.** The IPC writer
computes each buffer's `(offset, length)` as positions in a contiguous body, and the recording sink
records that *same* metadata. Two coordinate systems coexist: **metadata offsets** = positions in the
assembled contiguous body; **scatter-list addresses** = the producer's actual scattered source
locations. `writev` maps source→layout: it reads the scattered addresses and writes them contiguously
in order, realizing exactly the layout the offsets describe. Verified: concatenating the scatter-list
pieces *in order* reproduces the `BufferOutputStream` stream **byte-for-byte** (568==568). So the
scatter-list *is* the contiguous stream, stored as "copy these regions in this order" rather than
pre-assembled; `BufferOutputStream` assembles it on the producer, the sink path on the consumer —
same bytes, same metadata, same offsets, only *where/when* differs.

That `scatter_list` is *exactly* the `local_iov[]` array `process_vm_writev` wants (Part 5). So on
each fetch the producer hands the cached scatter-list straight to the syscall — **it never
re-serializes.** Build the recipe once at `put` time; replay cheaply on each fetch.

> ⚠️ **Correctness invariant of the two branches** (subtle — read this):
> - **bytes branch** (small metadata/headers): `b = bytes(data)` copies **synchronously inside
>   `write()`**, before the call returns. So even if PyArrow reuses its internal scratch afterward,
>   we already snapshotted → **always safe.**
> - **pa.Buffer branch** (big column bodies): records `(address, size)` and copies **nothing**. This
>   is safe **only because** the `pa.Buffer` aliases the table's **own immutable, distinct,
>   ref-counted** column memory (pinned by `_refs`, and the whole table is also kept alive via
>   `FlightCore._tables[key]`). Immutable + alive + distinct ⇒ aliasing by address is safe.
> - **The hazard:** if a future PyArrow handed this branch a **reused, mutable** `pa.Buffer` (fill →
>   `write` → refill same buffer → `write`), every scatter-list entry would point at the same address
>   holding the *last* chunk's bytes → **silent corruption** (all chunks alias the last one). The sink
>   has no code defense against this; it relies on the immutability/distinctness invariant.
> - **Empirically verified (PyArrow 19.0.1, `/tmp/arrow_sink_probe.py`):** column bodies arrive as
>   `pa.Buffer` whose addresses *exactly match the table's own buffers*, one per chunk, **no address
>   reused** across a 2-chunk table; metadata arrives as `bytes`. So the invariant holds today.
>   `proto/verify_flight_store.py` (multi-chunk, many-column, `equals` checks) is the regression guard
>   if a PyArrow upgrade ever breaks it.
> - The original worry — PyArrow copying bodies to **bytes** first — would merely lose the zero-copy
>   *perf* (bytes branch stays correct). The dangerous case is reuse of a *mutable pa.Buffer*, above.

---

## Part 4 — Arrow Flight: Arrow's network RPC

**RPC** = Remote Procedure Call: "I call a function, but it runs in another process/machine; args +
return travel over the network." **gRPC** is Google's popular RPC system (Ray already uses it
heavily).

**Arrow Flight is a gRPC service purpose-built for shipping Arrow data.** Control messages (which
dataset, tickets) are normal gRPC/protobuf; the bulk data travels as **Arrow IPC batches** — so no
slow row-by-row serialization. Think "HTTP for Arrow tables."

Standard verbs (the PR uses only the first two):
- **`DoGet(ticket) -> stream<RecordBatch>`** — "download." Client presents an opaque **ticket**
  (bytes meaning "the table called X"); server streams Arrow batches back.
  → **the PR's cross-node data transfer.**
- **`DoAction(type, body) -> stream<result>`** — "run a custom command." Generic: a type string +
  arbitrary bytes. → **the PR's doorbell/control channel** (`"scatter_write_vm"`, `"delete"`); no
  table data flows through it.
- `DoPut`, `GetFlightInfo`, `ListFlights`, `DoExchange` — not central here.

**In the PR**, every worker process runs **one Flight server** (`FlightCore`), used two ways:
1. **Real data pipe** (cross-node): `do_get` returns `RecordBatchStream(table)`; gRPC streams it.
2. **Just a doorbell** (same-node): consumer calls `do_action("scatter_write_vm", {pid, addr, size})`
   to *ask* the producer to copy data into the consumer's memory via the syscall — the **table bytes
   do NOT travel through Flight** in this case.

---

## Part 5 — `process_vm_readv` / `process_vm_writev`: direct cross-process memory copy

Normally the OS isolates processes — they can't touch each other's memory, so they share via pipes,
sockets, or shared-memory files (plasma uses the last), all of which copy data through an
intermediary.

Two Linux syscalls (kernel ≥ 3.2) skip the middleman: **`process_vm_readv`** and
**`process_vm_writev`**. They tell the kernel "copy these bytes straight from process A's address
space into process B's." One syscall, one kernel-mediated copy — no socket, no shared file, no
serialization.

> **⚠️ These syscalls are NOT zero-copy, NOT shared memory, NOT copy-on-write.** They perform
> **exactly one eager copy**: afterward, producer and consumer hold two *independent* copies in two
> different physical locations (mutating one does not affect the other). It's a `memcpy` that crosses
> a process boundary.
>
> **Why userspace can't do it / how the kernel can:** each process has its own virtual address space;
> the MMU translates virtual→physical via *that process's* page tables, so a producer pointer is
> meaningless in the consumer. The kernel (running these syscalls) can access *any* process's page
> tables: it (1) checks ptrace permission, (2) pins the remote process's physical pages
> (`get_user_pages_remote`), (3) copies directly between source and destination physical pages — one
> pass, **no intermediate kernel buffer** (unlike a pipe/socket, which copies user→kernel→user = 2
> copies). The `iovec` vector gathers many scattered source regions into one contiguous destination
> in a single syscall.
>
> **Mechanism comparison:**
> | Mechanism | Physical pages | Copies to transfer | Notes |
> |---|---|---|---|
> | shared memory (`mmap MAP_SHARED`, plasma `/dev/shm`) | same, both mapped | 0 | but must *fill* it first (1 copy in); usually immutable to consumer |
> | fork / COW | same, RO, copied lazily on write | 0 until write | parent→child inheritance, not a transfer between running procs |
> | pipe / socket | separate | 2 (user→kernel→user) | + per-chunk syscall/framing overhead |
> | **`process_vm_writev`** | **separate** | **1** (direct, no kernel buf) | result is **private + mutable** to consumer |
>
> **So where is the "zero-copy" in this design?** NOT in the transfer. It's the two steps around it:
> `_RecordingSink` serialize (0 copy), the move (**1 copy — unavoidable**, bytes must physically get
> from A's RAM to B's RAM), `open_stream().read_all()` deserialize (0 copy). The honest label for the
> same-node path is **"single-copy"**, not "zero-copy". Win vs socket = 1 copy not 2 + one syscall;
> win vs plasma = no shared segment to manage, no store daemon, and a **private mutable** result
> (what the benchmark's "modify" mode exploits — plasma's shared memory is immutable, so mutating
> forces an extra copy the VM path avoids).

```c
process_vm_writev(pid,                   // the OTHER process
                  local_iov,  liovcnt,   // (pointer,len) regions in MY memory
                  remote_iov, riovcnt,   // (pointer,len) regions in the OTHER process
                  flags);                // 0
//  process_vm_readv has the identical shape.
```
- `struct iovec { void *iov_base; size_t iov_len; }` — just (pointer, length).
- The **`v` = vector** → **scatter/gather**: list MANY regions in one call. Total length on both
  sides must match.
- `local_iov[]` = **the calling process**; `pid` + `remote_iov[]` = **the other process**.
  `read`/`write` is named from the **caller's** view:
  - `process_vm_readv`  → copies **remote → local** = caller **pulls** data out of the target.
  - `process_vm_writev` → copies **local → remote** = caller **pushes** data into the target.
  - Either direction moves the same bytes; they differ in *which* process makes the syscall (and
    thus needs ptrace permission over the other).
- **The PR uses `writev` (producer-push):** the consumer rings the Flight doorbell with its `{pid,
  buffer_addr, size}`; the **producer** calls the syscall, `local_iov[]` = its cached scatter-list,
  `remote_iov[]` = the consumer's one buffer. Push is chosen because the scatter-list already lives
  on the producer (a pull would force shipping all the producer's buffer addresses to the consumer +
  leak its layout). The Cython `vm_read`/`vm_read_into_arrow_buffer` (read/pull) wrappers exist but
  are **unused leftovers**.

**The PR's same-node fast path, end to end:**
1. Consumer wants table `X`; reads its coordinate dict `{producer_pid, flight_uri, key, ipc_size}`.
2. Consumer allocates ONE empty buffer of `ipc_size`, notes its address, rings the doorbell:
   `DoAction("scatter_write_vm", {key, my_pid, my_buffer_addr, ipc_size})`.
3. **The producer** receives it and calls `process_vm_writev`:
   - `local_iov[]`  = the **scatter-list** (one entry per live column buffer + tiny headers,
     scattered around the producer's heap) — i.e. `sink.scatter_list`.
   - `remote_iov[]` = **one** entry = the consumer's single buffer (`my_pid`, `my_buffer_addr`,
     `ipc_size`).
   - The kernel copies all those scattered producer buffers, laid down contiguously, into the
     consumer's one buffer — **in one syscall.**
4. The consumer's buffer now holds a byte-perfect IPC stream → `ipc.open_stream(buf).read_all()`
   → the table back, zero-copy.

Direction subtlety: **the producer calls the syscall and *writes into* the consumer.** The consumer
just provides an empty buffer and rings the bell.

Cost: **one direct memory copy, producer heap → consumer heap.** Plasma instead does
serialize-into-shared-memory + seal + consumer-map/copy-out → that's the speedup. Bonus: the
consumer's buffer is **mutable**, so in-place column edits are zero-copy; plasma shared memory is
immutable, forcing a copy on mutate.

### ⚠️ Hard constraints (these bite in practice)
- **Linux only.** macOS/Windows have no such syscall → the C++ shim returns `ENOSYS`.
  **Our dev box is macOS — this fast path cannot run locally**; only plasma + the cross-node Flight
  gRPC path work on a Mac. Need a Linux box/cluster to exercise it. (Same-node check in code:
  `node_id == my_node and sys.platform == "linux"`.)
- **Permission (ptrace / Yama):** writing another process's memory needs ptrace permission, gated by
  `/proc/sys/kernel/yama/ptrace_scope`: `0`=any same-uid process (what the prototype forces via
  `_enable_ptrace()`, needs sudo), `1`=parent-only (common default), `2`=admin-only, `3`=disabled.
  Also requires **same uid** and **same PID namespace** (matters in containers).
- **Same machine only** (process↔process on one host) → hence the cross-node Flight fallback.

---

## Part 6 — How they stack

```
Arrow in-memory buffers  ──IPC format──▶  byte stream (tiny header + raw buffers)
                                              │
              ┌───────────────────────────────┴────────────────────────────┐
   same node (Linux)                                          different nodes
   process_vm_writev: copy producer's                        Arrow Flight DoGet:
   live buffers straight into the consumer's                 stream IPC batches
   pre-allocated buffer (1 syscall, no copy                  over gRPC, consumer
   to a socket); Flight DoAction is only                     read_all()s the table
   the doorbell. Recipe = _RecordingSink.scatter_list
```

- **Arrow IPC** = the byte-layout (header + buffers).
- **`_RecordingSink`** = builds a *recipe* (scatter-list) of where those bytes already live, without
  copying them.
- **Arrow Flight** = the RPC server every worker runs: real data pipe cross-node, doorbell same-node.
- **`process_vm_writev`** = the kernel one-shot copy that replays the recipe straight into the
  consumer's buffer, same-node.

---

## Glossary
- **buffer** — a contiguous raw byte array; Arrow's storage unit for a column's bytes.
- **RecordBatch / Table** — schema + buffers for a chunk of rows / one-or-more chunks per column.
- **IPC** — Arrow's byte-stream serialization format (header map + raw buffer body).
- **sink** — a file-like write target (`.write/.tell/.flush`); `_RecordingSink` is a fake one.
- **scatter-list / scatter-gather** — list of `(addr, size)` regions moved in one `*_vm_*` syscall.
- **Flight** — Arrow's gRPC-based RPC for moving Arrow data (`DoGet`, `DoAction`, …).
- **gRPC** — Google's RPC framework (control plane under Flight; also used throughout Ray).
- **`process_vm_readv/writev`** — Linux syscalls copying memory directly between two processes.
- **ptrace_scope** — Linux/Yama knob gating one process's permission to read/write another's memory.
- **plasma** — Ray's default shared-memory object store (the thing we're bypassing).
