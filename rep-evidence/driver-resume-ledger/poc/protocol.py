"""The progress-ledger protocol: M1 epoch fencing, M2 commit coalescing, M3 compaction.

The coordinator is written **once**, as a generator of key-value operations.  It
yields an operation and is sent the result.  That shape is what lets the checker
suspend it, crash it, duplicate it or interleave it at any operation boundary
without the protocol knowing anything about the experiment being run on it.

Negative controls are produced by flipping named flags on :class:`Cfg`, never by
forking this file.  A control that has drifted from the real protocol is testing
a strawman, and the whole argument for the rig collapses.

Startup sequence (the part that has to be crash-safe):

    S1  keys(epoch/)                        -> highest claimed epoch k
    S2  put(epoch/k+1, my_id, overwrite=F)  -> may be a lost-ack retry
    S3  get(epoch/k+1)                      -> READ-BACK; != my_id => I lost
    S4  scan epochs descending for the highest readable state
    S5  put(state/k+1/base@n, carried)      -> carry forward into MY epoch
    S6  put(state/k+1/base_ptr, n)          -> LINEARIZATION POINT
    S7  delete everything below epoch k+1   -> sweep; MUST be after S6

Steady state:

    R1  put(state/k+1/seg/i, batch)         -> ack => units durably committed
    C1  put(state/k+1/base@m, merged)       -> fresh unique base, never in place
    C2  put(state/k+1/base_ptr, m)          -> LINEARIZATION POINT
    C3  delete seg/<=m and base@<m          -> idempotent, re-runnable
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Sequence, Tuple

# --------------------------------------------------------------------------
# operations the coordinator can yield
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Put:
    key: str
    value: Any
    overwrite: bool = True


@dataclass(frozen=True)
class Get:
    key: str


@dataclass(frozen=True)
class Delete:
    key: str


@dataclass(frozen=True)
class Keys:
    prefix: str


@dataclass(frozen=True)
class Ack:
    """Not a KV operation.

    The point at which the application is told "these units are durably
    committed".  Everything after this must never lose them.
    """

    units: Tuple[int, ...]


@dataclass(frozen=True)
class Exec:
    """The unit's real side effect happens here.

    Not a KV operation.  Counted so the rig can measure *redo* -- how much work
    a crash costs -- which is the only observable that separates a correct
    resume from one that silently starts over.
    """

    unit: int


@dataclass(frozen=True)
class Abort:
    reason: str


@dataclass(frozen=True)
class Note:
    text: str


MUTATIONS = (Put, Delete)


# --------------------------------------------------------------------------
# configuration + variant flags
# --------------------------------------------------------------------------


@dataclass
class Cfg:
    session: str = "session-A"
    job: str = "job1"
    n_units: int = 6
    W: int = 1  # M2 coalescing window: commit every W completed units
    K: int = 2  # M3: compact every K segments

    # --- negative-control flags.  All defaults describe the REAL protocol. ---

    #: readback (real) | raw | inverted | ignore
    fence_mode: str = "readback"
    #: False => one shared state keyspace across epochs (NC3)
    epoch_scoped_state: bool = True
    #: True => startup sweep runs before the carry-forward pointer flip (NC4)
    sweep_before_ptr: bool = False
    #: True => compaction deletes segments before flipping base_ptr (NC5)
    compact_delete_before_ptr: bool = False
    #: True => compaction overwrites a single `base` key in place (NC6)
    compact_in_place: bool = False
    #: True => compaction flips base_ptr before writing the base it names (NC10)
    compact_ptr_before_base: bool = False
    #: False => no startup sweep at all (NC7)
    startup_sweep: bool = True
    #: False => constructor seeds an empty base instead of carrying forward (NC8)
    carry_forward: bool = True
    #: True => carry-forward scans epochs from highest to lowest, which is the
    #: pre-iteration-5 order refuted by runs/20260910-043934-C13 (NC15)
    carry_forward_epochs_descending: bool = False
    #: True => carry-forward reads base_ptr FIRST and the segments last, which
    #: is the pre-iteration-3 order refuted by runs/20260910-042707-C1 (NC13)
    carry_forward_ptr_first: bool = False
    #: False => ledger keys are not scoped by session_name (NC9)
    session_scoped: bool = True
    #: True => resume re-executes every unit instead of reading the ledger
    #: (NC12).  Safe, and therefore invisible to every safety invariant -- it
    #: shows up only as redo, and only after a crash.
    resume_ignores_ledger: bool = False
    #: True => the application is told "committed" BEFORE the segment is durable
    #: (NC11).  Fails only under crash injection, which is what makes it the
    #: control that proves the crash injector works.
    ack_before_commit: bool = False


# --------------------------------------------------------------------------
# key layout
# --------------------------------------------------------------------------


def root(cfg: Cfg) -> str:
    return f"{cfg.session}/{cfg.job}" if cfg.session_scoped else cfg.job


def epoch_prefix(cfg: Cfg) -> str:
    return f"{root(cfg)}/epoch/"


def epoch_key(cfg: Cfg, e: int) -> str:
    return f"{epoch_prefix(cfg)}{e:04d}"


def state_root(cfg: Cfg) -> str:
    return f"{root(cfg)}/state/"


def state_prefix(cfg: Cfg, e: int) -> str:
    if not cfg.epoch_scoped_state:
        return f"{state_root(cfg)}shared/"
    return f"{state_root(cfg)}{e:04d}/"


def base_key(cfg: Cfg, e: int, n: int) -> str:
    if cfg.compact_in_place:
        return f"{state_prefix(cfg, e)}base"
    return f"{state_prefix(cfg, e)}base@{n:04d}"


def ptr_key(cfg: Cfg, e: int) -> str:
    return f"{state_prefix(cfg, e)}base_ptr"


def seg_key(cfg: Cfg, e: int, i: int) -> str:
    return f"{state_prefix(cfg, e)}seg/{i:04d}"


def epoch_of_state_key(cfg: Cfg, key: str) -> Optional[int]:
    """Which epoch does this state key belong to?  None if unscoped/unknown."""
    rest = key[len(state_root(cfg)) :]
    head = rest.split("/", 1)[0]
    try:
        return int(head)
    except ValueError:
        return None


def _seq_of(key: str) -> int:
    tail = key.rsplit("/", 1)[1]
    if tail.startswith("base@"):
        return int(tail[len("base@") :])
    return int(tail)


# --------------------------------------------------------------------------
# state
# --------------------------------------------------------------------------


def empty_state() -> dict:
    return {"seq": 0, "done": []}


def merge(a: dict, b: dict) -> dict:
    return {"seq": max(a["seq"], b["seq"]), "done": sorted(set(a["done"]) | set(b["done"]))}


# --------------------------------------------------------------------------
# the coordinator
# --------------------------------------------------------------------------


CARRY_FORWARD_RETRIES = 4


def _read_epoch_state(cfg: Cfg, e: int):
    """Read the committed state of epoch ``e``, or None if it has none.

    **Segments are read first and ``base_ptr`` last.**  This is not stylistic.
    The carry-forward read spans four separate, non-atomic operations, and a
    *fenced* coordinator -- one that a higher epoch has superseded but which has
    not noticed and is still compacting -- deletes segments as a matter of
    routine.  ``runs/20260910-042707-C1`` produced 627 counterexamples in which
    the old pointer-first order returned a pre-compaction base plus ``None`` for
    every segment the compaction had since deleted, losing everything in
    between.

    Reading the pointer last closes it. A segment ``i`` can only be deleted by a
    compaction at some ``m >= i``, and that compaction sets ``base_ptr = m``
    *before* deleting; so for any segment the reader misses, the pointer it then
    reads is already ``>= m >= i`` and ``base@p`` contains that segment's units
    by construction.  The residual case -- ``base@p`` itself deleted by a later
    compaction between the pointer read and the base read -- is closed by
    re-reading the pointer, bounded, since a fenced writer compacts finitely
    many times before it dies.
    """
    if cfg.carry_forward_ptr_first:
        # NC13: the refuted order, kept so the fix is measured against the
        # exact code that failed rather than a recollection of it.
        ptr = yield Get(ptr_key(cfg, e))
        if ptr is None:
            return None
        base = yield Get(base_key(cfg, e, ptr))
        if base is None:
            return None
        state = {"seq": base["seq"], "done": list(base["done"])}
        for k in (yield Keys(state_prefix(cfg, e) + "seg/")):
            i = _seq_of(k)
            if i > ptr:
                seg = yield Get(k)
                if seg is not None:
                    state = merge(state, seg)
        return state

    segs = {}
    for k in (yield Keys(state_prefix(cfg, e) + "seg/")):
        seg = yield Get(k)
        if seg is not None:
            segs[_seq_of(k)] = seg

    base = None
    ptr = None
    for _ in range(CARRY_FORWARD_RETRIES):
        ptr = yield Get(ptr_key(cfg, e))
        if ptr is None:
            return None
        base = yield Get(base_key(cfg, e, ptr))
        if base is not None:
            break
    if base is None:
        # Still torn after the retry budget.  Conservative: report the epoch
        # unreadable rather than returning a partial state.  If this ever
        # happens for real it shows up as an INV-DUR violation, which is the
        # outcome we want to be able to see.
        return None

    state = {"seq": base["seq"], "done": list(base["done"])}
    for i, seg in segs.items():
        if i > ptr:
            state = merge(state, seg)
    return state


def _commit(cfg: Cfg, e: int, seq: int, batch: Sequence[int]):
    """M2: make a batch of completed units durable, then tell the application.

    The ordering is the whole point.  ``ack_before_commit`` inverts it, which is
    a bug no crash-free test can see.
    """
    payload = {"seq": seq, "done": sorted(batch)}
    if cfg.ack_before_commit:
        yield Ack(tuple(batch))
        yield Put(seg_key(cfg, e, seq), payload)
    else:
        yield Put(seg_key(cfg, e, seq), payload)
        yield Ack(tuple(batch))


def _compact(cfg: Cfg, e: int, seq: int, done: Iterable[int]):
    payload = {"seq": seq, "done": sorted(done)}

    def delete_superseded():
        for k in (yield Keys(state_prefix(cfg, e))):
            tail = k.rsplit("/", 1)[1]
            if k.endswith("base_ptr"):
                continue
            if "/seg/" in k and _seq_of(k) <= seq:
                yield Delete(k)
            elif tail.startswith("base@") and _seq_of(k) < seq:
                yield Delete(k)

    if cfg.compact_ptr_before_base:
        yield Put(ptr_key(cfg, e), seq)
        yield Put(base_key(cfg, e, seq), payload)
        yield from delete_superseded()
    elif cfg.compact_delete_before_ptr:
        yield Put(base_key(cfg, e, seq), payload)
        yield from delete_superseded()
        yield Put(ptr_key(cfg, e), seq)
    else:
        yield Put(base_key(cfg, e, seq), payload)  # C1
        yield Put(ptr_key(cfg, e), seq)  # C2 -- linearization point
        yield from delete_superseded()  # C3 -- idempotent
    yield Note(f"compacted epoch {e} at seq {seq}")


def _sweep(cfg: Cfg, e: int):
    """Remove everything belonging to an epoch below ``e``.

    Safe only after this epoch's own carry-forward is durable (S6).  Epoch fence
    keys below ``e`` go too, otherwise the fence keyspace grows without bound in
    the number of coordinator restarts; the highest one always survives, so a
    later instance still claims a strictly higher epoch.
    """
    for k in (yield Keys(state_root(cfg))):
        owner = epoch_of_state_key(cfg, k)
        if owner is not None and owner < e:
            yield Delete(k)
    for k in (yield Keys(epoch_prefix(cfg))):
        if int(k.rsplit("/", 1)[1]) < e:
            yield Delete(k)


def coordinator(cfg: Cfg, instance_id: str, startup_only: bool = False):
    """The detached job coordinator, as a generator of KV operations."""
    # ---- S1: find the highest claimed epoch --------------------------------
    ekeys = yield Keys(epoch_prefix(cfg))
    epochs = sorted(int(k.rsplit("/", 1)[1]) for k in ekeys)
    e = (max(epochs) + 1) if epochs else 1

    # ---- S2/S3: M1 -- claim it, then prove the claim is mine ---------------
    raw = yield Put(epoch_key(cfg, e), instance_id, overwrite=False)
    if cfg.fence_mode == "readback":
        observed = yield Get(epoch_key(cfg, e))
        won = observed == instance_id
    elif cfg.fence_mode == "raw":
        # No read-back.  Correct in the absence of a lost ack, and only then.
        won = raw == 1
    elif cfg.fence_mode == "inverted":
        # `_internal_kv_put` returns True when the key ALREADY EXISTED.
        won = raw == 0
    elif cfg.fence_mode == "ignore":
        won = True
    else:  # pragma: no cover
        raise ValueError(cfg.fence_mode)

    if not won:
        yield Abort(f"{instance_id}: lost epoch {e}")
        return

    yield Note(f"{instance_id} owns epoch {e}")

    # ---- S4: carry forward the highest readable prior state ----------------
    state = empty_state()
    if cfg.carry_forward:
        # Epochs are scanned ASCENDING, and the highest readable one wins.
        #
        # Not stylistic either.  Epoch `e`'s state is deleted only by a sweep
        # from some epoch `m > e`, and that sweep runs *after* `m` published its
        # own base_ptr.  So "epoch e is unreadable" implies "some epoch m > e was
        # already published".  A reader that visits e before m therefore visits m
        # after it was published, and finds it.  Descending order gets this
        # exactly backwards -- it looks at m too early and at e too late -- and
        # runs/20260910-043934-C13 produced 5448 counterexamples in that window,
        # in which a third coordinator carried forward an entirely empty state.
        #
        # Same rule as the segments-before-pointer fix, one level up: read in the
        # order that makes "I missed X" imply "X's replacement is already
        # published".
        order = sorted(epochs, reverse=True) if cfg.carry_forward_epochs_descending else sorted(epochs)
        for prev in order:
            got = yield from _read_epoch_state(cfg, prev)
            if got is not None:
                state = got
                if cfg.carry_forward_epochs_descending:
                    break

    if cfg.sweep_before_ptr and cfg.startup_sweep:
        yield from _sweep(cfg, e)  # NC4: destroys what we have not yet re-durabilised

    # ---- S5/S6 -------------------------------------------------------------
    yield Put(base_key(cfg, e, state["seq"]), state)
    yield Put(ptr_key(cfg, e), state["seq"])

    # ---- S7 ----------------------------------------------------------------
    if cfg.startup_sweep and not cfg.sweep_before_ptr:
        yield from _sweep(cfg, e)

    if startup_only:
        yield Note(f"{instance_id} startup complete (audit)")
        return

    # ---- steady state: M2 --------------------------------------------------
    done = set(state["done"])
    seq = state["seq"]
    segs_since_compact = 0
    pending: List[int] = []

    todo = (
        list(range(cfg.n_units))
        if cfg.resume_ignores_ledger
        else [u for u in range(cfg.n_units) if u not in done]
    )
    for u in todo:
        yield Exec(u)
        pending.append(u)
        if len(pending) < cfg.W:
            continue
        seq += 1
        yield from _commit(cfg, e, seq, pending)
        done |= set(pending)
        pending = []
        segs_since_compact += 1
        if segs_since_compact >= cfg.K:
            yield from _compact(cfg, e, seq, done)
            segs_since_compact = 0

    if pending:
        seq += 1
        yield from _commit(cfg, e, seq, pending)
        done |= set(pending)

    yield Note(f"{instance_id}: job complete, {len(done)} units")


# --------------------------------------------------------------------------
# an INDEPENDENT reader, used only by the invariant checker
# --------------------------------------------------------------------------


def read_ledger(kv, cfg: Cfg) -> Tuple[Optional[dict], bool]:
    """What would a fresh coordinator recover right now?

    Written separately from the protocol on purpose: a bug in the protocol's own
    reader must not be able to mask itself in the invariant check.

    Returns ``(state_or_None, dangling_pointer_seen)``.
    """
    dangling = False
    ekeys = kv.keys(epoch_prefix(cfg))
    epochs = sorted(int(k.rsplit("/", 1)[1]) for k in ekeys)
    for e in sorted(epochs, reverse=True):
        ptr = kv.get(ptr_key(cfg, e))
        if ptr is None:
            continue
        base = kv.get(base_key(cfg, e, ptr))
        if base is None:
            dangling = True
            continue
        done = set(base["done"])
        seq = base["seq"]
        for k in kv.keys(state_prefix(cfg, e) + "seg/"):
            i = _seq_of(k)
            if i > ptr:
                seg = kv.get(k)
                if seg is not None:
                    done |= set(seg["done"])
                    seq = max(seq, i)
        return {"seq": seq, "done": sorted(done), "epoch": e}, dangling
    return None, dangling
