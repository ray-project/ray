"""Concurrent driver: two coordinator instances, interleaved at KV-op granularity.

Iteration 1 asked what a crash can do to a single writer.  This asks the harder
question: what can a *stale* writer do?  The scenario the design has to survive
is the ``RESTARTING``-at-GCS-death window from ``gcs_actor_manager.cc:1846-1858``
-- an actor that was mid-restart when the GCS died is unconditionally
rescheduled, racing with worker reclamation, so for some window two coordinator
processes exist and only one of them knows it.

Backtracking over interleavings needs the world to be restorable, and a Python
generator cannot be cloned.  So a trace is identified by its **decision list**
and replayed from a clean store every time.  Deterministic, and a counterexample
is fully described by ``(spawn_after, decisions, faults)``.

**Partial-order reduction.**  Exploring every interleaving of two ~40-operation
instances is C(80,40) traces, which is not a number.  The scheduler therefore
only branches when the two instances' next operations *conflict* -- same key, or
a mutation inside a scan's prefix.  Non-conflicting operations commute in both
their effect on the store and in what each instance observes, so ordering them
cannot expose a new state.  This is the textbook persistent-set approximation:
it is a heuristic, not a proof, because it does not close over conflicts that
only appear later in a run.  It is registered as a fidelity gap and cross-checked
against a seeded random interleaving fuzzer, which explores the same space
without the reduction.

**Fencing and the durability contract.**  Once a higher epoch has been claimed,
the old instance is fenced: its writes go to keys the new epoch ignores, and the
design does not promise they survive.  So an ack is only covered by INV-DUR if
the acking instance still held the highest claimed epoch at the moment it acked.
Acks issued after that point are counted separately, as ``fenced_acks`` -- work
the coordinator told its driver was committed and which the ledger will not
return.  Whether that is acceptable is exactly what this iteration is for.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from kvfake import KVStore
import protocol as P
from protocol import Abort, Ack, Cfg, Delete, Exec, Get, Keys, Note, Put
from checker import Violation


# --------------------------------------------------------------------------
# conflict detection -> where the scheduler is allowed to branch
# --------------------------------------------------------------------------


def _touches(op) -> Optional[str]:
    return getattr(op, "key", None)


def conflict(a, b, cfg: Optional[Cfg] = None) -> bool:
    """Do these two pending operations fail to commute?

    ``Ack`` is not a KV operation, but it *observes* the epoch keyspace: whether
    an ack is covered by the durability contract depends on whether a higher
    epoch has been claimed yet.  Omitting that made the reduction unsound in the
    one direction that mattered -- B could never claim an epoch while A was
    still committing, so the fenced-writer window was unreachable.
    """
    if a is None or b is None:
        return False
    if cfg is not None:
        a = Keys(P.epoch_prefix(cfg)) if isinstance(a, Ack) else a
        b = Keys(P.epoch_prefix(cfg)) if isinstance(b, Ack) else b
    a_mut, b_mut = isinstance(a, P.MUTATIONS), isinstance(b, P.MUTATIONS)
    if not a_mut and not b_mut:
        return False  # two reads always commute
    for mut, other in ((a, b), (b, a)):
        if not isinstance(mut, P.MUTATIONS):
            continue
        k = _touches(mut)
        if isinstance(other, Keys) and k is not None and k.startswith(other.prefix):
            return True
        ok = _touches(other)
        if ok is not None and ok == k:
            return True
    return False


# --------------------------------------------------------------------------
# one live instance
# --------------------------------------------------------------------------


@dataclass
class Instance:
    ident: str
    gen: Any
    pending: Any = None  # the operation it is blocked on
    result: Any = None
    status: str = "running"
    ops: int = 0
    epoch: Optional[int] = None
    budget: Optional[int] = None  # stale-tail cap

    def advance(self) -> None:
        """Pull the next operation out of the generator."""
        if self.status != "running":
            return
        if self.budget is not None and self.ops >= self.budget:
            self.status = "suspended"  # the stale writer finally dies
            self.pending = None
            return
        try:
            self.pending = self.gen.send(self.result)
            self.result = None
        except StopIteration:
            self.status = "finished"
            self.pending = None


@dataclass
class ConcTrace:
    spawn_after: int
    decisions: List[int]
    faults: Dict[Tuple[str, int], str] = field(default_factory=dict)
    branches: List[int] = field(default_factory=list)  # options at each branch point
    violations: List[Violation] = field(default_factory=list)
    epoch_owners: Dict[int, List[str]] = field(default_factory=dict)
    acked: set = field(default_factory=set)
    fenced_acks: List[Tuple[str, int, int]] = field(default_factory=list)
    executions: int = 0
    fenced_ops: int = 0
    #: the largest number of operations any single instance issued AFTER being
    #: fenced. If this never approaches the stale_tail cap, the cap is inert and
    #: a sweep over tail lengths has tested nothing.
    max_tail_used: int = 0
    suspended_by_cap: int = 0
    status: Dict[str, str] = field(default_factory=dict)
    orphan_keys: List[str] = field(default_factory=list)
    final_keys: List[str] = field(default_factory=list)
    log: List[str] = field(default_factory=list)

    def summary(self) -> str:
        head = f"spawn@{self.spawn_after} dec={self.decisions}"
        if self.faults:
            head += f" faults={ {f'{k[0]}#{k[1]}': v for k, v in self.faults.items()} }"
        body = "; ".join(str(v) for v in self.violations) if self.violations else "clean"
        return f"[{head}] -> {body}"


# --------------------------------------------------------------------------
# running one interleaving
# --------------------------------------------------------------------------


def _max_claimed_epoch(kv: KVStore, cfg: Cfg) -> int:
    ks = kv.keys(P.epoch_prefix(cfg))
    return max((int(k.rsplit("/", 1)[1]) for k in ks), default=0)


def _check(kv: KVStore, cfg: Cfg, tr: ConcTrace, where: str) -> None:
    state, dangling = P.read_ledger(kv, cfg)
    if dangling:
        tr.violations.append(Violation("INV-PTR", "base_ptr names a missing base", where))
    done = set(state["done"]) if state else set()
    lost = tr.acked - done
    if lost:
        tr.violations.append(
            Violation(
                "INV-DUR",
                f"unfenced acked units {sorted(lost)} are not recoverable "
                f"(recovered {sorted(done)})",
                where,
            )
        )


def run_interleaving(
    cfg: Cfg,
    spawn_after: int,
    decisions: Sequence[int],
    faults: Optional[Dict[Tuple[str, int], str]] = None,
    stale_tail: int = 8,
    window: int = 8,
    rng: Optional[random.Random] = None,
    spawns: Optional[Sequence[int]] = None,
    same_id: bool = False,
) -> ConcTrace:
    """Replay one interleaving of N coordinator instances from a clean store.

    ``spawn_after``  when instance B is created, measured in A's operations.
    ``spawns``       optional extra spawn points for instances C, D, ...,
                     measured in global scheduling steps after B appeared.
    ``same_id``      every instance uses the SAME instance id.  This models the
                     ``RESTARTING``-at-GCS-death duplicate, where two processes
                     of the *same actor* exist -- and it is only realistic if an
                     implementer derives the id from the actor id, which is the
                     natural and stable-looking choice.  See C13.
    ``decisions``    which instance to step at each enumerated branch point.
    ``stale_tail``   operations a fenced instance may still issue.
    ``window``       scheduling steps after the first spawn that are enumerated
                     exhaustively; round-robin outside it.
    """
    kv = KVStore()
    tr = ConcTrace(spawn_after=spawn_after, decisions=list(decisions), faults=dict(faults or {}))
    names = [chr(ord("A") + i) for i in range(12)]

    def make(idx: int) -> Instance:
        return Instance(
            "A" if same_id else names[idx],
            P.coordinator(cfg, "A" if same_id else names[idx]),
        )

    a = make(0)
    a.advance()
    live: List[Instance] = [a]
    pending_spawns = list(spawns or [])
    spawned_b = False
    d_idx = 0
    guard = 0
    rr = 0
    steps_since_spawn = 0

    while True:
        guard += 1
        if guard > 6000:  # pragma: no cover
            tr.violations.append(Violation("INV-TERM", "trace did not terminate", "guard"))
            break

        if not spawned_b and a.ops >= spawn_after:
            nb = make(1)
            nb.advance()
            live.append(nb)
            spawned_b = True
        if spawned_b and pending_spawns and steps_since_spawn >= pending_spawns[0]:
            pending_spawns.pop(0)
            nc = make(len(live))
            nc.advance()
            live.append(nc)

        # A stale tail starts when an instance is FENCED, not when the next one
        # spawns: budgeting from spawn time let the schedule burn the tail
        # before anything had been claimed, so the window was never entered.
        if len(live) > 1:
            top = _max_claimed_epoch(kv, cfg)
            for inst in live:
                if inst.budget is None and inst.epoch is not None and top > inst.epoch:
                    inst.budget = inst.ops + stale_tail

        runnable = [i for i in live if i.status == "running" and i.pending is not None]
        if not runnable:
            if all(i.status != "running" for i in live):
                break
            for i in live:
                i.advance()
            if not [i for i in live if i.status == "running" and i.pending is not None]:
                break
            continue

        if len(runnable) == 1:
            chosen = runnable[0]
        elif steps_since_spawn < window:
            # No reduction inside the window.  A reduction that fixes the order
            # of "commuting" operations starves an instance, and the first
            # version of this rig did exactly that: the second instance never
            # claimed an epoch while the first was running, so every trace came
            # back clean for the wrong reason.
            tr.branches.append(len(runnable))
            if d_idx < len(tr.decisions):
                pick = tr.decisions[d_idx]
            else:
                pick = rng.randrange(len(runnable)) if rng is not None else 0
                tr.decisions.append(pick)
            d_idx += 1
            chosen = runnable[pick % len(runnable)]
        else:
            chosen = runnable[rr % len(runnable)]
            rr += 1

        if spawned_b:
            steps_since_spawn += 1
        _step(kv, cfg, tr, chosen)

    tr.status = {f"{i.ident}{n}": i.status for n, i in enumerate(live)}
    tr.max_tail_used = max(
        (i.ops - (i.budget - stale_tail) for i in live if i.budget is not None),
        default=0)
    tr.suspended_by_cap = sum(1 for i in live if i.status == "suspended")
    _check(kv, cfg, tr, "end of trace")

    audit = Instance("audit", P.coordinator(cfg, "audit", startup_only=True))
    audit.advance()
    while audit.status == "running" and audit.pending is not None:
        _step(kv, cfg, tr, audit, audit=True)
    _check(kv, cfg, tr, "after audit startup")

    state, _ = P.read_ledger(kv, cfg)
    win = state["epoch"] if state else None
    if win is not None:
        orphans = []
        for k in kv.logical_keys():
            if k.startswith(P.state_root(cfg)):
                o = P.epoch_of_state_key(cfg, k)
                if o is not None and o < win:
                    orphans.append(k)
            elif k.startswith(P.epoch_prefix(cfg)) and int(k.rsplit("/", 1)[1]) < win:
                orphans.append(k)
        tr.orphan_keys = orphans
        if orphans:
            tr.violations.append(
                Violation("INV-GC", f"{len(orphans)} orphan key(s): {orphans[:3]}", "audit")
            )

    if not any(st in ("finished", "suspended") for st in tr.status.values()):
        tr.violations.append(
            Violation("INV-LIVE", f"no instance made progress: {tr.status}", "end of trace")
        )

    # INV-ONE: exactly one instance may believe it owns a given epoch.
    # With same_id the identities collide, so count distinct *claims* instead:
    # two constructor executions that both read back the shared id is exactly
    # the split brain C13 is about.
    for e, owners in tr.epoch_owners.items():
        if len(owners) > 1:
            tr.violations.append(
                Violation("INV-ONE", f"epoch {e} claimed {len(owners)}x by {owners}", "fencing")
            )

    if tr.fenced_acks:
        tr.violations.append(
            Violation(
                "INV-FENCE-ACK",
                f"{len(tr.fenced_acks)} ack(s) issued by a fenced instance, e.g. "
                f"{tr.fenced_acks[:3]} (instance, its epoch, unit)",
                "fencing",
            )
        )

    tr.final_keys = kv.logical_keys()
    return tr


def _step(kv: KVStore, cfg: Cfg, tr: ConcTrace, inst: Instance, audit: bool = False) -> None:
    op = inst.pending
    tr.log.append(f"{inst.ident}#{inst.ops}: {op}")
    if not audit and inst.epoch is not None and _max_claimed_epoch(kv, cfg) > inst.epoch:
        tr.fenced_ops += 1
    fault = None if audit else tr.faults.get((inst.ident, inst.ops))
    inst.ops += 1

    if isinstance(op, Abort):
        inst.status = "aborted"
        inst.pending = None
        return
    if isinstance(op, Note):
        if " owns epoch " in op.text:
            e = int(op.text.rsplit(" ", 1)[1])
            inst.epoch = e
            tr.epoch_owners.setdefault(e, []).append(inst.ident)
        inst.result = None
        inst.advance()
        return
    if isinstance(op, Exec):
        tr.executions += 1
        inst.result = None
        inst.advance()
        return
    if isinstance(op, Ack):
        top = _max_claimed_epoch(kv, cfg)
        if inst.epoch is not None and inst.epoch >= top:
            tr.acked |= set(op.units)
        else:
            for u in op.units:
                tr.fenced_acks.append((inst.ident, inst.epoch, u))
        _check(kv, cfg, tr, f"{inst.ident}#{inst.ops - 1} after Ack")
        inst.result = None
        inst.advance()
        return

    if isinstance(op, Put):
        inst.result = kv.put(op.key, op.value, op.overwrite)
    elif isinstance(op, Get):
        inst.result = kv.get(op.key)
    elif isinstance(op, Delete):
        inst.result = kv.delete(op.key)
    elif isinstance(op, Keys):
        inst.result = kv.keys(op.prefix)
    else:  # pragma: no cover
        raise TypeError(op)

    if isinstance(op, P.MUTATIONS):
        _check(kv, cfg, tr, f"{inst.ident}#{inst.ops - 1} after {type(op).__name__}")
        if fault == "lost_ack":
            inst.result = (
                kv.put(op.key, op.value, op.overwrite)
                if isinstance(op, Put)
                else kv.delete(op.key)
            )
            _check(kv, cfg, tr, f"{inst.ident}#{inst.ops - 1} after retried")
    inst.advance()


# --------------------------------------------------------------------------
# the search
# --------------------------------------------------------------------------


@dataclass
class ConcResult:
    traces: int = 0
    violating: List[ConcTrace] = field(default_factory=list)
    kinds: Dict[str, int] = field(default_factory=dict)
    max_branches: int = 0
    fenced_ack_traces: int = 0
    spawn_points: int = 0
    #: traces in which a fenced instance actually executed >=1 operation.
    #: The coverage control from experiments/C1.md: a run in which this is small
    #: has not searched the space it claims to have searched.
    fenced_active_traces: int = 0
    max_tail_used: int = 0
    traces_hitting_cap: int = 0
    dur_examples: List[ConcTrace] = field(default_factory=list)
    one_examples: List[ConcTrace] = field(default_factory=list)

    def record(self, tr: ConcTrace) -> None:
        self.traces += 1
        self.max_branches = max(self.max_branches, len(tr.branches))
        if tr.fenced_acks:
            self.fenced_ack_traces += 1
        if tr.fenced_ops:
            self.fenced_active_traces += 1
        self.max_tail_used = max(self.max_tail_used, tr.max_tail_used)
        if tr.suspended_by_cap:
            self.traces_hitting_cap += 1
        kinds_here = {v.kind for v in tr.violations}
        if "INV-DUR" in kinds_here and len(self.dur_examples) < 5:
            self.dur_examples.append(tr)
        if "INV-ONE" in kinds_here and len(self.one_examples) < 5:
            self.one_examples.append(tr)
        for v in tr.violations:
            self.kinds[v.kind] = self.kinds.get(v.kind, 0) + 1
        if tr.violations and len(self.violating) < 40:
            self.violating.append(tr)


def search_concurrent(
    cfg: Cfg,
    max_spawn: int = 60,
    stale_tail: int = 8,
    window: int = 8,
    faults: Optional[Dict[Tuple[str, int], str]] = None,
    max_traces: Optional[int] = None,
    spawns: Optional[Sequence[int]] = None,
    same_id: bool = False,
    spawn_step: int = 1,
) -> ConcResult:
    """Exhaustive (under the conflict reduction) DFS over spawn points and
    conflicting interleavings."""
    res = ConcResult()

    for spawn in range(0, max_spawn, spawn_step):
        seen_any = False

        def rec(dec: List[int]) -> None:
            nonlocal seen_any
            if max_traces is not None and res.traces >= max_traces:
                return
            tr = run_interleaving(
                cfg, spawn, dec, faults=faults, stale_tail=stale_tail, window=window,
                spawns=spawns, same_id=same_id,
            )
            res.record(tr)
            seen_any = True
            if len(tr.branches) > len(dec):
                n = tr.branches[len(dec)]
                for c in range(n):
                    rec(dec + [c])

        rec([])
        res.spawn_points += 1
        # once B spawns after A has already finished, higher spawn points repeat
        if not seen_any:
            break
    return res


def fuzz_concurrent(
    cfg: Cfg,
    seeds: Sequence[int],
    traces_per_seed: int = 400,
    stale_tail: int = 8,
    window: int = 10 ** 6,
    spawns: Optional[Sequence[int]] = None,
    same_id: bool = False,
) -> ConcResult:
    """Cross-check: random interleavings, no partial-order reduction applied to
    the *choice* (the reduction only decides where branches exist, and here the
    rng picks at every branch)."""
    res = ConcResult()
    for seed in seeds:
        rng = random.Random(seed)
        for _ in range(traces_per_seed):
            spawn = rng.randrange(0, 55)
            tr = run_interleaving(
                cfg, spawn, [], stale_tail=stale_tail, window=window, rng=rng,
                spawns=spawns, same_id=same_id,
            )
            res.record(tr)
    return res
