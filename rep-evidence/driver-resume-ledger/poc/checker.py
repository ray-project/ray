"""The enumerator: crash and fault injection over every reachable schedule.

A real cluster tests one interleaving per run, chosen by luck.  This tests all of
them within a bounded fault budget, which for ordering claims is both cheaper and
strictly more severe.

Fault model.  Because an acked mutation is durable by construction
(``WriteOptions::sync = true``), there is no "lost write" fault.  What there is:

``crash_before``
    the operation never reaches GCS; this instance dies.
``crash_after``
    the operation is applied and durable; this instance dies before learning
    that it succeeded.  Every linearization point in the protocol has to survive
    a crash landing exactly here.
``lost_ack``
    the operation is applied, the acknowledgement is lost, and the client
    re-issues it.  Not exotic: ``UNAVAILABLE``/``UNKNOWN`` are retried
    infinitely (``grpc_util.h:127-133``), so this happens on every head
    failover.  ``DEADLINE_EXCEEDED`` (ray#55996) is the same shape from the
    protocol's point of view -- applied or not, retried by us -- so it collapses
    into this fault rather than needing its own.

Invariants are checked after **every** durable mutation, not at end of trace.  A
protocol that loses progress transiently and re-derives it later is still wrong,
because the next crash can land inside that window.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from kvfake import KVStore
import protocol as P
from protocol import Abort, Ack, Cfg, Delete, Exec, Get, Keys, Note, Put

FAULTS = ("crash_before", "crash_after", "lost_ack")
Position = Tuple[int, int]  # (instance index, operation index within instance)


@dataclass
class Violation:
    kind: str
    detail: str
    where: str

    def __str__(self) -> str:
        return f"{self.kind} at {self.where}: {self.detail}"


@dataclass
class Trace:
    plan: Dict[Position, str]
    executed: List[Tuple[Position, str]] = field(default_factory=list)
    violations: List[Violation] = field(default_factory=list)
    instances: List[dict] = field(default_factory=list)
    acked: set = field(default_factory=set)
    final_keys: List[str] = field(default_factory=list)
    fsyncs: int = 0
    scanned_entries: int = 0
    compactions: int = 0
    executions: int = 0
    crashes: int = 0
    audit_epoch: Optional[int] = None
    orphan_keys: List[str] = field(default_factory=list)
    redo: int = 0

    def summary(self) -> str:
        plan = ", ".join(
            f"i{p[0]}#{p[1]}:{f}" for p, f in sorted(self.plan.items())
        ) or "(no faults)"
        return f"[{plan}] -> " + (
            "; ".join(str(v) for v in self.violations) if self.violations else "clean"
        )


# --------------------------------------------------------------------------
# invariants
# --------------------------------------------------------------------------


def check_invariants(kv: KVStore, cfg: Cfg, tr: Trace, where: str) -> None:
    state, dangling = P.read_ledger(kv, cfg)
    if dangling:
        tr.violations.append(
            Violation("INV-PTR", "base_ptr names a base key that does not exist", where)
        )
    done = set(state["done"]) if state else set()
    lost = tr.acked - done
    if lost:
        tr.violations.append(
            Violation(
                "INV-DUR",
                f"acked units {sorted(lost)} are not recoverable "
                f"(recovered {sorted(done)})",
                where,
            )
        )


# --------------------------------------------------------------------------
# executing one instance
# --------------------------------------------------------------------------


def _apply(kv: KVStore, op) -> Any:
    if isinstance(op, Put):
        return kv.put(op.key, op.value, op.overwrite)
    if isinstance(op, Get):
        return kv.get(op.key)
    if isinstance(op, Delete):
        return kv.delete(op.key)
    if isinstance(op, Keys):
        return kv.keys(op.prefix)
    raise TypeError(op)  # pragma: no cover


def _drive(kv: KVStore, cfg: Cfg, tr: Trace, inst: int, gen, audit: bool = False) -> dict:
    """Run one instance to completion, crash or abort.

    ``audit=True`` runs the instance outside the fault model entirely: it takes
    no faults and contributes no positions to the search space.  The audit is
    the "one clean instance" that INV-GC is defined against, so injecting a
    crash into it would make the invariant vacuously false.
    """
    result = None
    op_idx = 0
    while True:
        try:
            op = gen.send(result)
        except StopIteration:
            return {"status": "finished", "n_ops": op_idx}

        pos: Position = (inst, op_idx)
        fault = None if audit else tr.plan.get(pos)
        kind = type(op).__name__
        if not audit:
            tr.executed.append((pos, kind))
        op_idx += 1
        result = None

        if isinstance(op, Abort):
            return {"status": "aborted", "n_ops": op_idx, "reason": op.reason}
        if isinstance(op, Note):
            if "compacted" in op.text:
                tr.compactions += 1
            continue

        if fault == "crash_before":
            return {"status": "crashed", "n_ops": op_idx, "at": kind}

        if isinstance(op, Exec):
            # The unit's side effect lands here.  A crash immediately after it
            # is the at-least-once case the design accepts (C10): the effect
            # happened, nothing recorded it, and it will be redone.
            tr.executions += 1
            if fault == "crash_after":
                return {"status": "crashed", "n_ops": op_idx, "at": kind}
            continue

        if isinstance(op, Ack):
            tr.acked |= set(op.units)
            check_invariants(kv, cfg, tr, f"i{inst}#{pos[1]} after Ack")
        else:
            result = _apply(kv, op)
            if isinstance(op, P.MUTATIONS):
                check_invariants(kv, cfg, tr, f"i{inst}#{pos[1]} after {kind}")

        if fault == "crash_after":
            return {"status": "crashed", "n_ops": op_idx, "at": kind}

        if fault == "lost_ack" and isinstance(op, P.MUTATIONS):
            # applied, ack lost, client re-issues the identical operation
            result = _apply(kv, op)
            check_invariants(kv, cfg, tr, f"i{inst}#{pos[1]} after retried {kind}")


# --------------------------------------------------------------------------
# executing one whole trace
# --------------------------------------------------------------------------


def run_trace(cfg: Cfg, plan: Dict[Position, str], max_instances: int = 8) -> Trace:
    """Replay a fault plan from a clean store.

    Instances run sequentially: a crashed coordinator is replaced by a fresh one,
    which is exactly what ``max_restarts=-1`` does.  Concurrent instances are a
    different question (C1/C13) and get a different driver.
    """
    kv = KVStore()
    tr = Trace(plan=dict(plan))

    inst = 0
    while inst < max_instances:
        gen = P.coordinator(cfg, f"i{inst}")
        outcome = _drive(kv, cfg, tr, inst, gen)
        tr.instances.append(outcome)
        inst += 1
        if outcome["status"] != "crashed":
            break

    tr.crashes = sum(1 for i in tr.instances if i["status"] == "crashed")

    # --- INV-REDO: a crash must cost at most the uncommitted window ---------
    # Each crash can lose at most the units accumulated in the current
    # coalescing batch, i.e. W of them.  Anything more means resume did not
    # actually resume.  This is the only invariant a fault-free run cannot
    # violate, which makes it the control that proves the crash injector works.
    redo = tr.executions - cfg.n_units
    tr.redo = redo
    if redo > tr.crashes * cfg.W:
        tr.violations.append(
            Violation(
                "INV-REDO",
                f"{redo} unit(s) re-executed after {tr.crashes} crash(es); "
                f"budget is {tr.crashes * cfg.W} (= crashes x W)",
                "end of trace",
            )
        )

    # --- INV-LIVE: somebody has to have made it -----------------------------
    # Under the real protocol every instance either finishes or is crashed by
    # the plan; an instance that *aborts* has concluded it lost an epoch it
    # actually owns.  In the sequential model that can only happen if fencing
    # misreads a lost ack, which makes this the control that proves the
    # lost_ack injector is live.
    if not any(i["status"] == "finished" for i in tr.instances):
        tr.violations.append(
            Violation(
                "INV-LIVE",
                "no instance ever completed: "
                + ", ".join(
                    f"{i['status']}({i.get('reason') or i.get('at','')})"
                    for i in tr.instances
                ),
                "end of trace",
            )
        )

    # --- the audit: one clean instance completes startup, then we look ------
    gen = P.coordinator(cfg, "audit", startup_only=True)
    tr.instances.append(_drive(kv, cfg, tr, inst, gen, audit=True))
    check_invariants(kv, cfg, tr, "after audit startup")

    state, _ = P.read_ledger(kv, cfg)
    tr.audit_epoch = state["epoch"] if state else None
    if tr.audit_epoch is not None:
        orphans = []
        for k in kv.logical_keys():
            if k.startswith(P.state_root(cfg)):
                owner = P.epoch_of_state_key(cfg, k)
                if owner is not None and owner < tr.audit_epoch:
                    orphans.append(k)
            elif k.startswith(P.epoch_prefix(cfg)):
                if int(k.rsplit("/", 1)[1]) < tr.audit_epoch:
                    orphans.append(k)
        tr.orphan_keys = orphans
        if orphans:
            tr.violations.append(
                Violation(
                    "INV-GC",
                    f"{len(orphans)} key(s) below the winning epoch survived a "
                    f"clean startup: {orphans[:4]}",
                    "after audit startup",
                )
            )

    tr.final_keys = kv.logical_keys()
    tr.fsyncs = kv.fsyncs
    tr.scanned_entries = kv.scanned_entries
    return tr


# --------------------------------------------------------------------------
# the search
# --------------------------------------------------------------------------


@dataclass
class SearchResult:
    traces: int = 0
    violating: List[Trace] = field(default_factory=list)
    max_instances_seen: int = 0
    compactions_seen: int = 0
    max_keys: int = 0
    max_redo: int = 0
    kinds: Dict[str, int] = field(default_factory=dict)

    def record(self, tr: Trace) -> None:
        self.traces += 1
        self.max_instances_seen = max(self.max_instances_seen, len(tr.instances))
        self.compactions_seen = max(self.compactions_seen, tr.compactions)
        self.max_keys = max(self.max_keys, len(tr.final_keys))
        self.max_redo = max(self.max_redo, tr.redo)
        for v in tr.violations:
            self.kinds[v.kind] = self.kinds.get(v.kind, 0) + 1
        if tr.violations and len(self.violating) < 12:
            self.violating.append(tr)


def search(
    cfg: Cfg,
    budget: int,
    faults: Tuple[str, ...] = FAULTS,
    stop_on_violation: bool = False,
    max_traces: Optional[int] = None,
) -> SearchResult:
    """Exhaustive DFS over fault plans of size <= ``budget``.

    Fault positions are added in increasing order so each set is enumerated
    exactly once.  Because a crash truncates its instance, the set of reachable
    positions depends on the plan, so positions are discovered by execution
    rather than precomputed.
    """
    res = SearchResult()

    def rec(plan: Dict[Position, str], after: Position) -> bool:
        tr = run_trace(cfg, plan)
        res.record(tr)
        if stop_on_violation and tr.violations:
            return True
        if max_traces is not None and res.traces >= max_traces:
            return True
        if len(plan) >= budget:
            return False
        for pos, kind in tr.executed:
            if pos <= after or pos in plan:
                continue
            for f in faults:
                if f == "lost_ack" and kind not in ("Put", "Delete"):
                    continue
                if kind == "Exec" and f == "crash_before":
                    # identical in effect to crash_after on the previous op
                    continue
                if kind in ("Abort", "Note"):
                    continue
                child = dict(plan)
                child[pos] = f
                if rec(child, pos):
                    return True
        return False

    rec({}, (-1, -1))
    return res


# --------------------------------------------------------------------------
# C12: does session scoping stop a reused storage path from being adopted?
# --------------------------------------------------------------------------


def run_session_scoping(cfg: Cfg, prior_session: str = "session-A",
                        new_session: str = "session-B") -> dict:
    """Run a job to completion under one session, then start a coordinator
    under another **on the same store** -- which is what a new cluster pointed
    at an existing ``gcs_storage_path`` gets, because the KV DB's cluster-id
    guard is disabled (``gcs_server.cc:791``).
    """
    from dataclasses import replace as _replace

    old = _replace(cfg, session=prior_session)
    new = _replace(cfg, session=new_session)

    kv = KVStore()
    tr_old = Trace(plan={})
    gen = P.coordinator(old, "old-cluster")
    _drive(kv, old, tr_old, 0, gen)
    keys_after_old = set(kv.logical_keys())
    completed = sorted(tr_old.acked)

    tr_new = Trace(plan={})
    gen = P.coordinator(new, "new-cluster")
    _drive(kv, new, tr_new, 0, gen, audit=True)  # no faults; we want clean behaviour

    adopted, _ = P.read_ledger(kv, new)
    # what did the new session actually execute?
    executed = tr_new.executions
    survivors = set(kv.logical_keys())
    destroyed = sorted(k for k in keys_after_old if k not in survivors)

    violations = []
    if executed < cfg.n_units:
        violations.append(
            f"INV-SESS: the new session executed only {executed} of "
            f"{cfg.n_units} units -- it adopted the prior cluster's ledger"
        )
    if destroyed:
        violations.append(
            f"INV-SESS: the new session deleted {len(destroyed)} key(s) belonging "
            f"to the prior cluster, e.g. {destroyed[:3]}"
        )
    return {
        "prior_completed": completed,
        "new_session_executed": executed,
        "new_session_recovered": adopted["done"] if adopted else [],
        "prior_keys_destroyed": destroyed,
        "violations": violations,
    }
