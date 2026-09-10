"""Stage B: what the ledger actually costs, and where the usable band is.

Reuses ``protocol.py`` unmodified and counts the fsyncs it really issues rather
than trusting a hand-derived formula -- which matters, because the design doc's
formula turned out to omit compaction entirely.

Fsyncs are the currency: ``WriteOptions::sync = true`` unconditionally
(``rocksdb_store_client.cc:104-117``), so one mutation is one fsync, measured by
REP-64 at ~3.81 ms p50 on ext4.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Dict, List

from kvfake import FSYNC_SECONDS, KVStore
import protocol as P
from protocol import Ack, Cfg, Delete, Exec, Get, Keys, Note, Put

#: REP-64 benchmark ceilings.
SAME_KEY_WRITES_PER_S = 1.0 / FSYNC_SECONDS  # ~262/s, strand-serialised
AGGREGATE_OPS_PER_S = 593.0  # with offload + group commit


@dataclass
class CostRun:
    n: int
    w: int
    k: int
    fsyncs: int
    puts: int
    deletes: int
    reads: int
    max_keys: int
    final_keys: int
    recovery_ops: int
    scanned_entries: int

    @property
    def naive_bound(self) -> int:
        """What the design doc's `ceil(N/W) + O(1)` predicts."""
        return math.ceil(self.n / self.w)

    @property
    def overhead_ratio(self) -> float:
        return self.fsyncs / max(1, self.naive_bound)


def measure(cfg: Cfg) -> CostRun:
    """Run one clean job, then one carry-forward, counting everything."""
    kv = KVStore()
    puts = deletes = 0
    max_keys = 0

    def drive(gen, count: bool = True):
        nonlocal puts, deletes, max_keys
        result = None
        ops = 0
        while True:
            try:
                op = gen.send(result)
            except StopIteration:
                return ops
            result = None
            ops += 1
            if isinstance(op, (Note, Ack, Exec)):
                continue
            if isinstance(op, Put):
                result = kv.put(op.key, op.value, op.overwrite)
                if count:
                    puts += 1
            elif isinstance(op, Get):
                result = kv.get(op.key)
            elif isinstance(op, Delete):
                result = kv.delete(op.key)
                if count:
                    deletes += 1
            elif isinstance(op, Keys):
                result = kv.keys(op.prefix)
            own = [k for k in kv.logical_keys()]
            max_keys = max(max_keys, len(own))

    drive(P.coordinator(cfg, "worker"))
    fsyncs_job = kv.fsyncs
    reads_job = kv.reads
    before = kv.reads + kv.fsyncs
    drive(P.coordinator(cfg, "recover", startup_only=True), count=False)
    recovery_ops = (kv.reads + kv.fsyncs) - before

    return CostRun(
        n=cfg.n_units, w=cfg.W, k=cfg.K,
        fsyncs=fsyncs_job, puts=puts, deletes=deletes, reads=reads_job,
        max_keys=max_keys, final_keys=len(kv.logical_keys()),
        recovery_ops=recovery_ops, scanned_entries=kv.scanned_entries,
    )


def band(rate: float, n: int, overhead: float,
         write_budget_frac: float = 0.10, redo_frac: float = 0.01) -> Dict:
    """For a completion rate and job size, which W values are usable?

    Two-sided, which is the point: large W is cheap and forgetful, small W is
    expensive and precise, and the design is only interesting where both
    constraints can be satisfied at once.
    """
    ceiling = SAME_KEY_WRITES_PER_S * write_budget_frac
    ok = []
    for w in (1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000):
        if w > n:
            break
        writes_per_s = (rate / w) * overhead
        expected_redo = w / 2.0
        if writes_per_s <= ceiling and expected_redo <= redo_frac * n:
            ok.append(w)
    return {
        "rate": rate, "n": n, "usable_W": ok,
        "min_W": min(ok) if ok else None, "max_W": max(ok) if ok else None,
        "feasible": bool(ok),
    }
