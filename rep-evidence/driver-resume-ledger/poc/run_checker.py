"""Harness: runs a pre-registered experiment into an immutable ``runs/<id>/``.

Usage::

    python3 poc/run_checker.py --experiment C6 --out runs/<id>

Writes ``results.json`` (machine readable), ``stdout.log`` (human readable) and
``config.json`` (workloads, budgets, git revision) into the run directory.  Run
directories are evidence and are never edited after the fact.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, replace
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from checker import FAULTS, run_session_scoping, search  # noqa: E402
from concurrent import fuzz_concurrent, search_concurrent  # noqa: E402
from protocol import Cfg  # noqa: E402
from costmodel import AGGREGATE_OPS_PER_S, SAME_KEY_WRITES_PER_S, band, measure  # noqa: E402

# Workload shapes.  Each is chosen so a trace crosses at least two compactions
# and, once a crash is injected, at least one epoch change.
WORKLOADS = {
    "small": Cfg(n_units=6, W=1, K=2),
    "batched": Cfg(n_units=8, W=2, K=2),
    "wide": Cfg(n_units=12, W=3, K=3),
}

# Negative controls: one flag each, every one a bug a competent engineer could
# plausibly ship.  Each must produce a concrete counterexample trace.
CONTROLS: Dict[str, Dict] = {
    "NC1_fence_ignored": dict(fence_mode="ignore"),
    "NC2_fence_inverted": dict(fence_mode="inverted"),
    "NC3_state_not_epoch_scoped": dict(epoch_scoped_state=False),
    "NC4_sweep_before_ptr": dict(sweep_before_ptr=True),
    "NC5_compact_delete_before_ptr": dict(compact_delete_before_ptr=True),
    "NC6_compact_base_in_place": dict(compact_in_place=True),
    "NC7_no_startup_sweep": dict(startup_sweep=False),
    "NC8_ctor_seeds_empty_base": dict(carry_forward=False),
    "NC10_compact_ptr_before_base": dict(compact_ptr_before_base=True),
    "NC1b_fence_raw_no_readback": dict(fence_mode="raw"),
    "NC11_ack_before_commit": dict(ack_before_commit=True),
    "NC12_resume_ignores_ledger": dict(resume_ignores_ledger=True),
    "NC13_carry_forward_ptr_first": dict(carry_forward_ptr_first=True),
}

# Which controls are load-bearing for which experiment's validity.
CONTROLS_FOR = {
    "C6": [
        "NC4_sweep_before_ptr",
        "NC5_compact_delete_before_ptr",
        "NC6_compact_base_in_place",
        "NC7_no_startup_sweep",
        "NC10_compact_ptr_before_base",
        # These two exist to prove the fault injectors themselves are live.
        "NC11_ack_before_commit",
        "NC12_resume_ignores_ledger",   # can ONLY fail under crash injection
        "NC1b_fence_raw_no_readback",   # can ONLY fail under lost_ack injection
    ],
}
# Controls whose failure to go red voids the run.  NC6 and NC10 were flagged in
# the C6 card as possibly-safe-in-this-model, so they inform but do not void.
VOIDING = {
    "C6": [
        "NC4_sweep_before_ptr",
        "NC5_compact_delete_before_ptr",
        "NC7_no_startup_sweep",
        # Without these two, a green result would be compatible with the crash
        # and lost-ack injectors doing nothing at all: NC4/NC5/NC7/NC10 all go
        # red with zero faults injected, so on their own they only prove the
        # invariant checker works, not that the fault model does.
        "NC12_resume_ignores_ledger",
        "NC1b_fence_raw_no_readback",
    ],
}


def git_rev() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()
    except Exception:  # pragma: no cover
        return "unknown"


class Tee:
    def __init__(self, path):
        self.f = open(path, "w")

    def __call__(self, *a):
        line = " ".join(str(x) for x in a)
        print(line)
        self.f.write(line + "\n")
        self.f.flush()


def run_c6(log, budgets: List[int]) -> dict:
    out = {"real": {}, "controls": {}}

    log("=" * 78)
    log("REAL PROTOCOL -- exhaustive crash/lost-ack enumeration")
    log("=" * 78)
    for wl_name, cfg in WORKLOADS.items():
        for b in budgets:
            t0 = time.time()
            res = search(cfg, budget=b, faults=FAULTS)
            dt = time.time() - t0
            key = f"{wl_name}/B={b}"
            out["real"][key] = {
                "workload": asdict(cfg),
                "budget": b,
                "traces": res.traces,
                "violations_by_kind": res.kinds,
                "max_instances": res.max_instances_seen,
                "max_compactions": res.compactions_seen,
                "max_final_keys": res.max_keys,
                "seconds": round(dt, 2),
                "examples": [t.summary() for t in res.violating],
            }
            status = "CLEAN" if not res.kinds else f"VIOLATIONS {res.kinds}"
            log(
                f"  {key:16s} traces={res.traces:<7d} "
                f"instances<={res.max_instances_seen} "
                f"compactions<={res.compactions_seen} "
                f"keys<={res.max_keys:<3d} {dt:6.1f}s  {status}"
            )
            for ex in res.violating[:3]:
                log(f"      ! {ex.summary()}")

    log("")
    log("=" * 78)
    log("NEGATIVE CONTROLS -- each MUST produce a counterexample")
    log("=" * 78)
    base = WORKLOADS["small"]
    for name in CONTROLS_FOR["C6"]:
        cfg = replace(base, **CONTROLS[name])
        t0 = time.time()
        res = search(cfg, budget=2, faults=FAULTS, stop_on_violation=True)
        dt = time.time() - t0
        red = bool(res.kinds)
        out["controls"][name] = {
            "flags": CONTROLS[name],
            "went_red": red,
            "traces_until_first_violation": res.traces,
            "violations_by_kind": res.kinds,
            "counterexample": res.violating[0].summary() if res.violating else None,
            "seconds": round(dt, 2),
            "voids_run_if_green": name in VOIDING["C6"],
        }
        log(f"  {name:34s} {'RED  ' if red else 'GREEN'} "
            f"after {res.traces} trace(s), {dt:.1f}s")
        if res.violating:
            log(f"      counterexample: {res.violating[0].summary()}")

    return out


CONC_WORKLOADS = {"tiny": Cfg(n_units=4, W=1, K=2), "small": Cfg(n_units=6, W=1, K=2)}
CONC_CONTROLS = ["NC1_fence_ignored", "NC2_fence_inverted", "NC3_state_not_epoch_scoped"]
C16_CONTROLS = ["NC13_carry_forward_ptr_first", "NC1_fence_ignored", "NC2_fence_inverted"]


def run_c1(log, windows=(4, 6), seeds=(1, 2, 3), controls=None) -> dict:
    controls = controls or CONC_CONTROLS
    out = {"real": {}, "fuzz": {}, "controls": {}}

    log("=" * 78)
    log("REAL PROTOCOL -- two concurrent instances, windowed exhaustive search")
    log("=" * 78)
    for wl, cfg in CONC_WORKLOADS.items():
        for w in windows:
            t0 = time.time()
            res = search_concurrent(cfg, max_spawn=45, window=w, stale_tail=8)
            dt = time.time() - t0
            out["real"][f"{wl}/w={w}"] = {
                "workload": asdict(cfg),
                "window": w,
                "traces": res.traces,
                "violations_by_kind": res.kinds,
                "fenced_active_traces": res.fenced_active_traces,
                "fenced_ack_traces": res.fenced_ack_traces,
                "seconds": round(dt, 2),
                "inv_dur_examples": [t.summary() for t in res.dur_examples],
                "inv_one_examples": [t.summary() for t in res.one_examples],
                "minimal_inv_dur_log": (
                    min(res.dur_examples, key=lambda t: len(t.log)).log
                    if res.dur_examples
                    else None
                ),
            }
            log(f"  {wl}/w={w:<2d} traces={res.traces:<7d} fenced-active={res.fenced_active_traces:<6d} "
                f"{dt:6.1f}s  {res.kinds or 'CLEAN'}")

    log("")
    log("  cross-check: random interleaving fuzzer, no window bound")
    for wl, cfg in CONC_WORKLOADS.items():
        t0 = time.time()
        res = fuzz_concurrent(cfg, seeds=list(seeds), traces_per_seed=500)
        out["fuzz"][wl] = {
            "seeds": list(seeds),
            "traces": res.traces,
            "violations_by_kind": res.kinds,
            "fenced_active_traces": res.fenced_active_traces,
            "fenced_ack_traces": res.fenced_ack_traces,
            "seconds": round(time.time() - t0, 2),
            "inv_dur_examples": [t.summary() for t in res.dur_examples],
        }
        log(f"  {wl:8s} traces={res.traces:<6d} fenced-active={res.fenced_active_traces:<6d} "
            f"{res.kinds or 'CLEAN'}")

    log("")
    log("=" * 78)
    log("NEGATIVE CONTROLS")
    log("=" * 78)
    base = CONC_WORKLOADS["tiny"]
    for name in controls:
        cfg = replace(base, **CONTROLS[name])
        res = search_concurrent(cfg, max_spawn=45, window=6, stale_tail=8, max_traces=6000)
        out["controls"][name] = {
            "flags": CONTROLS[name],
            "violations_by_kind": res.kinds,
            "traces": res.traces,
            "went_red_INV_ONE": "INV-ONE" in res.kinds,
            "went_red_any_fencing": bool({"INV-ONE", "INV-LIVE"} & set(res.kinds)),
            "went_red_INV_DUR": "INV-DUR" in res.kinds,
            "example": res.one_examples[0].summary() if res.one_examples else (
                res.violating[0].summary() if res.violating else None),
            "epoch_owners_example": (
                res.one_examples[0].epoch_owners if res.one_examples else None),
        }
        log(f"  {name:30s} {res.kinds or 'CLEAN'}  ({res.traces} traces)")
        if res.one_examples:
            log(f"      INV-ONE: {res.one_examples[0].epoch_owners}")
    return out


def run_c13(log) -> dict:
    out = {"two_instance_regression": {}, "three_instance": {}, "fuzz": {}, "controls": {}}

    log("=" * 78)
    log("CONTROL FIRST -- two-instance regression (must be clean of INV-DUR)")
    log("=" * 78)
    for wl, cfg in CONC_WORKLOADS.items():
        res = search_concurrent(cfg, max_spawn=45, window=5, stale_tail=8)
        out["two_instance_regression"][wl] = {
            "traces": res.traces, "violations_by_kind": res.kinds,
            "fenced_active_traces": res.fenced_active_traces,
        }
        log(f"  {wl:8s} traces={res.traces:<7d} {res.kinds or 'CLEAN'}")

    log("")
    log("=" * 78)
    log("THREE CONCURRENT INSTANCES")
    log("=" * 78)
    for wl, cfg in CONC_WORKLOADS.items():
        for w in (4, 5):
            t0 = time.time()
            res = search_concurrent(
                cfg, max_spawn=30, window=w, stale_tail=8, spawns=[3], spawn_step=3
            )
            out["three_instance"][f"{wl}/w={w}"] = {
                "traces": res.traces, "violations_by_kind": res.kinds,
                "fenced_active_traces": res.fenced_active_traces,
                "seconds": round(time.time() - t0, 2),
                "inv_dur_examples": [t.summary() for t in res.dur_examples],
                "minimal_inv_dur_log": (
                    min(res.dur_examples, key=lambda t: len(t.log)).log
                    if res.dur_examples else None),
            }
            log(f"  {wl}/w={w} traces={res.traces:<7d} fenced-active={res.fenced_active_traces:<6d} "
                f"{res.kinds or 'CLEAN'}")

    log("")
    log("  fuzz, three instances, unbounded window")
    for wl, cfg in CONC_WORKLOADS.items():
        res = fuzz_concurrent(cfg, seeds=[1, 2, 3], traces_per_seed=400, spawns=[3])
        out["fuzz"][wl] = {"traces": res.traces, "violations_by_kind": res.kinds,
                           "fenced_active_traces": res.fenced_active_traces}
        log(f"  {wl:8s} traces={res.traces:<6d} {res.kinds or 'CLEAN'}")

    log("")
    log("=" * 78)
    log("NEGATIVE CONTROLS")
    log("=" * 78)
    base = CONC_WORKLOADS["tiny"]
    specs = [
        ("NC14_same_instance_id", {}, dict(same_id=True)),
        ("NC13_carry_forward_ptr_first", CONTROLS["NC13_carry_forward_ptr_first"], {}),
        ("NC2_fence_inverted", CONTROLS["NC2_fence_inverted"], {}),
    ]
    for name, flags, kw in specs:
        res = search_concurrent(replace(base, **flags), max_spawn=30, window=5,
                                stale_tail=8, max_traces=8000, **kw)
        out["controls"][name] = {
            "flags": flags, "kwargs": kw, "traces": res.traces,
            "violations_by_kind": res.kinds,
            "red_INV_ONE": "INV-ONE" in res.kinds,
            "red_INV_DUR": "INV-DUR" in res.kinds,
            "red_INV_LIVE": "INV-LIVE" in res.kinds,
            "example": res.one_examples[0].summary() if res.one_examples else None,
            "epoch_owners_example": (res.one_examples[0].epoch_owners
                                     if res.one_examples else None),
        }
        log(f"  {name:32s} {res.kinds or 'CLEAN'}  ({res.traces} traces)")
        if res.one_examples:
            log(f"      INV-ONE: {res.one_examples[0].epoch_owners}")
    return out


def run_c20(log) -> dict:
    out = {"regressions": {}, "n_instance": {}, "fuzz": {}, "controls": {}}

    log("=" * 78)
    log("REGRESSIONS FIRST")
    log("=" * 78)
    reg = search(WORKLOADS["small"], budget=2, faults=FAULTS)
    out["regressions"]["C6_sequential"] = {"traces": reg.traces,
                                           "violations_by_kind": reg.kinds}
    log(f"  C6 sequential      {reg.traces} traces {reg.kinds or 'CLEAN'}")
    for wl, cfg in CONC_WORKLOADS.items():
        r2 = search_concurrent(cfg, max_spawn=45, window=5, stale_tail=8)
        out["regressions"][f"two_instance_{wl}"] = {
            "traces": r2.traces, "violations_by_kind": r2.kinds}
        log(f"  2-instance {wl:8s} {r2.traces} traces {r2.kinds or 'CLEAN'}")

    log("")
    log("=" * 78)
    log("N CONCURRENT INSTANCES -- pushed past where the refutation appeared")
    log("=" * 78)
    for n, spawns in ((3, [3]), (4, [3, 3])):
        for wl, cfg in CONC_WORKLOADS.items():
            for w in (4, 5, 6):
                t0 = time.time()
                res = search_concurrent(cfg, max_spawn=30, window=w, stale_tail=8,
                                        spawns=spawns, spawn_step=3, max_traces=40000)
                out["n_instance"][f"n={n}/{wl}/w={w}"] = {
                    "instances": n, "traces": res.traces,
                    "violations_by_kind": res.kinds,
                    "fenced_active_traces": res.fenced_active_traces,
                    "seconds": round(time.time() - t0, 2),
                    "inv_dur_examples": [t.summary() for t in res.dur_examples],
                }
                log(f"  n={n} {wl}/w={w} traces={res.traces:<7d} "
                    f"fenced-active={res.fenced_active_traces:<6d} "
                    f"{res.kinds or 'CLEAN'}  {time.time()-t0:.0f}s")

    log("")
    log("  fuzz, unbounded window, 5 seeds")
    for n, spawns in ((3, [3]), (4, [3, 3])):
        for wl, cfg in CONC_WORKLOADS.items():
            res = fuzz_concurrent(cfg, seeds=[1, 2, 3, 4, 5], traces_per_seed=400,
                                  spawns=spawns)
            out["fuzz"][f"n={n}/{wl}"] = {"traces": res.traces,
                                          "violations_by_kind": res.kinds}
            log(f"  n={n} {wl:8s} traces={res.traces:<6d} {res.kinds or 'CLEAN'}")

    log("")
    log("=" * 78)
    log("NEGATIVE CONTROLS")
    log("=" * 78)
    base = CONC_WORKLOADS["tiny"]
    for name, flags, kw in (
        ("NC15_epochs_descending", dict(carry_forward_epochs_descending=True), dict(spawns=[3])),
        ("NC14_same_instance_id", {}, dict(same_id=True)),
        ("NC13_carry_forward_ptr_first", CONTROLS["NC13_carry_forward_ptr_first"], dict(spawns=[3])),
    ):
        res = search_concurrent(replace(base, **flags), max_spawn=30, window=5,
                                stale_tail=8, spawn_step=3, max_traces=8000, **kw)
        out["controls"][name] = {
            "flags": flags, "kwargs": {k: v for k, v in kw.items()},
            "traces": res.traces, "violations_by_kind": res.kinds,
            "red_INV_DUR": "INV-DUR" in res.kinds, "red_INV_ONE": "INV-ONE" in res.kinds,
        }
        log(f"  {name:32s} {res.kinds or 'CLEAN'}  ({res.traces} traces)")
    return out


def run_c12(log) -> dict:
    out = {"c12": {}, "c15": {}}
    log("=" * 78)
    log("C12 -- session scoping against a reused gcs_storage_path")
    log("=" * 78)
    for wl, cfg in WORKLOADS.items():
        out["c12"][f"real/{wl}"] = run_session_scoping(cfg)
        out["c12"][f"NC9/{wl}"] = run_session_scoping(replace(cfg, session_scoped=False))
        log(f"  {wl:8s} real: executed={out['c12'][f'real/{wl}']['new_session_executed']}/"
            f"{cfg.n_units} destroyed={len(out['c12'][f'real/{wl}']['prior_keys_destroyed'])} "
            f"{out['c12'][f'real/{wl}']['violations'] or 'CLEAN'}")
        log(f"  {wl:8s} NC9 : executed={out['c12'][f'NC9/{wl}']['new_session_executed']}/"
            f"{cfg.n_units} destroyed={len(out['c12'][f'NC9/{wl}']['prior_keys_destroyed'])} "
            f"{'RED' if out['c12'][f'NC9/{wl}']['violations'] else 'GREEN'}")

    log("")
    log("=" * 78)
    log("C15 -- is base@n uniqueness load-bearing under concurrency?")
    log("=" * 78)
    base = CONC_WORKLOADS["tiny"]
    inplace = replace(base, compact_in_place=True)
    seq = search(replace(WORKLOADS["small"], compact_in_place=True), budget=2, faults=FAULTS)
    out["c15"]["sequential_crash_regression"] = {
        "traces": seq.traces, "violations_by_kind": seq.kinds}
    log(f"  in-place, sequential crashes : {seq.traces} traces {seq.kinds or 'CLEAN'}")
    for label, kw in (("2-instance", {}), ("3-instance", dict(spawns=[3]))):
        ctrl = search_concurrent(base, max_spawn=30, window=5, stale_tail=8,
                                 spawn_step=3, **kw)
        exp = search_concurrent(inplace, max_spawn=30, window=5, stale_tail=8,
                                spawn_step=3, **kw)
        out["c15"][label] = {
            "comparison_arm_unmodified": {"traces": ctrl.traces,
                                          "violations_by_kind": ctrl.kinds},
            "in_place": {"traces": exp.traces, "violations_by_kind": exp.kinds,
                         "examples": [t.summary() for t in exp.dur_examples]},
        }
        log(f"  in-place, {label:12s}: {exp.traces} traces {exp.kinds or 'CLEAN'}   "
            f"(comparison arm {ctrl.kinds or 'CLEAN'})")
    return out


def run_c2(log) -> dict:
    import math
    out = {"measurements": [], "band": [], "control": {}}

    log("=" * 78)
    log("C2 -- measured cost of the ledger (fsyncs, keys, recovery)")
    log("=" * 78)
    log(f"  {'N':>6} {'W':>5} {'K':>3} | {'fsyncs':>7} {'ceil(N/W)':>9} {'ratio':>6} "
        f"| {'2+3/K':>6} | {'maxkeys':>7} {'recovops':>8}")
    control_ok = True
    for n in (10, 100, 1000, 10000):
        for w in (1, 10, 100, 1000):
            if w > n:
                continue
            for k in (2, 4, 8, 16):
                r = measure(Cfg(n_units=n, W=w, K=k))
                if r.fsyncs != r.puts + r.deletes:
                    control_ok = False
                pred = 2 + 3.0 / k
                out["measurements"].append({
                    "n": r.n, "w": r.w, "k": r.k, "fsyncs": r.fsyncs,
                    "puts": r.puts, "deletes": r.deletes,
                    "naive_bound": r.naive_bound,
                    "overhead_ratio": round(r.overhead_ratio, 3),
                    "predicted_ratio": round(pred, 3),
                    "max_keys": r.max_keys, "final_keys": r.final_keys,
                    "recovery_ops": r.recovery_ops,
                    "fsync_seconds": round(r.fsyncs * 0.00381, 3),
                })
                if k == 8:
                    log(f"  {n:>6} {w:>5} {k:>3} | {r.fsyncs:>7} {r.naive_bound:>9} "
                        f"{r.overhead_ratio:>6.2f} | {pred:>6.2f} | "
                        f"{r.max_keys:>7} {r.recovery_ops:>8}")
    out["control"] = {"fsyncs_equal_mutations": control_ok}

    # Split the regimes. The 2+3/K formula is a statement about compaction, so
    # it can only be tested where compaction actually runs -- that is, where the
    # job produces at least K segments (N/W >= K). Configurations that never
    # compact pay only the fixed startup cost and are reported separately rather
    # than being allowed to inflate the error against a formula that does not
    # claim to describe them.
    compacting = [m for m in out["measurements"]
                  if math.ceil(m["n"] / m["w"]) >= m["k"]]
    noncompacting = [m for m in out["measurements"]
                     if math.ceil(m["n"] / m["w"]) < m["k"]]
    ratios = [m["overhead_ratio"] for m in compacting]
    errs = [abs(m["overhead_ratio"] - m["predicted_ratio"]) / m["predicted_ratio"]
            for m in compacting]
    out["overhead_summary"] = {
        "regime": "compacting (ceil(N/W) >= K)",
        "n_points": len(compacting),
        "min": round(min(ratios), 3), "max": round(max(ratios), 3),
        "mean": round(sum(ratios) / len(ratios), 3),
        "max_relative_error_vs_formula": round(max(errs), 4),
        "noncompacting_n_points": len(noncompacting),
        "noncompacting_ratio_range": [
            round(min(m["overhead_ratio"] for m in noncompacting), 3),
            round(max(m["overhead_ratio"] for m in noncompacting), 3),
        ] if noncompacting else None,
    }
    log("")
    log(f"  overhead ratio over ceil(N/W): min={out['overhead_summary']['min']} "
        f"max={out['overhead_summary']['max']} mean={out['overhead_summary']['mean']}")
    log(f"  max relative error vs the frozen 2+3/K formula: "
        f"{out['overhead_summary']['max_relative_error_vs_formula']:.2%} "
        f"({out['overhead_summary']['n_points']} compacting points)")
    log(f"  non-compacting regime (ceil(N/W) < K, fixed startup cost only): "
        f"ratio {out['overhead_summary']['noncompacting_ratio_range']}")

    keys_by_n = {}
    for m in out["measurements"]:
        keys_by_n.setdefault((m["k"],), set()).add((m["n"], m["max_keys"]))
    out["key_count_flat_in_n"] = {
        str(k[0]): sorted({mk for _, mk in v}) for k, v in keys_by_n.items()}
    log(f"  steady-state max keys by K: {out['key_count_flat_in_n']}")

    log("")
    log("=" * 78)
    log("C3 (testable half) -- the usable band")
    log("=" * 78)
    overhead = out["overhead_summary"]["mean"]
    log(f"  budget = 10% of {SAME_KEY_WRITES_PER_S:.0f} same-key writes/s; "
        f"redo <= 1% of N; measured overhead x{overhead:.2f}")
    log(f"  {'rate/s':>9} | " + " ".join(f"N={n:<7}" for n in (1000, 50000, 1000000)))
    for rate in (0.1, 1, 10, 20, 100, 1000, 10000, 100000):
        row = []
        for n in (1000, 50000, 1000000):
            b = band(rate, n, overhead)
            out["band"].append(b)
            if not b["feasible"]:
                cell = "NONE"
            elif len(b["usable_W"]) > 2:
                cell = f"{b['min_W']}..{b['max_W']}"
            else:
                cell = ",".join(str(x) for x in b["usable_W"])
            row.append(f"{cell:<12}")
        log(f"  {rate:>9} | " + " ".join(row))
    return out


def run_gaps(log) -> dict:
    out = {"f6_stale_tail": {}, "f7_instances": {}, "depth": {}, "controls": {}}
    base = CONC_WORKLOADS["tiny"]

    log("=" * 78)
    log("F6 -- does a longer stale tail expose anything new?")
    log("=" * 78)
    for tail in (8, 16, 32, 64):
        for n, spawns in ((2, None), (3, [3])):
            kw = {} if spawns is None else dict(spawns=spawns)
            res = search_concurrent(base, max_spawn=24, window=4, stale_tail=tail,
                                    spawn_step=3, max_traces=20000, **kw)
            bad = {k: v for k, v in res.kinds.items() if k != "INV-FENCE-ACK"}
            out["f6_stale_tail"][f"tail={tail}/n={n}"] = {
                "traces": res.traces, "violations_by_kind": res.kinds,
                "violations_excluding_fence_ack": bad,
                "fenced_active_traces": res.fenced_active_traces,
                "max_tail_actually_used": res.max_tail_used,
                "traces_hitting_the_cap": res.traces_hitting_cap}
            log(f"  tail={tail:<3d} n={n}  traces={res.traces:<6d} "
                f"fenced-active={res.fenced_active_traces:<6d} "
                f"max-tail-used={res.max_tail_used:<3d} "
                f"hit-cap={res.traces_hitting_cap:<5d} {bad or 'CLEAN'}")

    log("")
    log("=" * 78)
    log("F7 -- 5 and 6 concurrent incarnations")
    log("=" * 78)
    for n, spawns in ((5, [3, 3, 3]), (6, [3, 3, 3, 3])):
        for w in (4, 5):
            t0 = time.time()
            res = search_concurrent(base, max_spawn=21, window=w, stale_tail=8,
                                    spawns=spawns, spawn_step=3, max_traces=30000)
            bad = {k: v for k, v in res.kinds.items() if k != "INV-FENCE-ACK"}
            out["f7_instances"][f"n={n}/w={w}"] = {
                "traces": res.traces, "violations_by_kind": res.kinds,
                "violations_excluding_fence_ack": bad,
                "fenced_active_traces": res.fenced_active_traces,
                "max_tail_actually_used": res.max_tail_used,
                "seconds": round(time.time() - t0, 2)}
            log(f"  n={n} w={w}  traces={res.traces:<6d} "
                f"fenced-active={res.fenced_active_traces:<6d} {bad or 'CLEAN'} "
                f"{time.time()-t0:.0f}s")

    log("")
    log("=" * 78)
    log("DEPTH -- sequential crash enumeration at fault budget 3")
    log("=" * 78)
    t0 = time.time()
    res = search(Cfg(n_units=4, W=1, K=2), budget=3, faults=FAULTS)
    out["depth"]["budget3"] = {"traces": res.traces, "violations_by_kind": res.kinds,
                               "seconds": round(time.time() - t0, 2)}
    log(f"  budget=3  traces={res.traces} {res.kinds or 'CLEAN'} {time.time()-t0:.0f}s")

    log("")
    log("=" * 78)
    log("CONTROLS -- must be red at EVERY configuration, not just the old ones")
    log("=" * 78)
    for name, flags in (("NC15_epochs_descending", dict(carry_forward_epochs_descending=True)),
                        ("NC13_carry_forward_ptr_first", dict(carry_forward_ptr_first=True))):
        # Sweep size matters for whether a control can fire at all. NC13's bug
        # needs a deeper interleaving than NC15's, and at max_spawn=21/window=4
        # it came back GREEN in runs/20260910-053138-GAPS -- which voided that
        # run under its own pre-registered rule. The rig was not blind (NC15 was
        # red at the same settings); the control was simply starved. So the
        # controls run at the sweep size where each is KNOWN to fire.
        for label, kw in (("n=3,tail=64", dict(spawns=[3], stale_tail=64)),
                          ("n=5,tail=8", dict(spawns=[3, 3, 3], stale_tail=8))):
            res = search_concurrent(replace(base, **flags), max_spawn=45, window=6,
                                    spawn_step=1, max_traces=20000, **kw)
            red = "INV-DUR" in res.kinds
            out["controls"][f"{name}/{label}"] = {
                "traces": res.traces, "violations_by_kind": res.kinds, "red": red}
            log(f"  {name:30s} {label:12s} {'RED ' if red else 'GREEN'} {res.kinds}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment", required=True, choices=["C6", "C1", "C16", "C13", "C20", "C12", "C2", "GAPS"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--budgets", default="1,2")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    log = Tee(os.path.join(args.out, "stdout.log"))
    budgets = [int(x) for x in args.budgets.split(",")]

    cfgdump = {
        "experiment": args.experiment,
        "git_rev": git_rev(),
        "python": sys.version.split()[0],
        "budgets": budgets,
        "faults": list(FAULTS),
        "workloads": {k: asdict(v) for k, v in WORKLOADS.items()},
        "controls": CONTROLS,
        "seeds": "n/a -- deterministic exhaustive DFS, no randomness",
        "started": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    with open(os.path.join(args.out, "config.json"), "w") as f:
        json.dump(cfgdump, f, indent=2)

    log(f"experiment={args.experiment} git={cfgdump['git_rev'][:12]} budgets={budgets}")
    log("")
    if args.experiment == "C6":
        body = run_c6(log, budgets)
    elif args.experiment == "C1":
        body = run_c1(log)
    elif args.experiment == "C16":
        body = run_c1(log, windows=(4, 6, 8), seeds=(1, 2, 3, 4, 5),
                      controls=C16_CONTROLS)
        log("")
        log("  regression: the C6 sequential sweep, unchanged")
        reg = search(WORKLOADS["small"], budget=2, faults=FAULTS)
        body["c6_regression"] = {"traces": reg.traces, "violations_by_kind": reg.kinds}
        log(f"  C6 regression: {reg.traces} traces {reg.kinds or 'CLEAN'}")
    elif args.experiment == "GAPS":
        body = run_gaps(log)
        bad = {}
        for sect in ("f6_stale_tail", "f7_instances"):
            for v in body[sect].values():
                for k, n in v["violations_excluding_fence_ack"].items():
                    bad[k] = bad.get(k, 0) + n
        for k, n in body["depth"]["budget3"]["violations_by_kind"].items():
            bad[k] = bad.get(k, 0) + n
        greens = [k for k, v in body["controls"].items() if not v["red"]]
        # A stale-tail sweep proves nothing if the cap was never binding.
        inert = [k for k, v in body["f6_stale_tail"].items()
                 if v.get("traces_hitting_the_cap", 0) == 0]
        body["f6_inert_configurations"] = inert
        if inert and len(inert) == len(body["f6_stale_tail"]):
            verdict, why = "VOID", (
                "the stale-tail cap was never binding in any configuration, so "
                f"the F6 sweep tested nothing: {inert[:4]}")
        elif greens:
            verdict, why = "VOID", f"control(s) went green at scale: {greens}"
        elif bad:
            verdict, why = "gaps-confirmed-as-real", f"new violations found: {bad}"
        else:
            verdict, why = "gaps-narrowed", (
                "no violation at stale tails up to 64, up to 6 concurrent "
                "incarnations, or at sequential fault budget 3; all controls red")
        results = {"experiment": "GAPS", "verdict": verdict, "why": why,
                   "config": cfgdump, "new_violations": bad, **body}
        with open(os.path.join(args.out, "results.json"), "w") as f:
            json.dump(results, f, indent=2)
        log(""); log("=" * 78); log(f"VERDICT: {verdict} -- {why}"); log("=" * 78)
        return 0
    elif args.experiment == "C2":
        body = run_c2(log)
        if not body["control"]["fsyncs_equal_mutations"]:
            verdict, why = "VOID", "fsync counter disagrees with the mutation count"
        else:
            mult = body["overhead_summary"]["mean"] > 1.5
            flat = all(len(v) <= 2 for v in body["key_count_flat_in_n"].values())
            feasible = [b for b in body["band"] if b["feasible"]]
            c2 = "refuted" if mult else "proven"
            c3 = "proven" if feasible else "refuted"
            verdict = f"C2={c2} C3half={c3}"
            why = (f"write overhead is MULTIPLICATIVE at x{body['overhead_summary']['mean']:.2f} "
                   f"(min x{body['overhead_summary']['min']:.2f}), not additive; "
                   f"key count flat in N: {flat}; "
                   f"{len(feasible)}/{len(body['band'])} (rate,N) points have a usable W")
        results = {"experiment": "C2", "verdict": verdict, "why": why,
                   "config": cfgdump, **body}
        with open(os.path.join(args.out, "results.json"), "w") as f:
            json.dump(results, f, indent=2)
        log("")
        log("=" * 78)
        log(f"VERDICT: {verdict} -- {why}")
        log("=" * 78)
        return 0
    elif args.experiment == "C12":
        body = run_c12(log)
        c12_bad = any(v["violations"] for k, v in body["c12"].items() if k.startswith("real/"))
        nc9_red = all(v["violations"] for k, v in body["c12"].items() if k.startswith("NC9/"))
        arms_clean = all(
            not v["comparison_arm_unmodified"]["violations_by_kind"].get("INV-DUR")
            for k, v in body["c15"].items() if k.endswith("instance"))
        inplace_dur = sum(
            v["in_place"]["violations_by_kind"].get("INV-DUR", 0)
            for k, v in body["c15"].items() if k.endswith("instance"))
        inplace_dur += body["c15"]["sequential_crash_regression"][
            "violations_by_kind"].get("INV-DUR", 0)
        if not nc9_red:
            verdict, why = "VOID", "NC9 (no session scoping) did not go red"
        elif not arms_clean:
            verdict, why = "VOID", "the C15 comparison arm is not clean"
        else:
            c12 = "refuted" if c12_bad else "proven"
            c15 = "proven" if inplace_dur else "refuted"
            verdict = f"C12={c12} C15={c15}"
            why = (f"C12: new session executed every unit and destroyed nothing; "
                   f"NC9 red. C15: in-place base produced {inplace_dur} INV-DUR "
                   f"under crashes and concurrency")
        results = {"experiment": "C12", "verdict": verdict, "why": why,
                   "config": cfgdump, **body}
        with open(os.path.join(args.out, "results.json"), "w") as f:
            json.dump(results, f, indent=2)
        log("")
        log("=" * 78)
        log(f"VERDICT: {verdict} -- {why}")
        log("=" * 78)
        return 0
    elif args.experiment == "C20":
        body = run_c20(log)
        n3 = {}
        n4 = {}
        for k, v in body["n_instance"].items():
            tgt = n3 if v["instances"] == 3 else n4
            for kk, nn in v["violations_by_kind"].items():
                tgt[kk] = tgt.get(kk, 0) + nn
        for k, v in body["fuzz"].items():
            tgt = n3 if k.startswith("n=3") else n4
            for kk, nn in v["violations_by_kind"].items():
                tgt[kk] = tgt.get(kk, 0) + nn
        regr = {}
        for v in body["regressions"].values():
            for kk, nn in v["violations_by_kind"].items():
                regr[kk] = regr.get(kk, 0) + nn
        c = body["controls"]
        if not c["NC15_epochs_descending"]["red_INV_DUR"]:
            verdict, why = "VOID", "NC15 (descending epoch scan) did not go red"
        elif regr.get("INV-DUR"):
            verdict, why = "VOID", f"a regression sweep shows INV-DUR: {regr}"
        elif n3.get("INV-DUR"):
            verdict, why = "refuted", f"INV-DUR at three instances: {n3}"
        elif n4.get("INV-DUR"):
            verdict, why = "uncertain", (
                f"clean at three instances, {n4['INV-DUR']} INV-DUR at four -- "
                "the ascending scan is incomplete rather than wrong")
        else:
            verdict, why = "proven", (
                f"zero INV-DUR at 3 and 4 instances; n3={n3}; n4={n4}; "
                f"regressions {regr or 'clean'}; NC15 red")
        results = {"experiment": "C20", "verdict": verdict, "why": why,
                   "config": cfgdump, "n3_totals": n3, "n4_totals": n4,
                   "regression_totals": regr, **body}
        with open(os.path.join(args.out, "results.json"), "w") as f:
            json.dump(results, f, indent=2)
        log("")
        log("=" * 78)
        log(f"VERDICT: {verdict} -- {why}")
        log("=" * 78)
        return 0
    else:
        body = run_c13(log)
        two = {}
        for v in body["two_instance_regression"].values():
            for k, n in v["violations_by_kind"].items():
                two[k] = two.get(k, 0) + n
        three = {}
        for sect in ("three_instance", "fuzz"):
            for v in body[sect].values():
                for k, n in v["violations_by_kind"].items():
                    three[k] = three.get(k, 0) + n
        c = body["controls"]
        if not c["NC14_same_instance_id"]["red_INV_ONE"]:
            verdict, why = "VOID", "NC14 (shared instance id) did not produce INV-ONE"
        elif two.get("INV-DUR"):
            verdict, why = "VOID", ("two-instance regression shows INV-DUR: the "
                                    "N-instance refactor broke the C17 fix, so the "
                                    "three-instance result is uninterpretable")
        else:
            c13 = "refuted" if three.get("INV-ONE") or two.get("INV-ONE") else "proven"
            c19 = "refuted" if three.get("INV-DUR") else "proven"
            verdict = f"C13={c13} C19={c19}"
            why = (f"2-instance {two or 'clean'}; 3-instance {three or 'clean'}; "
                   f"NC14 red on INV-ONE")
        results = {"experiment": "C13", "verdict": verdict, "why": why,
                   "config": cfgdump, "two_totals": two, "three_totals": three, **body}
        with open(os.path.join(args.out, "results.json"), "w") as f:
            json.dump(results, f, indent=2)
        log("")
        log("=" * 78)
        log(f"VERDICT: {verdict} -- {why}")
        log("=" * 78)
        return 0

    if args.experiment in ("C1", "C16"):
        kinds_all = {}
        for sect in ("real", "fuzz"):
            for v in body[sect].values():
                for k, n in v["violations_by_kind"].items():
                    kinds_all[k] = kinds_all.get(k, 0) + n
        coverage = sum(v["fenced_active_traces"] for v in body["real"].values())
        # The C1 card predicted NC2 would fire INV-ONE. It does not: with the
        # inverted return, every instance concludes it lost, so the failure is
        # total livelock rather than split brain. The control is still valid --
        # it detects the bug -- but via INV-LIVE. Recorded here rather than
        # silently relaxed; see journal iteration 2.
        controls_ok = (
            body["controls"]["NC1_fence_ignored"]["went_red_INV_ONE"]
            and body["controls"]["NC2_fence_inverted"]["went_red_any_fencing"]
        )
        if args.experiment == "C16":
            controls_ok = controls_ok and body["controls"][
                "NC13_carry_forward_ptr_first"]["went_red_INV_DUR"]
        if not controls_ok:
            verdict, why = "VOID", "a fencing control failed to go red"
        elif coverage < 500:
            verdict, why = "INCONCLUSIVE", f"only {coverage} traces reached a fenced instance"
        elif args.experiment == "C16":
            reg_bad = bool(body["c6_regression"]["violations_by_kind"])
            c16 = "refuted" if kinds_all.get("INV-DUR") else "proven"
            verdict = f"C16prime={c16}" + (" C6-REGRESSED" if reg_bad else "")
            why = (f"INV-DUR={kinds_all.get('INV-DUR', 0)}, "
                   f"INV-FENCE-ACK={kinds_all.get('INV-FENCE-ACK', 0)}, "
                   f"INV-ONE={kinds_all.get('INV-ONE', 0)}, coverage={coverage}, "
                   f"C6 regression {'FAILED' if reg_bad else 'clean'}")
        else:
            c1 = "refuted" if kinds_all.get("INV-ONE") else "proven"
            c16 = "refuted" if kinds_all.get("INV-DUR") else "proven"
            verdict = f"C1={c1} C16={c16}"
            why = (f"INV-ONE={kinds_all.get('INV-ONE', 0)}, "
                   f"INV-DUR={kinds_all.get('INV-DUR', 0)}, "
                   f"INV-FENCE-ACK={kinds_all.get('INV-FENCE-ACK', 0)}, "
                   f"coverage={coverage} fenced-active traces")
        results = {"experiment": args.experiment, "verdict": verdict, "why": why,
                   "config": cfgdump, "totals": kinds_all, "coverage": coverage, **body}
        with open(os.path.join(args.out, "results.json"), "w") as f:
            json.dump(results, f, indent=2)
        log("")
        log("=" * 78)
        log(f"VERDICT: {verdict} -- {why}")
        log("=" * 78)
        return 0

    real_violations = {
        k: v["violations_by_kind"] for k, v in body["real"].items() if v["violations_by_kind"]
    }
    green_voiding = [
        n
        for n, c in body["controls"].items()
        if c["voids_run_if_green"] and not c["went_red"]
    ]

    if green_voiding:
        verdict = "VOID"
        why = f"negative control(s) passed: {green_voiding}"
    elif real_violations:
        verdict = "refuted"
        why = f"real protocol violated invariants: {real_violations}"
    else:
        verdict = "proven"
        why = "no violation across the exhaustive sweep; all voiding controls red"

    results = {
        "experiment": args.experiment,
        "verdict": verdict,
        "why": why,
        "config": cfgdump,
        **body,
    }
    with open(os.path.join(args.out, "results.json"), "w") as f:
        json.dump(results, f, indent=2)

    log("")
    log("=" * 78)
    log(f"VERDICT: {verdict} -- {why}")
    log("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
