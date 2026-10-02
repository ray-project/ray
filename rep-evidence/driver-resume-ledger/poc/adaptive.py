"""C32: is adaptive coalescing worth the complexity?

Fixed W has an asymmetry: write rate = r/W grows with the completion rate, while
the redo TIME window ~ W/r grows as the rate FALLS.  So one fixed W over-exposes
the slow phase in time, and another overshoots the write ceiling when things
speed up.  Adaptive W = ceil(r/target) holds the write rate by construction and
shrinks W when r falls.

That argument sounds airtight, which is exactly why the `constant` profile is a
negative control: with no rate variation there is nothing to exploit, so adaptive
MUST NOT beat fixed W there.  If it does, the fixed baseline was mistuned and the
comparison is rigged.

Pre-registered in experiments/C32.md.  Stdlib only.
"""
from __future__ import annotations

import json
import math
import os
import sys

FSYNC_SECONDS = 0.00381                 # REP-64 p50 on ext4
SAME_KEY_CEILING = 1.0 / FSYNC_SECONDS  # ~262 writes/s
TARGET_WRITE_RATE = 0.10 * SAME_KEY_CEILING   # 26.2/s, consistent with C3
W_MAX = 4096
DT = 0.5                                # trace resolution, seconds
DURATION = 600.0


def profile(name: str):
    """Completion rate r(t), units/second."""
    n = int(DURATION / DT)
    if name == "constant":
        return [(i * DT, 200.0) for i in range(n)]
    if name == "step":
        return [(i * DT, 50.0 if i * DT < DURATION / 2 else 500.0)
                for i in range(n)]
    if name == "ramp":
        return [(i * DT, 5.0 * (100.0 ** (i / max(1, n - 1))))
                for i in range(n)]
    if name == "bursty":
        return [(i * DT, 500.0 if (int(i * DT) // 20) % 2 == 0 else 20.0)
                for i in range(n)]
    raise ValueError(name)


def simulate(trace, policy: str, w_fixed: int = 1, tau: float = 5.0):
    """Walk the trace, emitting a commit every W completed units.

    Returns per-sample write rate and redo exposure.  Redo is measured two ways
    on purpose: in UNITS (what C2/C10 budget) and in TIME (how much wall-clock
    work a crash throws away).  Adaptive optimises the second, not the first,
    and the design must not claim both.
    """
    pending = 0.0
    w_now = w_fixed
    r_hat = trace[0][1]
    write_rates, w_hist, redo_units, redo_time = [], [], [], []
    for _, r in trace:
        if policy == "adaptive":
            # EWMA of the observed completion rate; tau seconds of memory.
            alpha = DT / max(DT, tau)
            r_hat = (1 - alpha) * r_hat + alpha * r
            w_now = max(1, min(W_MAX, math.ceil(r_hat / TARGET_WRITE_RATE)))
        else:
            w_now = w_fixed
        pending += r * DT
        commits = math.floor(pending / w_now)
        pending -= commits * w_now
        write_rates.append(commits / DT)
        w_hist.append(w_now)
        # Worst-case exposure at this instant.
        redo_units.append(w_now - 1)
        redo_time.append((w_now - 1) / max(1e-9, r))
    return {
        "write_rate_mean": sum(write_rates) / len(write_rates),
        "write_rate_p95": sorted(write_rates)[int(0.95 * (len(write_rates) - 1))],
        "write_rate_max": max(write_rates),
        "redo_units_max": max(redo_units),
        "redo_time_max": max(redo_time),
        "redo_time_p95": sorted(redo_time)[int(0.95 * (len(redo_time) - 1))],
        "w_min": min(w_hist), "w_max": max(w_hist),
    }


def tune_fixed(trace, mode: str) -> int:
    """W that puts the ledger exactly at target for the mean or the peak rate."""
    rates = [r for _, r in trace]
    r = (sum(rates) / len(rates)) if mode == "mean" else max(rates)
    return max(1, min(W_MAX, math.ceil(r / TARGET_WRITE_RATE)))


def main() -> int:
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    log_lines = []

    def log(*a):
        line = " ".join(str(x) for x in a)
        print(line, flush=True)
        log_lines.append(line)

    res = {"config": {"target_write_rate": round(TARGET_WRITE_RATE, 2),
                      "same_key_ceiling": round(SAME_KEY_CEILING, 1),
                      "duration_s": DURATION, "dt": DT, "tau_s": 5.0},
           "profiles": {}}

    for name in ("constant", "step", "ramp", "bursty"):
        tr = profile(name)
        wm, wp = tune_fixed(tr, "mean"), tune_fixed(tr, "peak")
        arms = {
            "fixed_mean": simulate(tr, "fixed", wm),
            "fixed_peak": simulate(tr, "fixed", wp),
            "adaptive": simulate(tr, "adaptive"),
        }
        rates = [r for _, r in tr]
        p = {"W_fixed_mean": wm, "W_fixed_peak": wp,
             "rate_min": min(rates), "rate_max": max(rates), "arms": arms}
        for k, v in arms.items():
            dev = abs(v["write_rate_mean"] - TARGET_WRITE_RATE) / TARGET_WRITE_RATE
            v["write_rate_dev_frac"] = round(dev, 4)
            v["over_ceiling"] = v["write_rate_p95"] > SAME_KEY_CEILING
        res["profiles"][name] = p
        log("")
        log("=" * 70)
        log(f"profile {name}: rate {min(rates):.0f}..{max(rates):.0f}/s, "
            f"W_mean={wm} W_peak={wp}")
        log("=" * 70)
        for k, v in arms.items():
            log(f"  {k:11s} wr_mean={v['write_rate_mean']:7.2f}/s "
                f"dev={v['write_rate_dev_frac']:6.2%} "
                f"redo_units_max={v['redo_units_max']:6d} "
                f"redo_time_max={v['redo_time_max']:7.2f}s "
                f"W={v['w_min']}..{v['w_max']}")

    def a(p, arm, k):
        return res["profiles"][p]["arms"][arm][k]

    # Control: on a constant profile adaptive must TIE fixed_mean.
    tie_wr = abs(a("constant", "adaptive", "write_rate_mean")
                 - a("constant", "fixed_mean", "write_rate_mean")) \
        / max(1e-9, a("constant", "fixed_mean", "write_rate_mean"))
    tie_rt = abs(a("constant", "adaptive", "redo_time_max")
                 - a("constant", "fixed_mean", "redo_time_max")) \
        / max(1e-9, a("constant", "fixed_mean", "redo_time_max"))

    varying = ("step", "ramp", "bursty")
    adaptive_holds_rate = all(
        a(p, "adaptive", "write_rate_dev_frac") <= 0.20 for p in varying)
    # Does a fixed policy bound BOTH on every varying profile?
    def fixed_bounds_both(p, arm):
        return (a(p, arm, "write_rate_dev_frac") <= 0.20
                and a(p, arm, "redo_time_max")
                <= a(p, "adaptive", "redo_time_max") * 1.05)
    fixed_matches_everywhere = all(
        fixed_bounds_both(p, "fixed_mean") or fixed_bounds_both(p, "fixed_peak")
        for p in varying)
    redo_time_win = {p: round(a(p, "fixed_peak", "redo_time_max")
                              / max(1e-9, a(p, "adaptive", "redo_time_max")), 2)
                     for p in varying}

    res["verdict"] = {
        "NC_constant_write_rate_tie_frac": round(tie_wr, 4),
        "NC_constant_redo_time_tie_frac": round(tie_rt, 4),
        "NC_constant_is_a_tie": tie_wr <= 0.05 and tie_rt <= 0.05,
        "VOID_adaptive_won_on_constant": tie_wr > 0.05 or tie_rt > 0.05,
        "adaptive_write_rate_dev": {p: a(p, "adaptive", "write_rate_dev_frac")
                                    for p in varying},
        "adaptive_holds_target_rate": adaptive_holds_rate,
        "fixed_mean_dev": {p: a(p, "fixed_mean", "write_rate_dev_frac")
                           for p in varying},
        "fixed_peak_redo_time_vs_adaptive": redo_time_win,
        "fixed_matches_adaptive_everywhere": fixed_matches_everywhere,
        "adaptive_redo_units_max": {p: a(p, "adaptive", "redo_units_max")
                                    for p in varying},
        "fixed_peak_redo_units_max": {p: a(p, "fixed_peak", "redo_units_max")
                                      for p in varying},
        "C32_proven": (tie_wr <= 0.05 and tie_rt <= 0.05
                       and adaptive_holds_rate
                       and not fixed_matches_everywhere),
    }
    log(""); log("=" * 70)
    for k, v in res["verdict"].items():
        log(f"  {k:38s} {v}")
    log("=" * 70)
    with open(os.path.join(out_dir, "results.json"), "w") as f:
        json.dump(res, f, indent=2)
    with open(os.path.join(out_dir, "stdout.log"), "w") as f:
        f.write("\n".join(log_lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
