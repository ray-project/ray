"""Summarize abtest.py output: per point, OFF vs ON means, spread and paired deltas."""
import json
import re
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from bench import analyze_dir  # noqa: E402

METRICS = [
    ("ttft_ms", "mean"),
    ("ttft_ms", "p50"),
    ("ttft_ms", "p90"),
    ("tpot_ms", "mean"),
    ("tpot_ms", "p90"),
    ("itl_ms", "mean"),
    ("itl_ms", "p50"),
    ("itl_ms", "p90"),
    ("e2e_s", "mean"),
]


def main(root):
    root = Path(root)
    runs = {}
    for d in sorted(root.iterdir()):
        m = re.match(r"(r\d+_c\d+)_((?:on|off|py|hap)(?:-[\w]+)?)_(\d+)$", d.name)
        if m and ((d / "client0.json").exists() or (d / "client0.json.gz").exists()):
            result = analyze_dir(d)
            if (d / "component_cpu.json").exists():
                result["cpu"] = json.loads((d / "component_cpu.json").read_text())
            runs.setdefault(m.group(1), {}).setdefault(m.group(2), {})[
                int(m.group(3))
            ] = result
    summary = {}
    for point, modes in runs.items():
        off_key = next(k for k in modes if k.startswith("off"))
        for on_key in [k for k in modes if not k.startswith("off")]:
            report(point, off_key, on_key, modes, summary)
    (root / "summary.json").write_text(json.dumps(summary, indent=1))


def report(point, off_key, on_key, modes, summary):
    if True:
        off, on = modes[off_key], modes[on_key]
        cycles = sorted(set(off) & set(on))
        print(f"\n=== {point}: {on_key} vs {off_key}, {len(cycles)} paired cycles ===")
        print(
            "metric".ljust(16)
            + "OFF mean".rjust(10)
            + "ON mean".rjust(10)
            + "delta%".rjust(9)
            + "OFF spread%".rjust(13)
            + "ON spread%".rjust(12)
            + "  paired deltas %"
        )
        summary.setdefault(point, {})[on_key] = entry = {}
        for m, q in METRICS:
            o = np.array([off[c][m][q] for c in cycles])
            n = np.array([on[c][m][q] for c in cycles])
            delta = (n.mean() / o.mean() - 1) * 100
            paired = (n / o - 1) * 100
            spread_o = (o.max() - o.min()) / o.mean() * 100
            spread_n = (n.max() - n.min()) / n.mean() * 100
            entry[f"{m}.{q}"] = {
                "off": o.tolist(),
                "on": n.tolist(),
                "delta_pct": delta,
                "paired_pct": paired.tolist(),
            }
            print(
                f"{m}.{q}".ljust(16)
                + f"{o.mean():10.3f}{n.mean():10.3f}{delta:+8.2f}%"
                + f"{spread_o:12.2f}%{spread_n:11.2f}%  "
                + " ".join(f"{p:+.2f}" for p in paired)
            )
        for component in ("frontend_s", "haproxy_s"):
            o = [off[c].get("cpu", {}).get(component) for c in cycles]
            n = [on[c].get("cpu", {}).get(component) for c in cycles]
            if None not in o + n and cycles:
                o, n = np.array(o), np.array(n)
                print(
                    f"cpu.{component}".ljust(16) + f"{o.mean():10.2f}{n.mean():10.2f}"
                    f"{(n.mean() / o.mean() - 1) * 100:+8.2f}%  (CPU seconds per run, same requests)"
                )
        for c in cycles if on_key.startswith("on") else []:
            h = on[c]["haproxy"]
            gap = h["client_itl_tokens"] - h["itl_count"]
            ok = (
                h["ttft_count"]
                == h["client_requests"]
                == h["request_count"]
                == h["routing_count"]
                and 0 <= gap <= 0.001 * h["client_itl_tokens"]
                and h["errors"] == 0
            )
            print(
                f"   cycle {c} ON counts {'OK' if ok else 'MISMATCH'}: ttft={h['ttft_count']:.0f}/{h['client_requests']} "
                f"itl={h['itl_count']:.0f}/{h['client_itl_tokens']} (first-chunk batching {gap:.0f}) "
                f"err={h['errors']:.0f} | haproxy ttft {h['ttft_mean_ms']:.2f} vs client {h['client_ttft_mean_ms']:.2f} ms, "
                f"itl {h['itl_mean_ms']:.4f} vs tpot {h['client_tpot_mean_ms']:.4f} ms"
            )
            oh = off[c]["haproxy"]
            if oh and (oh["ttft_count"] or oh["itl_count"]):
                print(
                    f"   cycle {c} OFF unexpectedly recorded {oh['ttft_count']:.0f} TTFT / {oh['itl_count']:.0f} ITL"
                )


if __name__ == "__main__":
    main(sys.argv[1])
