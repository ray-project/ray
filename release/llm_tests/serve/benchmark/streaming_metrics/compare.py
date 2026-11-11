"""Compare rounds: python compare.py off:runs/a off:runs/b on:runs/c ...

Prints per load point the steady-state metrics of every round, each round's
change against the mean of the OFF rounds, and the OFF-to-OFF spread.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from bench import analyze_dir  # noqa: E402

POINTS = ["r6_c64", "r18_c128", "r36_c256"]
METRICS = [
    ("ttft_ms", "p50"),
    ("ttft_ms", "p90"),
    ("ttft_ms", "mean"),
    ("tpot_ms", "mean"),
    ("tpot_ms", "p90"),
    ("itl_ms", "p50"),
    ("itl_ms", "p90"),
    ("e2e_s", "mean"),
]


def main():
    rounds = [arg.split(":", 1) for arg in sys.argv[1:]]
    report = {}
    for point in POINTS:
        rows = []
        for label, path in rounds:
            d = Path(path) / point
            if not ((d / "client0.json").exists() or (d / "client0.json.gz").exists()):
                continue
            a = analyze_dir(d)
            rows.append((label, path, a))
        if not rows:
            continue
        offs = [a for label, _, a in rows if label == "off"]
        print(f"\n=== {point} ===")
        header = "round".ljust(34) + "".join(
            f"{m[:4]}_{q}".rjust(11) for m, q in METRICS
        )
        print(header)
        base = {}
        for m, q in METRICS:
            vals = [a[m][q] for a in offs]
            base[(m, q)] = sum(vals) / len(vals) if vals else None
        for label, path, a in rows:
            line = f"{label}:{Path(path).name}".ljust(34)
            line += "".join(f"{a[m][q]:11.3f}" for m, q in METRICS)
            print(line)
        print("-- % vs mean(OFF)")
        for label, path, a in rows:
            line = f"{label}:{Path(path).name}".ljust(34)
            for m, q in METRICS:
                b = base[(m, q)]
                line += f"{(a[m][q] / b - 1) * 100:+10.2f}%" if b else " " * 11
            print(line)
        for label, path, a in rows:
            h = a.get("haproxy")
            if label != "off" and h:
                # First chunks carrying >1 token add no ITL (Dynamo semantics),
                # so ITL count may trail sum(output - 1) by those extra tokens.
                gap = h["client_itl_tokens"] - h["itl_count"]
                ok = (
                    h["ttft_count"] == h["client_requests"]
                    and h["request_count"] == h["client_requests"]
                    and 0 <= gap <= 0.001 * h["client_itl_tokens"]
                    and h["errors"] == 0
                )
                print(
                    f"   {Path(path).name}: counts {'OK' if ok else 'MISMATCH'} "
                    f"ttft={h['ttft_count']:.0f}/{h['client_requests']} "
                    f"itl={h['itl_count']:.0f}/{h['client_itl_tokens']} "
                    f"req={h['request_count']:.0f} routing={h['routing_count']:.0f} "
                    f"err={h['errors']:.0f} | haproxy ttft={h['ttft_mean_ms']:.2f}ms "
                    f"(client {h['client_ttft_mean_ms']:.2f}) itl={h['itl_mean_ms']:.4f}ms "
                    f"(client tpot {h['client_tpot_mean_ms']:.4f}) "
                    f"routing={h['routing_mean_ms']:.3f}ms"
                )
        report[point] = {
            f"{label}:{path}": {f"{m}.{q}": a[m][q] for m, q in METRICS}
            for label, path, a in rows
        }
    Path("last_compare.json").write_text(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
