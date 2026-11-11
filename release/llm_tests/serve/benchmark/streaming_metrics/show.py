import sys
import json

for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        if line:
            print(line)
        continue
    try:
        d = json.loads(line)
    except Exception:
        continue
    print(
        d["dir"],
        {
            k: d[k]
            for k in [
                "steady_requests",
                "steady_inflight",
                "throttled_in_window",
                "failed",
                "chunks_per_token",
            ]
        },
    )
    h = d.get("haproxy")
    if h:
        print(
            "   haproxy",
            {k: (round(v, 3) if isinstance(v, float) else v) for k, v in h.items()},
        )
    for m in ["ttft_ms", "tpot_ms", "itl_ms", "e2e_s"]:
        print("  ", m, {k: round(v, 3) for k, v in d[m].items()})
