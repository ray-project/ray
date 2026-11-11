"""HAProxy metric counts and means against the client for every ON run.

Usage: python correctness.py RUN_DIR [RUN_DIR ...]
"""
import glob
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from bench import analyze_dir

rows = []
for root in sys.argv[1:]:
    for d in sorted(glob.glob(root + "/r*_c*_on*_[0-9]*")):
        if not re.search(r"_(on|ong)_\d+$", d):
            continue
        h = analyze_dir(d)["haproxy"]
        if h is None:
            continue
        rows.append((d.replace("/mnt/local_storage/sm/runs/", ""), h))
bad = 0
deficits, itl_rel, ttft_gap, req_gap = [], [], [], []
for name, h in rows:
    ok = (
        h["ttft_count"]
        == h["client_requests"]
        == h["request_count"]
        == h["routing_count"]
        and h["errors"] == 0
        and 0
        <= h["client_itl_tokens"] - h["itl_count"]
        <= 0.001 * h["client_itl_tokens"]
    )
    bad += not ok
    if not ok:
        print("MISMATCH", name, h)
    deficits.append(h["client_itl_tokens"] - h["itl_count"])
    itl_rel.append(abs(h["itl_mean_ms"] / h["client_tpot_mean_ms"] - 1) * 100)
    ttft_gap.append(h["client_ttft_mean_ms"] - h["ttft_mean_ms"])
    req_gap.append((h["client_e2e_mean_s"] - h["request_mean_s"]) * 1e3)
print(
    f"{len(rows)} ON runs, {bad} mismatches; ITL deficit {min(deficits)}-{max(deficits)} tokens "
    f'of up to {max(h["client_itl_tokens"] for _, h in rows)}; |HAProxy mean ITL / client mean TPOT - 1| max {max(itl_rel):.4f}%; '
    f"client minus HAProxy TTFT {min(ttft_gap):.2f}-{max(ttft_gap):.2f} ms; client minus HAProxy request {min(req_gap):.2f}-{max(req_gap):.2f} ms"
)
