"""Manual smoke test for the PromQL aggregations the forwarder relies on.

This is **not** a pytest test (filename intentionally lacks ``test_`` prefix
so CI does not auto-discover it). It is a manually run script that talks to
a real Prometheus and asserts the windowed aggregation invariants the
forwarder depends on:

    avg <= p95 <= max     (must hold for any window)
    max - avg > 0          (proves the windowed aggregation actually saw
                            varying samples for at least one node)

Decoupled from the forwarder code path so it can be used to de-risk the
PromQL contract **before** the forwarder is installed in a given
environment.

Usage::

    # OSS / typical Ray setup — Prometheus on localhost:9090
    python verify_prometheus_aggregates.py

    # Override (e.g. Anyscale customer-monitoring ingress)
    RAY_PROMETHEUS_HOST=http://localhost:9481 python verify_prometheus_aggregates.py

Findings (2026-05-14, Anyscale workspace, Ray 2.55.1):

- On Anyscale workspaces, ``localhost:9090`` is the ``anyscaled`` daemon's
  own metrics exporter, **not** a Prometheus query API. The real query
  endpoint is ``localhost:9481`` (the customer-monitoring ingress).
- The ingress is org-shared. A query without a ``SessionName`` /
  ``ClusterId`` selector returned thousands of series across every
  running cluster — which is exactly why the forwarder filters by
  ``SessionName`` (see ``_build_promql`` in ``prometheus_forwarder_head.py``).
- The PromQL invariants hold against the live ingress for every metric in
  the v1 allowlist, with non-trivial spread on CPU/memory series.
"""

import json
import os
import sys
import urllib.parse
import urllib.request

PROM_URL = os.environ.get("RAY_PROMETHEUS_HOST", "http://localhost:9090")
WINDOW = "90s"

# Mirrors METRIC_ALLOWLIST in ``constants.py``. Kept inline so this script
# can run before the forwarder branch is installed.
METRICS = [
    "ray_node_cpu_utilization",
    "ray_node_mem_used",
    "ray_node_mem_available",
    "ray_node_mem_total",
    "ray_node_cpu_count",
]

AGGS = {
    "avg": "avg_over_time({m}[{w}])",
    "p95": "quantile_over_time(0.95, {m}[{w}])",
    "max": "max_over_time({m}[{w}])",
}


def promql(query: str) -> list:
    url = f"{PROM_URL}/api/v1/query?{urllib.parse.urlencode({'query': query})}"
    with urllib.request.urlopen(url, timeout=15) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(f"PromQL failed: {body}")
    return body["data"]["result"]


# --- Sanity: endpoint reachable ------------------------------------------

print(f"Prometheus: {PROM_URL}")
up = promql("up")
print(f"  /api/v1/query 'up' returned {len(up)} series")

# --- Run the forwarder's query set ---------------------------------------

by_key: dict = {}  # (metric, agg, ip) -> float
for metric in METRICS:
    for agg, tmpl in AGGS.items():
        q = tmpl.format(m=metric, w=WINDOW)
        try:
            result = promql(q)
        except Exception as e:
            print(f"  !! {metric}/{agg} query failed: {e}")
            continue
        for series in result:
            ip = series["metric"].get("ip") or series["metric"].get("instance", "")
            try:
                by_key[(metric, agg, ip)] = float(series["value"][1])
            except (TypeError, ValueError):
                pass

print(f"\n=== Collected {len(by_key)} (metric, agg, ip) samples ===")


def show(metric: str) -> None:
    rows = [(k, v) for k, v in by_key.items() if k[0] == metric]
    if not rows:
        print(f"  (no samples for {metric})")
        return
    print(f"\n  {metric}:")
    for (_, agg, ip), v in sorted(rows):
        print(f"    {agg:>4s}  ip={ip:<22s}  value={v}")


for m in METRICS:
    show(m)

# --- Aggregation invariants ----------------------------------------------

print("\n=== Aggregation sanity checks ===")
violations: list = []
aggregation_proven = False
checked_nodes = 0

for metric in METRICS:
    per_ip: dict = {}
    for (m, agg, ip), val in by_key.items():
        if m == metric:
            per_ip.setdefault(ip, {})[agg] = val
    for ip, aggs in per_ip.items():
        if not {"avg", "max", "p95"} <= aggs.keys():
            continue
        checked_nodes += 1
        avg, mx, p95 = aggs["avg"], aggs["max"], aggs["p95"]
        line = f"  {metric:30s} ip={ip:22s} avg={avg:.4g}  p95={p95:.4g}  max={mx:.4g}"
        if not (avg <= p95 + 1e-6 and p95 <= mx + 1e-6):
            violations.append(line + "   <-- ORDER VIOLATION")
            print(violations[-1])
            continue
        # Spread above a tiny threshold proves the windowed aggregation
        # actually saw varying samples (not just a single scrape).
        spread = mx - avg
        rel = spread / max(abs(mx), 1e-9)
        if spread > 1e-6 and rel > 1e-9:
            aggregation_proven = True
            print(line + "   <-- spread observed")
        else:
            print(line)

if checked_nodes == 0:
    print(
        "  (no node had all three aggregations available — Prometheus may "
        "not be scraping ray metrics, or no data in the last window)"
    )

assert not violations, (
    f"Order invariant violated for {len(violations)} (metric, ip) pairs — "
    "PromQL aggregation is returning nonsense values."
)

assert aggregation_proven, (
    "All of avg, p95, max were identical for every (metric, ip) pair. "
    "Either nothing in the cluster is varying within the "
    f"{WINDOW} window, or the PromQL aggregation functions aren't being "
    "applied. Try a longer window or run a CPU-burning workload first."
)

print("\nAll checks passed.")
sys.exit(0)
