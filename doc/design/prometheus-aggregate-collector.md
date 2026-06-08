# Design: Prometheus Aggregate Collector (Databricks Telemetry)

Tracking PR: https://github.com/ray-project/ray/pull/63349
Branch: `xgui/databricks-telemetry`

A dashboard-head module that, once an hour, queries the cluster's local
Prometheus for a small allowlisted set of **cluster-level** metric
aggregates and **records each as an extra usage tag**
(`record_extra_usage_tag`). The existing `UsageStatsHead` then ships them
in its hourly usage-stats report — so this module performs **no HTTP POST
of its own**.

**Design choices (this revision):**
- **Cluster-level summary, not a per-series time series.** Each metric
  collapses to one cluster scalar (~15 values/cycle). Per-node /
  per-operator detail is dropped; hotspots still surface via
  `max`/`p95`/counts. Time is captured by the usage report's
  `collect_timestamp_ms`; downstream stacks the hourly reports into a
  series.
- **Reuse the usage-stats pipeline instead of a parallel POST.** Each
  value is recorded via `record_extra_usage_tag` (the key must exist in the
  `TagKey` enum, `src/ray/protobuf/usage.proto`); `UsageStatsHead` reads
  the GCS-KV tags and includes them in `extra_usage_tags` of the report it
  already POSTs to `https://usage-stats.ray.io/`. No endpoint, auth,
  chunking, or retry of our own — all inherited. Disabling usage stats
  (`ray disable-usage-stats`) disables this too, automatically.

## 1. What we query

**Frequency:** once per hour (`RAY_DATABRICKS_TELEMETRY_INTERVAL_S`,
default `3600`). Each cycle issues one instant query per emitted key.

**Window:** each query aggregates over the last interval (`[3600s]`).

**Session filter:** every selector is filtered by
`{SessionName='<this session>'}` so we only read this cluster's series (a
shared monitoring ingress can carry many clusters' series per metric).

**Collapse across series:** the query reduces every series to a single
cluster scalar with an outer PromQL aggregation (see §1.2). The result is
~40 numbers per cycle — no per-node rows.

### 1.1 Metric allowlist

We collect only the **runtime/utilization** signals the usage-stats report
does *not* already carry (see "Deliberately excluded" below).
`source` = where Ray reads the value. `meaning` = what one cluster scalar
represents. `aggs` = which cluster reductions we emit.

| Metric | Source | Meaning | Aggs (cluster) |
|---|---|---|---|
| _node count_ (derived) | count of `ray_node_cpu_count` series, by type | Worker/head node counts + hourly peak | count by type, peak over 1h |
| `ray_node_cpu_utilization` | `psutil.cpu_percent()` / cgroup | Whole-node system CPU %, all processes | avg, max, min, p95 |
| `ray_node_mem_used` / `_available` | `psutil.virtual_memory` / cgroup | Node memory bytes (used/available) | sum → util % derived |
| `ray_node_gpus_utilization` | GPU provider (NVML) | Per-GPU utilization % | avg, max, min, p95 |
| `ray_node_gram_used` / `_available` | GPU provider | Per-GPU memory bytes | sum → util % derived |
| `ray_memory_manager_worker_eviction` | raylet memory monitor | Worker evictions (counter) | sum of 1h increase |
| `ray_node_manager_unexpected_worker_failure` | raylet | `SYSTEM_ERROR` worker disconnects incl. kernel OOM (counter) | sum of 1h increase |
| `ray_data_cpu_usage_cores` / `gpu_usage_cores` | Ray Data stats | Cores used by Data operators | sum across operators |
| `ray_data_current_bytes` | Ray Data stats | Object-store bytes held by operators | sum across operators |

**Deliberately excluded — already in the usage-stats report**
(`UsageStatsToReport`, `python/ray/_common/usage/usage_lib.py`), which
`UsageStatsHead` POSTs to the same `https://usage-stats.ray.io/` sink:

| Not collected | Already reported as |
|---|---|
| cluster CPU count (`ray_node_cpu_count` sum) | `total_num_cpus` |
| total memory (`ray_node_mem_total` sum) | `total_memory_gb` |
| GPU count | `total_num_gpus` |
| alive node count | `total_num_nodes` |

This collector's value is the **runtime** signals usage stats lacks
(utilization %, memory used/available, OOM counts, Ray Data usage). Note
`ray_data_current_bytes` (object-store bytes *held* by operators) is
distinct from `total_object_store_memory_gb` (object-store *capacity*), so
it is kept.

Notes:
- **Intensive vs extensive reduction.** Percentages (CPU/GPU util) reduce
  across nodes with `avg`/`max`/`min`/`p95` (a cluster-wide `sum` of
  percentages is meaningless). Additive quantities (bytes, cores) reduce
  with `sum`.
- **Utilization % is derived**, not exported: memory util =
  `sum(mem_used) / (sum(mem_used) + sum(mem_available))` — no `mem_total`
  needed; same for GPU memory. There is no `ray_node_mem_utilization`
  gauge.

### 1.2 PromQL emitted

Each key is one instant query: an **outer cross-series reducer** wrapped
around the per-series `*_over_time` window (`M` = metric, `S` = session):

| Cluster agg | PromQL | Emitted key |
|---|---|---|
| avg | `avg(avg_over_time(M{SessionName='S'}[3600s]))` | `M_avg` |
| max | `max(max_over_time(M{SessionName='S'}[3600s]))` | `M_max` |
| min | `min(min_over_time(M{SessionName='S'}[3600s]))` | `M_min` |
| p95 | `avg(quantile_over_time(0.95, M{SessionName='S'}[3600s]))` | `M_p95` |

The inner function summarizes each node over time, the outer combines
across nodes: `_avg` (avg/avg) = typical load, `_max` (max/max) = absolute
worst moment, `_min` (min/min) = least-loaded node's quietest point (spots
idle/imbalanced nodes), `_p95` (avg of each node's 1h-p95) = the typical
node's sustained-high. Since per node `min ≤ avg ≤ p95 ≤ max`, a wide
`_avg`→`_p95` gap signals bursty load. (`_p95` is the **mean of per-node
1h-p95s**, not a true global percentile — label it as such.)

Additive metrics use `sum`, not these statistical reducers — counters
(OOM/worker-failure) as `sum(increase(M[3600s]))` and additive gauges
(counts/bytes/cores) as `sum(last_over_time(M[3600s]))` — see §1.1.

**Node count** is a `count` over the node series, not a value reduction.
The plain alive-node count is excluded (it's `total_num_nodes` in usage
stats); we emit only the head/worker split and the hourly peak:

| Key | PromQL | Meaning |
|---|---|---|
| `ray_cluster_num_workers` / `_head` | `count(ray_node_cpu_count{SessionName='S', IsHeadNode='false'})` | by node type |
| `ray_cluster_num_nodes_peak` | `max_over_time(count(ray_node_cpu_count{SessionName='S'})[3600s:60s])` | peak over the hour (autoscaling churn) |

Counting the `ray_node_cpu_count` series (every node's reporter emits it)
avoids depending on the autoscaler. The autoscaler's own
`autoscaler_active_nodes` is an alternative but only exists when the
autoscaler is running.

Each query returns a **single scalar** regardless of cluster size — so the
recorded tag set is constant whether the cluster has 5 nodes or 5,000.
Prometheus still scans the underlying series internally, so query *latency*
scales with series count, but the recorded values do not grow.

## 2. Data: recorded as usage tags

We do **not** build or POST a payload. Each cycle calls
`record_extra_usage_tag(TagKey.<KEY>, value, gcs_client)` per metric,
which writes to the GCS internal KV store. `UsageStatsHead` reads those on
its next report (`get_extra_usage_tags_to_report`) and includes them in the
`extra_usage_tags` of the `UsageStats` body it already POSTs. So our
metrics ride inside the existing report, e.g.:

```jsonc
// the usage-stats report UsageStatsHead sends (our keys interleaved with
// the existing total_num_cpus / total_num_nodes / ray_version / etc.)
{
  "schema_version": "0.1",
  "source": "...", "collect_timestamp_ms": 1730003600000,
  "total_num_cpus": 2864, "total_num_nodes": 359, "ray_version": "2.x.y",
  "extra_usage_tags": {
    "ray_node_cpu_utilization_avg": "41.2",
    "ray_node_cpu_utilization_max": "96.0",
    "ray_node_cpu_utilization_min": "3.1",
    "ray_node_cpu_utilization_p95": "78.5",
    "ray_node_mem_used_sum": "1379000000000",
    "ray_node_mem_available_sum": "812000000000",
    "ray_memory_manager_worker_eviction_sum": "4",
    "ray_node_manager_unexpected_worker_failure_sum": "1",
    "ray_data_cpu_usage_cores_sum": "612",
    "ray_cluster_num_workers": "358",
    "ray_cluster_num_head": "1",
    "ray_cluster_num_nodes_peak": "359"
  }
}
```

- **Keys must be `TagKey` enum entries.** Each emitted key is the lowercase
  of a `TagKey` name in `src/ray/protobuf/usage.proto`
  (e.g. `RAY_NODE_CPU_UTILIZATION_AVG` → `ray_node_cpu_utilization_avg`).
  Adding a metric = adding a `TagKey` entry — the governance gate for data
  leaving the cluster. `get_extra_usage_tags_to_report` rejects any key not
  in the enum.
- **Values are strings** (`extra_usage_tags` is `Dict[str, str]`); scalars
  are stringified. `record_extra_usage_tag` memoizes, so an unchanged value
  skips the KV write.
- **Key naming** is `<metric>_<agg>` (counter `_sum` = sum of the 1h
  `increase`; gauge `_sum` = sum of current values). Memory/GPU-memory
  utilization % is derived downstream from `mem_used` / `mem_available`.
- **No identity tags needed** — the usage report already carries
  `session_id`, `ray_version`, and the cluster session id.
- **No user identifiers.** Collapsing across series means the tags contain
  *no* node IPs, operator, or dataset names — genuinely anonymous.

The collector also mirrors the recorded tag set to
`<session_dir>/databricks_telemetry_latest_batch.json` for operator audit.

## 3. Tech design

### 3.1 Where Ray calls Prometheus today

All Prometheus *reads* in the repo go through the `/api/v1/query` REST
endpoint with `RAY_PROMETHEUS_HOST` (default `http://localhost:9090`).
Every reader lives under `python/ray/dashboard/` except one test helper:

| Reader | Location | Live pull? |
|---|---|---|
| `DataHead` | `dashboard/modules/data/data_head.py:153` | **Yes** — sole production reader today |
| `MetricsHead` | `dashboard/modules/metrics/metrics_head.py:429` | No — only `/-/healthy` ping + config generation |
| raw-metrics helper | `python/ray/_private/test_utils.py:1006` | Test-only |

No C++/Go/Java readers exist — those layers only *export* metrics. So the
established pattern is: **the dashboard process queries Prometheus over
HTTP**, and `DataHead` is the precedent this collector follows (same
endpoint, same `{SessionName='...'}` filter, same `_query_prometheus`
shape).

### 3.2 Where we add the collector

The collector must be a **singleton on the head node**, GCS-connected
(for `cluster_id`), with access to the session directory, alive for the
cluster's lifetime. Two placements satisfy this.

#### Option A — Dashboard head module (recommended)

A `DashboardHeadModule` in the head-node dashboard process, next to
`UsageStatsHead`.

| Pros | Cons |
|---|---|
| Singleton, head placement, GCS client, and lifecycle come free from the node lifecycle. | Tied to the dashboard process running. |
| `session_dir` is unambiguous → deterministic audit-file location. | `--include-dashboard=False` drops it unless added to the `--modules-to-load` allowlist (one-line fix, same as `UsageStatsHead`). |
| Matches the `UsageStatsHead` precedent exactly (periodic telemetry that POSTs out). | Shares the head event loop (mitigated: blocking I/O runs in a thread pool). |
| Loads in minimal installs (`is_minimal_module()`); no extra process or scheduling slot. | Shares the head's failure domain (acceptable — telemetry is best-effort). |

**Lifecycle:** the dashboard head loads the module and schedules its
`run()` on the head event loop. `run()` does: warmup sleep → initial
collect → random offset (de-correlate clusters) → periodic loop every
interval. Starts and dies with the cluster automatically.

#### Option B — Standalone detached Ray actor

A detached actor pinned to the head node, running its own loop.

| Pros | Cons |
|---|---|
| Independent failure domain from the dashboard. | Doesn't remove coupling — something must still create it at boot (no self-start), reintroducing a dependency on an always-on component. |
| Explicit, self-contained lifecycle. | Must hand-build singleton enforcement, `max_restarts`, head-restart survival, namespace, cross-session cleanup. |
| Conceptually decoupled from the dashboard. | `session_dir` ambiguous unless pinned to head node (custom resource / affinity); occupies a scheduling slot; diverges from precedent. |

**Lifecycle:** some bootstrap component calls `Actor.remote()` at cluster
start; the actor runs an internal asyncio periodic loop; it must declare
`max_restarts`/`lifetime="detached"` and be explicitly torn down or
namespaced to avoid leaking across sessions.

**Recommendation: Option A.** The collector is bonded to the head-node
control-plane process, not the dashboard UI. An actor relocates that
coupling and adds lifecycle/placement work; its only real advantage
(isolated failure domain) doesn't matter for an hourly, best-effort pull.

### 3.3 How we ship (no POST)

Each cycle:
1. queries Prometheus (§1.2) → `{<metric>_<agg>: value}` tags;
2. for each, `record_extra_usage_tag(TagKey.<KEY>, value, self.gcs_client)`
   → a GCS internal-KV put (memoized: unchanged values skip the write);
3. mirrors the tag set to the session dir for audit.

That's the whole "ship" path — **no endpoint, no `requests`, no auth, no
chunking, no retry.** The actual egress is `UsageStatsHead`'s existing
hourly report: it calls `get_extra_usage_tags_to_report(gcs_client)`,
folds our tags into `extra_usage_tags`, and POSTs via
`UsageReportClient.report_usage_data` to `https://usage-stats.ray.io/`
(`usage_lib.py:340,1000`) — the same unauthenticated client every Ray
cluster already uses successfully.

Consequences:
- **Receiver: unchanged and already accepting.** Our keys live in the
  existing `UsageStats` (schema `0.1`) `extra_usage_tags` dict, so no new
  receiver schema (no `ray-project/telemetry#30`). A standalone test
  (synthetic v0.1 body over HTTPS) returned **HTTP 200**, confirming the
  deployed receiver accepts it.
- **Auth/403 non-issues.** We don't open a new POST surface; the prior
  benchmark 403 was a plain-`http://` artifact (the sink is HTTPS-only).
- **Opt-out coupling for free.** `ray disable-usage-stats` stops the report
  → stops our egress too.
- **Failure mode:** a failed Prometheus query is skipped (DEBUG); a failed
  KV put is logged and the tag simply isn't refreshed this cycle (the prior
  value persists). Collection counters (`total_success/failed`,
  `last_recorded_count`) are mirrored to
  `databricks_telemetry_status.json`.

**Cost:** our metrics land in the **usage-stats dataset** (telemetry in a
usage-tags field — a small semantic overload, fine at ~15 keys), and every
metric key must be added to the `TagKey` enum (a proto change + rebuild;
this is the intended audit gate).

## 4. Performance

### 4.1 Pull cost

Per-query latency on a 5-node cluster: min 10.5 ms, median 43.8 ms,
p95 140.5 ms, max 173.5 ms; sequential batch build **1.71 s**. On a
**358-node** cluster, all 34 (per-series) queries ran cold **1.7 s**,
warm **0.7–0.85 s** — pull scales with query fan-out, not node count, and
runs off the head event loop in a thread pool.

With the cluster-summary design the **recorded tag set is constant**
(~15 scalars) at any cluster size; the outer PromQL reducer collapses
series server-side. Query *latency* is similar to the per-series run
(Prometheus still scans the series internally), but the recorded values do
not grow, and there's no payload to build or POST.

### 4.2 Why we moved to the cluster summary

The 358-node scale run is what motivated this revision. The **per-series**
payload there was **4,401 samples / 3.84 MB raw** (286 KB gzip), dominated
by ~21 repeated `attrs` labels per sample. That exceeded Firehose's ~1 MB
record cap and forced size-based chunking (5 requests/batch) plus a new
v0.2 receiver schema. Collapsing to a cluster summary eliminates the
payload-size problem, the chunking, and the receiver schema change in one
move — at the cost of per-series granularity.

Note: GPU and `ray_data_gpu_usage_cores` were empty/0 on that run (CPU-only
cluster); a GPU + Ray Data re-run is still needed to exercise those keys.

## Configuration reference

| Env var | Default | Behavior |
|---|---|---|
| `RAY_DATABRICKS_TELEMETRY_ENABLED` | `1` | Master switch. |
| `RAY_DATABRICKS_TELEMETRY_INTERVAL_S` | `3600` | Cycle interval (== PromQL window). |
| `RAY_PROMETHEUS_HOST` (reused) | `http://localhost:9090` | Prometheus to query. |

There is **no endpoint/auth/timeout env var** — egress is the usage-stats
report's, not ours.

## Follow-ups

- Register `PrometheusForwarderHead` in the `--include-dashboard=False`
  module allowlist (`services.py:1356`) so the dashboard toggle doesn't
  silently disable telemetry.
- End-to-end confirmation on a healthy-Prometheus cluster (Runs 2–3 were
  blocked by a Mimir-ring outage): record tags → confirm they appear in the
  next usage-stats report's `extra_usage_tags`.
- GPU + Ray Data re-run to populate GPU / `ray_data_gpu_*` keys.
