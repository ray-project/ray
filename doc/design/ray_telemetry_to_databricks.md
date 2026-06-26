# Ray Telemetry → Databricks Pipeline

| Field          | Value                            |
|----------------|----------------------------------|
| Author         | xgui@anyscale.com                |
| Reviewers      | TBD                              |
| Created        | 2026-05-10                       |
| Updated        | 2026-05-14                       |
| Status         | Draft — for review               |
| Target release | TBD                              |

## 1. Summary

Pipe Ray cluster metrics into a centralized Databricks Delta lake by adding a
**periodic puller inside the Ray dashboard** that reads the cluster's local
Prometheus, formats a curated batch as long-table JSON, and POSTs it to a
new route on the **existing `ray-project/telemetry` service** (the same Ray
Serve deployment that already powers `usage-stats.ray.io`). The service
forwards the batch to a new Kinesis Firehose stream, which lands JSON in S3;
Databricks Auto Loader materializes it into a Delta table for SQL.

Two changes:

- **Calling side** (`ray-project/ray`, this PR) — one new dashboard module
  (`PrometheusForwarderHead`, ~300 LOC) modeled on the existing `UsageStatsHead`.
- **Server side** (`ray-project/telemetry`) — one new pydantic schema, one
  new Ray Serve deployment / route, one new Firehose stream + terraform.

**On by default with opt-out**, mirroring the usage-stats UX. The existing
Prometheus / Grafana / usage-stats paths are untouched.

## 2. Motivation

We want SQL over Ray cluster metrics across many user clusters. Today Ray
exposes two telemetry surfaces, neither of which fits:

1. **Prometheus pull endpoint** (operational) — `/metrics` on the dashboard at
   `:44227` and per-node via `ReporterAgent`. Designed for a co-located Prometheus
   server to scrape into a local TSDB. No path to a warehouse.
2. **Usage stats POST** (product analytics) — one row per cluster per hour to
   `https://usage-stats.ray.io/`. Fixed dataclass schema, closed `TagKey` enum,
   no time-series semantics.

An earlier draft of this doc proposed pointing the user's Prometheus directly
at our ingest via `remote_write`. That approach has two caveats serious enough
to rule it out:

- **No control over data volume or format.** Whatever the user's Prometheus
  scrapes ships in Prometheus's wire protocol. Filtering and rate limiting
  have to be reactive on our side, and the schema is dictated by the
  Prometheus protobuf rather than our Delta shape.
- **User-side config burden.** Operators must edit `prometheus.yml` and reload
  Prometheus. Many ops teams have locked-down config processes; in practice the
  minimum-friction configuration is the only one users actually do.

Pulling from Prometheus *inside Ray* and pushing on our schedule resolves both:
we own the query, the cadence, and the wire format end-to-end, and the operator's
action collapses to setting an env var (or accepting the default).

## 3. Goals / Non-goals

**Goals**

- Push Ray's full metric surface (Python user metrics, Serve/Data/Train, raylet/GCS/scheduler internals) to Databricks Delta for SQL.
- Minimal operator config. Default behavior is "it just works"; opt-out is a one-line config or env var.
- Full server-side control over volume, cadence, and schema. Wire format matches the target Delta long table.
- Coexist with the existing Prometheus path — Grafana keeps working unchanged.
- No AWS / Databricks credentials on user machines.
- Bounded blast radius on a token leak.

**Non-goals**

- Replace the usage-stats system. The closed-enum, anonymized POST stays for product analytics.
- Real-time metrics (sub-minute latency). v1 freshness budget is hourly, matched to the chosen S3 + Auto Loader landing.
- Reach customers who don't run Prometheus. v1 assumes the standard Ray monitoring deployment (Prometheus on `localhost:9090`).
- Traces / logs. Out of scope; the architecture is metrics-only.

## 4. Background — current Ray telemetry surface

### 4.1 Prometheus/Grafana path

```
C++ stats (raylet/GCS) ─┐
                        ├─► MetricsAgent (Python)
Python user metrics ────┘    ├─ OpenTelemetryMetricRecorder
                             │    └─ PrometheusMetricReader  ──► :44227/metrics
                             └─ OpenCensus → prometheus_exporter ──► --metrics-export-port
                                                                          │
                                                            User's Prometheus
                                                                  scrape
                                                                          ▼
                                                                    Grafana
```

What the user's Prometheus is already scraping today (full list in
`python/ray/dashboard/modules/reporter/reporter_agent.py:133-407`):

- **Node-level**: `node_cpu_utilization`, `node_cpu_count`, `node_mem_used/available/total`, `node_gpus_available`, `node_gpus_utilization`, `node_gram_used/available`, `node_gpu_power_milliwatts`, `node_gpu_temperature_celsius`, disk I/O (`node_disk_io_*`), network (`node_network_*`).
- **Per-process / per-component**: `component_cpu_percentage`, `component_rss_mb`, `component_uss_mb`, `component_num_fds`, `component_gpu_percentage`, `component_gpu_memory_mb`, broken down by `Component` (`raylet`, `gcs_server`, individual workers).
- **C++ raylet/GCS/scheduler internals**: task lifecycle counters, scheduler queue depths, object store metrics — proxied through `MetricsAgent`.
- **User application metrics**: anything emitted via `ray.util.metrics.Counter/Gauge/Histogram`, including Serve's `serve_replica_qps`, Train's training-loop metrics, custom application counters.

The puller reads from this same local Prometheus, so any metric on this list
is reachable. The hardcoded allowlist in §6.4 controls which subset actually
ships.

Key files (for reviewer context):
- `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` — Python OTel SDK MeterProvider with `PrometheusMetricReader`.
- `python/ray/_private/metrics_agent.py` — `OpencensusProxyMetric`, `ProxyMetricsCollector` (C++ → Python bridge).
- `python/ray/_private/telemetry/metric_cardinality.py` — `MetricCardinality.RECOMMENDED` already strips high-cardinality labels (e.g. `worker_id`) before metrics reach Prometheus. **Whatever Prometheus sees today is what we'll forward — same cardinality policy.**
- `python/ray/dashboard/modules/metrics/metrics_head.py` — Grafana provisioning, `RAY_PROMETHEUS_HOST` env var (default `http://localhost:9090`) we reuse for the Prometheus URL.
- `python/ray/dashboard/modules/usage_stats/usage_stats_head.py` — the lifecycle pattern we copy.

### 4.2 Usage stats path

A separate, narrow "phone home" system: hourly POST of a closed
`UsageStatsToReport` dataclass to `https://usage-stats.ray.io/`. Documented
here for contrast — it is **not** the right shape for time-series metrics
(wrong cadence, closed schema, hard-coded destination, anonymized). Our new
pipeline is a parallel channel that borrows the lifecycle pattern and the
opt-out UX but ships richer time-series data to our own infrastructure.

Key files: `python/ray/_common/usage/usage_lib.py`,
`python/ray/dashboard/modules/usage_stats/usage_stats_head.py`,
`src/ray/protobuf/usage.proto`.

## 5. Proposed architecture

```
User's Ray cluster                              Centralized infra (us)
┌────────────────────────────────────┐         ┌──────────────────────────┐
│ Ray dashboard (head node)          │         │ ray-project/telemetry    │
│  ├─ UsageStatsHead (existing)      │         │ (existing Ray Serve dep) │
│  │   POST /  ──────────────────────┼─────────┼─►  /                     │
│  │                                 │         │     UsageStats model     │
│  │                                 │         │     → Firehose A → S3    │
│  │                                 │         │       (existing path)    │
│  │                                 │         │                          │
│  └─ NEW PrometheusForwarderHead    │         │  NEW /v1/metrics route   │
│      - reads RAY_PROMETHEUS_HOST   │         │     - validate bearer    │
│      - hourly: PromQL aggregates   │ HTTPS   │       token → tenant_id  │
│      - long-table JSON + Bearer    │ POST    │     - MetricsBatch model │
│      - drop on failure ────────────┼────────►│       (extra=Extra.forbid│
│      - status mirror in session_dir│ JSON +  │     - put_record to      │
│                                    │ token   │       Firehose B         │
│                                    │         │                          │
│                                    │         │           │              │
│ User's Prometheus (already running)│         │           ▼              │
│   - scrapes Ray as today           │         │   s3://ray-metrics-      │
│                                    │         │   bronze/tenant_id={id}/ │
│ Grafana (unchanged)                │         │                          │
└────────────────────────────────────┘         │           │              │
                                               │           ▼              │
                                               │   Databricks Auto Loader │
                                               │   (hourly) → Delta long  │
                                               │   table for SQL          │
                                               └──────────────────────────┘
```

The "ingest service" is **not new infrastructure** — it is the existing
`ray-project/telemetry` Ray Serve deployment behind CloudFront that already
serves `https://usage-stats.ray.io/`. We add a sibling route, a sibling
pydantic schema, a sibling Firehose stream. CloudFront/TLS, autoscaling,
on-call, and observability counters all reuse the existing stack.

**Topology decisions** (locked in):

- **Tenancy**: centralized — single Databricks workspace, multi-tenant by `tenant_id`. Tenant resolved server-side from the bearer token.
- **Prometheus location**: assume `localhost:9090` via the existing `RAY_PROMETHEUS_HOST` env var. If unreachable, the puller logs and skips the cycle; no fallback.
- **Network**: outbound HTTPS from user → us. No inbound holes. No new daemons running on user machines (the puller is in-process inside the existing dashboard agent).
- **Cadence**: hourly default, env-configurable.
- **Volume control**: hardcoded metric allowlist in Ray (§6.4). Adding a metric requires a Ray PR + release.
- **Failure mode**: drop on POST failure. Acceptable because Prometheus retains the raw data and we can backfill from the next cycle if we want (initially we won't bother).
- **Default state**: **on by default**, opt-out via env var / config file / interactive prompt, matching the existing usage-stats UX.
- **Schema**: long-table JSON, mapped 1:1 onto the Delta long table.

## 6. Detailed design — Ray-side

This is where the bulk of the v1 work lives.

### 6.1 New module: `PrometheusForwarderHead`

**Path**: `python/ray/dashboard/modules/databricks_telemetry/prometheus_forwarder_head.py`

A new dashboard module modeled directly on
`python/ray/dashboard/modules/usage_stats/usage_stats_head.py`. The shape is:

```python
class PrometheusForwarderHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config):
        super().__init__(config)
        self.enabled = forwarder_enabled()
        self.client = ForwarderClient()
        self.total_success = 0
        self.total_failed = 0
        self.seq_no = 0

    @async_loop_forever(_forwarder_interval_s())
    async def periodically_forward(self):
        if not self.enabled:
            return
        await self._forward_async()

    async def _forward_async(self):
        loop = get_or_create_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, self._forward_sync)

    def _forward_sync(self):
        try:
            batch = self._build_batch()  # PromQL query_range, flatten
            self.client.post(_forwarder_endpoint(), batch, _forwarder_token())
            self.total_success += 1
        except Exception as e:
            logger.info(f"Databricks telemetry forward failed: {e}")
            self.total_failed += 1
        finally:
            self.seq_no += 1
            self._write_status_mirror(...)
```

This is the same pattern as `UsageStatsHead._report_usage_sync` /
`_report_usage_async` / `periodically_report_usage` — the lifecycle, error
handling, status counters, and dashboard-module wiring are all reused.

### 6.2 Building a batch

For each metric in the hardcoded allowlist, the puller issues a Prometheus
`query_range` covering the interval since the last successful batch:

```
GET {RAY_PROMETHEUS_HOST}/api/v1/query_range
    ?query=<metric_name>{}
    &start=<last_batch_end>
    &end=<now>
    &step=60s
```

Step is fixed at 60s in v1 (matches the typical Ray Prometheus scrape interval).
Larger step = fewer points per batch but coarser resolution; we can revisit.

The response is flattened from Prometheus's matrix shape:

```json
{ "status": "success",
  "data": { "resultType": "matrix",
            "result": [ { "metric": {"__name__": "ray_node_cpu_utilization",
                                     "ip": "10.0.0.1", ...},
                          "values": [[1715425100, "73.2"], ...] }, ... ] } }
```

…into the long-table shape we want in Delta:

```json
{
  "schema_version": "0.1",
  "source": "ray-prometheus-forwarder",
  "cluster_id": "abc123",                 // GCS cluster_id hex
  "ray_version": "...",
  "session_id": "...",
  "batch_start_ts_ms": 1715421600000,
  "batch_end_ts_ms":   1715425200000,
  "seq_no": 17,
  "samples": [
    {"ts_ms": 1715421660000, "metric": "ray_node_cpu_utilization",
     "attrs": {"ip":"10.0.0.1","node_id":"n1"}, "value": 73.2},
    {"ts_ms": 1715421660000, "metric": "ray_node_gpus_utilization",
     "attrs": {"ip":"10.0.0.1","GpuIndex":"0"}, "value": 92.0}
  ]
}
```

This format is **already in the bronze→silver shape**, so the Databricks-side
transform is a near-trivial JSON-to-Delta projection. Tenant ID is omitted
client-side — the ingest stamps it from the validated token, so a leaked
token cannot write under another tenant.

### 6.3 Forwarder client

A small `ForwarderClient` class (~80 LOC) in
`forwarder_client.py`, parallel to `UsageReportClient`:

```python
class ForwarderClient:
    def post(self, url, batch, token):
        r = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=batch,
            timeout=30,
        )
        r.raise_for_status()
```

No retry, no buffering, no disk persistence. If the POST fails for any
reason, the batch is dropped and we move on. Operationally:
- The next cycle re-queries Prometheus with a fresh range; we lose only the
  data points already gone from Prometheus retention (typically 15d), which
  in practice means we lose nothing.
- `total_failed` and `last_error` go into the status mirror file so support
  can see what's happening.

### 6.4 Hardcoded metric allowlist

A module-level constant in `prometheus_forwarder_head.py`:

```python
# Add metrics here, with code review. Each entry ships every cycle.
# Mirrors the cardinality/privacy posture of the Prometheus path itself.
DATABRICKS_FORWARDER_METRICS = [
    # Node hardware
    "ray_node_cpu_utilization",
    "ray_node_cpu_count",
    "ray_node_mem_used",
    "ray_node_mem_available",
    "ray_node_mem_total",
    "ray_node_gpus_available",
    "ray_node_gpus_utilization",
    "ray_node_gram_used",
    "ray_node_gram_available",
    "ray_node_gpu_power_milliwatts",
    "ray_node_gpu_temperature_celsius",
    "ray_node_disk_usage",
    "ray_node_disk_free",
    "ray_node_network_sent",
    "ray_node_network_received",

    # Per-component
    "ray_component_cpu_percentage",
    "ray_component_rss_mb",
    "ray_component_uss_mb",
    "ray_component_gpu_percentage",
    "ray_component_gpu_memory_mb",

    # Core internals
    "ray_tasks",
    "ray_actors",
    "ray_object_store_memory_used",
    "ray_object_store_objects",
    "ray_scheduler_pending_tasks",

    # Libraries (add as adopted)
    # "ray_serve_replica_qps",
    # "ray_data_op_throughput",
    # "ray_train_step_time_ms",
]
```

This is the closed-list contract — same review posture as the `TagKey` enum
in `src/ray/protobuf/usage.proto`. Adding a metric is a Ray PR; reviewers
check that the metric's labels don't contain user-content or PII before
green-lighting.

A regex-style "ship everything matching `ray_.*`" is **explicitly rejected**
(captured in §13 alternatives) because the whole point of the puller-vs-`remote_write`
choice is to keep this gate.

### 6.5 Configuration / env vars

| Variable | Default | Description |
|---|---|---|
| `RAY_DATABRICKS_TELEMETRY_ENABLED` | `1` | Master switch. `0` disables the feature regardless of other settings. |
| `RAY_DATABRICKS_TELEMETRY_ENDPOINT` | (set per release) | Ingest URL. We ship the production URL pre-baked so the default-on UX works out of the box. |
| `RAY_DATABRICKS_TELEMETRY_TOKEN_FILE` | unset | Path to a file containing the bearer token. Reread on each POST. |
| `RAY_DATABRICKS_TELEMETRY_INTERVAL_S` | `3600` | Forward interval. Same default as usage stats. |
| `RAY_PROMETHEUS_HOST` | `http://localhost:9090` | Already exists in Ray. Reused as-is. |

Opt-out also honors `~/.ray/config.json` `{"databricks_telemetry": false}` and
the existing `ray disable-usage-stats` flow can grow a sibling
`ray disable-databricks-telemetry` command. See §9 for the privacy treatment.

### 6.6 Interaction with cardinality controls

`MetricCardinality.RECOMMENDED` already runs upstream of Prometheus (in
`open_telemetry_metric_recorder.py:73-91` and the OC proxy collectors). The
puller reads from Prometheus, so it sees the same already-filtered labels
that operators see on their Grafana. No new filter to maintain on our side.

The hardcoded allowlist gives us a *second* layer: even if a Ray metric were
to leak a high-cardinality label tomorrow, it wouldn't ship until someone
explicitly added the metric to the list.

## 7. Detailed design — Ingest service

The ingest already exists. The `ray-project/telemetry` repo
(<https://github.com/ray-project/telemetry>) hosts the Ray Serve deployment
behind CloudFront that serves `https://usage-stats.ray.io/` today. We
extend it rather than stand up a new service.

### 7.1 What's already there (read-only context)

```
ray-project/telemetry/
├── telemetry/
│   ├── server.py            # 140 LOC — Ray Serve @serve.deployment Server
│   │                        #   - parses JSON, validates pydantic, stamps
│   │                        #     CloudFront geo headers, put_record to
│   │                        #     a single Firehose stream (env-configured)
│   │                        #   - autoscaling 1..10 replicas, target=1 req
│   │                        #   - emits its own Counters (invalid_requests,
│   │                        #     failed_firehose_puts)
│   ├── prober.py            # 98 LOC — health check
│   └── schemas/
│       ├── __init__.py      # models = {"0.1": UsageStats, ...}
│       └── v_0_1.py         # 107 LOC — UsageStats pydantic model
│                            #   extra=Extra.forbid (strict — unknown fields → 400)
└── terraform/
    ├── modules/             # reusable Firehose / S3 / Serve infra
    ├── dev/                 # dev environment
    └── prod/                # prod environment
```

Two AWS env knobs the existing service reads
(`RAY_TELEMETRY_SERVICE_FIREHOSE_REGION` and `_STREAM`) — we'll need a
second pair for the metrics stream.

### 7.2 What we add

Three concrete changes in `ray-project/telemetry`:

#### a. New pydantic schema for the metrics batch

Either add a `MetricsBatch` model to `schemas/v_0_1.py`, or open a new
`schemas/v_0_2.py` — preference is the latter so the metrics schema can
evolve independently of `UsageStats`. Same strictness (`extra=Extra.forbid`).

```python
# schemas/v_0_2.py — sketch only
class MetricsSample(BaseModel, extra=Extra.forbid):
    ts_ms: StrictInt
    metric: str
    agg: str
    attrs: Dict[str, str]
    value: StrictFloat

class MetricsBatch(BaseModel, extra=Extra.forbid):
    schema_version: str
    source: str                       # "ray-prometheus-forwarder"
    cluster_id: str
    ray_version: Optional[str]
    session_name: Optional[str]
    batch_window_start_ms: StrictInt
    batch_window_end_ms: StrictInt
    seq_no: StrictInt
    samples: List[MetricsSample]
```

Register it in `schemas/__init__.py`'s `models` map so the dispatcher can
find it by `schema_version`.

#### b. New route / deployment

Two options, picked below:

| Option | Trade-off |
|---|---|
| **(A) Second route on the same `Server` deployment** | Simpler. Shares replicas with usage-stats. Bad: a bursty metrics flow can throttle usage-stats and vice versa (`target_ongoing_requests=1`). |
| **(B) New `@serve.deployment` class for metrics** (recommended) | Independent autoscaling, independent error budget, independent counters. Tiny extra footprint — same process group, same code, just a separate deployment binding. |

Sketch for (B):

```python
# server.py — add alongside the existing Server class
@serve.deployment(autoscaling_config={...})
class MetricsServer:
    def __init__(self, firehose_region, firehose_stream, token_validator):
        self.firehose = boto3.client("firehose", ...)
        self.firehose_stream = firehose_stream         # NEW separate stream
        self.token_validator = token_validator
        self.num_invalid_requests = Counter(
            "metrics_num_invalid_requests", tag_keys=("schema_version",))
        self.num_failed_firehose_puts = Counter(
            "metrics_num_failed_firehose_puts", tag_keys=("exception",))
        self.num_unauthorized = Counter(
            "metrics_num_unauthorized", tag_keys=("reason",))

    async def __call__(self, request: Request):
        # 1. Auth — strip any client-supplied tenant_id, stamp from token.
        auth = request.headers.get("Authorization", "")
        tenant_id = self.token_validator.validate(auth)
        if not tenant_id:
            self.num_unauthorized.inc(tags={"reason": "bad_token"})
            return Response("Unauthorized", status_code=401)

        # 2. Parse + validate (same machinery the existing Server uses).
        body = await request.json()
        if "tenant_id" in body:
            del body["tenant_id"]                       # strip-then-stamp
        body["tenant_id"] = tenant_id

        try:
            self._validate(body)                        # pydantic forbid-extra
        except Exception:
            self.num_invalid_requests.inc(tags={"schema_version":
                body.get("schema_version", "")})
            return Response("Invalid request", status_code=400)

        # 3. Forward to Firehose (separate stream, same pattern).
        body["server_receive_timestamp_ms"] = int(time.time() * 1000)
        await self._send_to_firehose(json.dumps(body) + "\n")
        return "success"
```

Routing: Ray Serve supports a single ingress that dispatches by path. Either
use a thin dispatcher deployment that fans out to `Server` / `MetricsServer`,
or expose the new deployment under `/v1/metrics` directly via Serve's path
prefix config. Specifics depend on how the existing service is bound at
CloudFront; pick whichever requires fewer terraform changes.

#### c. New Firehose stream + S3 destination

In `terraform/modules/` add a sibling Firehose stream (`ray-metrics-bronze`)
that targets a sibling S3 bucket (or sibling prefix on the same bucket).
Partition by `tenant_id` and hour at the Firehose level — Firehose's
dynamic-partitioning supports this; alternatively partition in the
downstream Auto Loader job.

Env wire-up: new variables `RAY_METRICS_SERVICE_FIREHOSE_REGION` /
`_STREAM`, set in the `prod/` and `dev/` terraform.

### 7.3 Token validation

The existing service has **no auth** — usage-stats is anonymous and
public-by-design. The metrics route needs per-tenant tokens for query-time
isolation in Databricks (a leaked token must not let an attacker write
under another tenant's prefix).

Smallest viable token store for v1:

- A DynamoDB table keyed by SHA-256(token), value `{tenant_id, status,
  created_at, last_used}`.
- A tiny admin CLI / Lambda for `mint` / `revoke` / `list`.
- `TokenValidator.validate()` is one DynamoDB `GetItem` per request with
  short-lived in-process LRU caching (≤60 s) to keep the hot path cheap.

This is the §12.1 open question; the design above is a strawman, not a
commitment.

### 7.4 Why a strict pydantic schema matters

Both existing usage-stats and the new metrics route use
`extra=Extra.forbid`. This means a Ray-side bug that adds a stray field
(say, leaks a token into the body) gets **rejected at the boundary** with
400 rather than silently flowing through to S3. It's the same gate that
caught the `total_num_cpus` float-vs-int issue documented in
`server.py:103-114`. Worth keeping.

### 7.5 Failure semantics

Same as today's usage-stats:

- Pydantic 400 → forwarder logs, counts `total_failed`, drops the batch.
- Firehose 5xx → server returns 500 → forwarder behaves identically.
- Server emits `metrics_num_invalid_requests` and
  `metrics_num_failed_firehose_puts` counters; ops alerting reuses the
  existing dashboards.

## 8. Detailed design — Databricks side

Unchanged from earlier drafts.

- **Auto Loader job** with `cloudFiles.format=json`, `cloudFiles.schemaLocation`
  configured, `Trigger.AvailableNow` on hourly schedule.
- **Bronze table** mirrors S3 layout: raw forwarder JSON unchanged, partitioned
  by `tenant_id` and `date`.
- **Silver table** is what consumers query:

  ```
  CREATE TABLE ray_metrics_long (
      ts            TIMESTAMP,
      tenant_id     STRING,
      cluster_id    STRING,
      metric_name   STRING,
      attrs         MAP<STRING, STRING>,
      value         DOUBLE
  )
  PARTITIONED BY (date(ts), tenant_id);
  -- Optional liquid clustering on metric_name.
  ```

- **Transform** is a Databricks notebook / DLT pipeline that explodes the
  `samples` array into long rows. Because the wire format already matches
  the silver shape, the transform is mechanical.

## 9. Security & privacy

### 9.1 Default-on behavior

This is the most important section. Shipping cluster metrics to our infra by
default is a significant trust ask, so the design lifts the existing
usage-stats UX wholesale:

- **First-run prompt** during `ray start --head` for interactive sessions, mirroring
  `USAGE_STATS_CONFIRMATION_MESSAGE`. Auto-proceeds enabled after 10s for non-interactive.
- **Disable mechanisms** (all functional, listed in precedence order):
  1. `RAY_DATABRICKS_TELEMETRY_ENABLED=0` env var.
  2. `~/.ray/config.json` `{"databricks_telemetry": false}` config (persists across clusters).
  3. New CLI command `ray disable-databricks-telemetry` (matches `ray disable-usage-stats`).
  4. `--disable-databricks-telemetry` flag on `ray start`.
- **Coupled-disable rule**: if usage stats is disabled (env var, config file, or CLI),
  Databricks telemetry is also automatically disabled. The reverse is **not** true
  — a user can disable Databricks telemetry without affecting usage stats. Rationale:
  a user opting out of phone-home is opting out of all phone-home; a user opting out
  of richer telemetry isn't necessarily rejecting the lightweight anonymized one.
- **Status mirror file**: every cycle writes
  `<session_dir>/databricks_telemetry_status.json` with the batch's metric
  names, sample count, target endpoint, and success/failure. Lets operators
  audit exactly what's leaving.
- **Documentation page** linked from the first-run prompt explains: what's
  collected (the allowlist in §6.4), where it goes, how to disable, retention.

### 9.2 Credential locality

```
User side                           Our side
┌──────────────────────────┐       ┌──────────────────────────┐
│ Has:                     │       │ Has:                     │
│  - Ingest URL            │ HTTPS │  - S3 IAM role           │
│  - Per-tenant bearer     │──────►│  - Databricks creds      │
│    token (in token file) │  +    │  - Token issuance signer │
│                          │ token │                          │
│ Cannot:                  │       │ S3 bucket policy:        │
│  - reach S3              │       │  - write: ingest role    │
│  - list other tenants    │       │  - read: pipeline +      │
│  - read Delta tables     │       │    analysts only         │
└──────────────────────────┘       └──────────────────────────┘
```

Token leak blast radius: an attacker can write garbage JSON batches under
that `tenant_id` until we rotate. They cannot exfiltrate other tenants' data,
cannot reach S3, cannot reach Databricks. Rotation = mint new token, update
`RAY_DATABRICKS_TELEMETRY_TOKEN_FILE`, restart the dashboard agent (cluster
restart not required).

For the default-on UX without a token, we mint a per-cluster anonymous-class
token from `cluster_id` at first contact (TBD in §11) — same trust posture as
usage stats today.

### 9.3 Data review

The samples shipped are exactly the metrics already exposed on the user's
local Prometheus, filtered to the hardcoded allowlist in §6.4, with
`MetricCardinality.RECOMMENDED` already applied upstream. No new categories
of data are emitted by Ray for this pipeline.

Review items for security/privacy sign-off:
- Confirm each allowlist entry's label set contains no user-content or PII.
- Document that `cluster_id` (GCS hex) is not derived from any user-supplied
  string and is not stable across `ray start` invocations.
- Document the `ip` label policy — node IPs ship, which may be internal RFC1918
  addresses. Acceptable per the existing usage-stats hardware reporting; flag
  for legal anyway.

### 9.4 Egress

The puller initiates HTTPS:443 outbound. No inbound firewall holes. Honors
`HTTPS_PROXY` / `NO_PROXY` via standard `requests` library behavior.

## 10. The user-side change

For most users: nothing. The feature is on by default; we ship the production
ingest URL pre-baked. The first `ray start --head` prints the standard
"telemetry is enabled, here's how to disable it" message and proceeds.

For users who want to disable: any one of:

```bash
ray disable-databricks-telemetry
# or
export RAY_DATABRICKS_TELEMETRY_ENABLED=0
# or
ray start --head --disable-databricks-telemetry
# or already-disabled usage stats → auto-disabled (§9.1)
```

For users who want to point at their own ingest (rare; testing or self-hosted):

```bash
export RAY_DATABRICKS_TELEMETRY_ENDPOINT=https://my-ingest/api/v1/forward
export RAY_DATABRICKS_TELEMETRY_TOKEN_FILE=/path/to/token
```

That's the full surface. No `prometheus.yml` edit, no `kill -HUP` of
Prometheus, no relabel_configs to debug.

## 11. Rollout / phasing

| Phase | Scope | Risk |
|---|---|---|
| **1. Server-side: schema + route + Firehose** | In `ray-project/telemetry`: add `MetricsBatch` pydantic model, register in `schemas/__init__.py`, add `MetricsServer` deployment to `server.py`, add Firehose stream + S3 bucket in `terraform/modules`. Wire into `dev/` first. Hand-craft a JSON poster to validate end-to-end into S3. | Low. Extending an existing Serve deployment with the same patterns it already uses. |
| **2. Server-side: token validation** | DynamoDB table for tokens, admin mint/revoke CLI, `TokenValidator` integration in `MetricsServer`. Auth required on `/v1/metrics`. | Medium. New cross-service surface; security review needed. |
| **3. Databricks-side: bronze + silver** | Auto Loader job on the new S3 prefix, bronze schema, silver transform notebook, `ray_metrics_long` Delta table DDL, sample queries. Can run in parallel with Phase 2. | Low. Standard Auto Loader work. |
| **4. Ray-side: forwarder opt-in** | `PrometheusForwarderHead` shipped as opt-in (`RAY_DATABRICKS_TELEMETRY_ENABLED` defaults to `0`). Internal dogfood with select clusters. Validate schema, cardinality, costs against the now-live server. | Medium. New code in dashboard agent. |
| **5. Privacy/legal review + docs** | Disclosure doc, security review sign-off, runbook for support. Determine the anonymous-token-mint flow for default-on users without an explicit token. | Medium. Cross-team; not blocking the technical work. |
| **6. Flip default to on** | Default-on with opt-out. Watch for opt-out spikes, on-call paging, support tickets. | Medium. Trust ask; revert is one env-var change at build time. |
| **7. Library metrics expansion** | Add Serve/Data/Train/Tune metrics to the allowlist (§6.4). Coordinate with library owners. | Low. Per-metric PRs. |

## 12. Open questions

1. **Token model for default-on**. Two viable shapes:
   (a) anonymous per-cluster tokens minted automatically on first contact, keyed
       by GCS `cluster_id` (matches usage-stats anonymity);
   (b) explicit per-tenant tokens provisioned out-of-band, like the
       `remote_write` design assumed.
   Default-on requires (a) for OSS users. Anyscale-managed clusters can use
   (b) for richer per-customer attribution. Probably we need both.
2. **Coupled-disable scope.** Should `RAY_USAGE_STATS_ENABLED=0` *also* disable
   the Databricks pipeline by default (current §9.1 proposal), or should each
   be fully independent? Leaning coupled — matches user expectations about
   "phone home" being one decision.
3. **PromQL step size.** Fixed 60s is fine for hourly batches and matches
   typical Ray scrape intervals. If we discover heterogeneous scrape intervals
   in the wild, may need to auto-detect from `prometheus_target_interval_length_seconds`.
4. **Library-metric expansion governance.** Who owns adding `ray_serve_*` /
   `ray_data_*` entries to the allowlist? Same reviewer pool as `TagKey`,
   or library teams individually? Probably the latter, with a privacy
   reviewer in the loop.
5. **Histogram representation in Delta.** Prometheus exposes histograms as
   `_bucket` / `_count` / `_sum` series; our allowlist treats them as separate
   metric names, which is the simplest and matches PromQL conventions. Alternative:
   re-aggregate into a struct on the puller side. v1 picks the former; flagged
   in case reviewers want richer types.
6. **Per-cluster volume budget.** Should the ingest enforce a max-batch-size /
   max-batches-per-hour per tenant? Currently relies on the hardcoded
   allowlist as the only cap. A bad-actor allowlist-PR could blow this up;
   server-side enforcement is cheap insurance.

## 13. Risks

- **Default-on trust.** Shipping operational data by default is a non-trivial
  ask. Mitigation: clear first-run prompt, easy opt-out, transparent status
  mirror file, documentation prominence. We **will** see opt-out reports and
  occasional negative blog posts; that's expected and the §9 controls keep
  it small.
- **Closed allowlist becomes a maintenance treadmill.** Every new metric
  someone wants in Databricks is a Ray PR. Acceptable trade for the volume
  and privacy control we gain, but library teams will push back. Documenting
  the add-a-metric flow in `CONTRIBUTING.rst` helps.
- **Prometheus localhost assumption.** If a user's Prometheus runs elsewhere
  (a separate VM, in-cluster monitoring stack), the puller silently fails.
  Status mirror file surfaces this, but operators might not check.
  Mitigation: log a clear warning at the first failure ("local Prometheus
  unreachable at `RAY_PROMETHEUS_HOST`; Databricks telemetry will be empty
  until configured").
- **Token leakage via env var inspection.** `RAY_DATABRICKS_TELEMETRY_TOKEN_FILE`
  points to a file, not the token itself — better than inline. Document
  putting the file under `0600` mode in a secrets dir.
- **Drop-on-failure means silent gaps.** If our ingest is down for an hour,
  that hour's data is lost. Acceptable per the locked-in decision, but Delta
  consumers must be aware. Document in the silver-table schema notes.
- **Allowlist drift between Ray versions.** A metric in the allowlist that's
  later renamed / removed becomes a silent empty query. Mitigation: log the
  empty-result case once per allowlist entry per cycle.

## 14. Alternatives considered

- **Prometheus `remote_write` directly to our ingest.** Previous draft of this
  document. **Rejected** for v1 due to:
  (a) no server-side control over data volume or format — whatever the
      user's Prometheus scrapes ships in Prom's protobuf;
  (b) operator must edit `prometheus.yml` and reload Prometheus — high
      friction, low adoption in practice.
  Kept on the table as a possible future addition for power users who want
  full-fidelity ingest.
- **Push from Ray via OTLP + bundled OTel Collector.** Future-proof, supports
  traces/logs, sub-minute capable. Rejected for v1: substantial Ray code
  changes, ~30–100 MB binary bundling, ~50–200 MB runtime RSS. Revisit if
  we ever need traces or sub-minute freshness.
- **Pull from Prometheus by Databricks itself.** Architecturally simplest.
  Rejected: user Prometheus instances are not reachable from our central
  Databricks (firewall / VPC isolation).
- **Push samples without Prometheus in the loop (read from Ray's internal
  metric registry directly).** Skips the Prom dependency, but Ray's internal
  registry is fragmented across OC and OTel paths and lacks aggregation/step
  alignment. Pulling from Prom reuses its scrape pipeline and gives us a
  stable, downsample-able view. Reconsider when the OC→OTel migration
  finishes.
- **Direct write from puller to S3.** Would require shipping AWS credentials
  to user machines. Rejected on §9 grounds.
- **Reuse the usage-stats endpoint literally** (single route, shared
  pydantic schema). Wrong shape: closed enum, single-row payload, no
  time-series semantics. The chosen design **reuses the service** (same
  `ray-project/telemetry` Ray Serve deployment, CloudFront, on-call,
  observability) but adds a sibling route, schema, and Firehose stream
  for the metrics-shaped payload. Operational reuse without payload
  conflation.
- **Open allowlist (regex `ray_.*`).** Rejected — defeats the whole purpose of
  switching from `remote_write` to a puller. We chose this design *because*
  we want a closed list.
- **Wide Delta table per metric.** Faster scans for known metrics but every
  new allowlist entry requires a schema migration. The long-table shape
  evolves for free.

## 15. Testing plan

- **Unit (forwarder module).** Mock `RAY_PROMETHEUS_HOST` with a stub server
  returning canned `query_range` responses; assert the flattened JSON shape
  matches the bronze schema, that allowlist filtering is respected, and that
  ingest failures are caught and counted without raising. Reuse `setUpClass`
  / `tearDownClass` per the project testing guidelines.
- **Unit (server-side `MetricsServer`).** Auth: valid token → 200, tenant_id
  stamped from token mapping into the body before Firehose put_record;
  invalid token → 401; missing → 401; client-supplied `tenant_id` in the
  body is stripped before validation. Pydantic validation: payload with
  unknown field → 400 + `metrics_num_invalid_requests` counter increments.
- **Integration (Ray side).** Ray cluster + real Prometheus + stub ingest
  in a Buildkite job. Assert the forwarder emits PromQL with the
  `SessionName` filter and that the captured POST body conforms to the
  `MetricsBatch` pydantic schema (using the server-side model as the
  ground truth — drop a copy of the schema into the test fixtures).
- **Integration (server side).** Deploy `MetricsServer` to `dev` terraform
  environment; hand-craft a JSON poster with a freshly-minted token;
  assert the record lands in S3 under the right `tenant_id` prefix.
- **Manual aggregation smoke** (`tests/verify_prometheus_aggregates.py`).
  Already in the PR. Verifies `avg ≤ p95 ≤ max` and non-trivial spread
  hold against the live Prometheus the forwarder will query.
- **Default-on UX.** Snapshot test on the `ray start` interactive output to
  catch unintentional message changes during PR review.
- **Opt-out paths.** Each disable mechanism (env var, config file, CLI
  command, coupled to usage-stats) tested in isolation.
- **Failure modes.** Prometheus unreachable → empty batch, status mirror
  updated, next cycle retries. Ingest 5xx → batch dropped, counter
  incremented. Token rejected → 401 logged once, not in a tight loop.

## 16. Appendix — file inventory

### `ray-project/ray` (this PR)

| Path | Change |
|---|---|
| `python/ray/dashboard/modules/databricks_telemetry/__init__.py` | **NEW** |
| `python/ray/dashboard/modules/databricks_telemetry/prometheus_forwarder_head.py` | **NEW** — periodic loop + allowlist + batch builder. |
| `python/ray/dashboard/modules/databricks_telemetry/forwarder_client.py` | **NEW** — HTTP POST client + status mirror writer. |
| `python/ray/dashboard/modules/databricks_telemetry/constants.py` | **NEW** — env var names, prompt strings, default endpoint, `MetricSpec` + allowlist. |
| `python/ray/dashboard/modules/databricks_telemetry/tests/verify_prometheus_aggregates.py` | **NEW** — manual-run PromQL aggregation smoke test (not pytest-discovered). |
| `python/ray/_private/ray_constants.py` | New env vars (follow-up). |
| `python/ray/scripts/scripts.py` | Add `ray disable-databricks-telemetry` + `--disable-databricks-telemetry` flag (follow-up). |
| `python/ray/_common/usage/usage_lib.py` | Add coupled-disable hook so disabling usage stats also disables Databricks telemetry (follow-up). |
| `doc/source/cluster/configure-manage-dashboard.rst` (or new file) | Disclosure / opt-out documentation (follow-up). |

### `ray-project/telemetry` (separate PR)

| Path | Change |
|---|---|
| `telemetry/schemas/v_0_2.py` | **NEW** — `MetricsSample` + `MetricsBatch` pydantic models (`extra=Extra.forbid`). |
| `telemetry/schemas/__init__.py` | Register the new schema in `models`. |
| `telemetry/server.py` | **NEW** `MetricsServer` deployment, route on `/v1/metrics`. Token validation + strip-then-stamp `tenant_id` + Firehose put_record. |
| `telemetry/auth.py` | **NEW** — `TokenValidator` (DynamoDB lookup + short-lived in-process cache). |
| `terraform/modules/firehose-metrics/` | **NEW** — Firehose stream + S3 bucket + IAM. |
| `terraform/modules/dynamodb-tokens/` | **NEW** — tokens table for the validator. |
| `terraform/dev/`, `terraform/prod/` | Wire the new modules in, set `RAY_METRICS_SERVICE_FIREHOSE_REGION/_STREAM`. |
| `scripts/mint_token.py` | **NEW** — admin CLI for issuing/revoking tokens. |

### Databricks workspace (config, no repo)

| Item | Change |
|---|---|
| Auto Loader job | **NEW** — `cloudFiles.format=json`, `Trigger.AvailableNow` hourly, source = new S3 bucket from Firehose. |
| Bronze table | **NEW** — `ray_metrics_bronze`, raw Firehose JSON. |
| Silver transform | **NEW** — DLT pipeline / notebook that explodes `samples[]` into long rows. |
| `ray_metrics_long` Delta table | **NEW** — long-table DDL with partitioning + optional liquid clustering. |
