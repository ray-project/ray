# ruff: noqa: E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    Row,
    Target,
)

# ---------------------------------------------------------------------------
# Reusable PromQL fragments
# ---------------------------------------------------------------------------
# SGLang metrics are emitted from the tokenizer_manager (replica) and the
# scheduler actors (separate workers). They carry model_name + WorkerId but not
# the Serve deployment/replica labels, and ray_serve_deployment_request_counter_total
# has no model_name label. So SGLang panels group by WorkerId directly instead of
# joining through the Serve counter (which dropped every scheduler series).
_WORKER_JOIN = ""

# Standard SGLang metric filter
_SGLANG_FILTER = (
    'model_name=~"$sglang_model_name", WorkerId=~"$workerid", {global_filters}'
)

# Filter for ray_serve_* metrics (they have no model_name label).
_SERVE_FILTER = 'WorkerId=~"$workerid", deployment=~"$deployment", {global_filters}'

# Legends: SGLang panels key on WorkerId; the Serve requests panel on deployment/replica.
_WORKER = "{{WorkerId}}"
_DEP_REPLICA = "{{deployment}}: {{replica}}"


def _mean_with_join(metric_base: str) -> str:
    """Mean = sum(_sum) / sum(_count) with NaN guard + WorkerId join."""
    return (
        "(\n"
        "  (\n"
        f"    sum by(WorkerId) (rate({metric_base}_sum{{{{{_SGLANG_FILTER}}}}}[$interval]))\n"
        "    /\n"
        f"    sum by(WorkerId) (rate({metric_base}_count{{{{{_SGLANG_FILTER}}}}}[$interval]))\n"
        "  )\n"
        "  and on(WorkerId)\n"
        "  (\n"
        f"    sum by(WorkerId) (rate({metric_base}_count{{{{{_SGLANG_FILTER}}}}}[$interval])) > 0\n"
        "  )\n"
        ")" + _WORKER_JOIN
    )


def _percentile_with_join(metric_base: str, quantile: float) -> str:
    """histogram_quantile with NaN guard + WorkerId join."""
    return (
        "(\n"
        "  histogram_quantile(\n"
        f"    {quantile},\n"
        f"    sum by (le, WorkerId) (rate({metric_base}_bucket{{{{{_SGLANG_FILTER}}}}}[$interval]))\n"
        "  )\n"
        "  and on(WorkerId)\n"
        "  (\n"
        f"    sum by(WorkerId) (rate({metric_base}_count{{{{{_SGLANG_FILTER}}}}}[$interval])) > 0\n"
        "  )\n"
        ")" + _WORKER_JOIN
    )


def _gauge_with_join(metric: str) -> str:
    """Simple gauge metric with WorkerId join (no rate, no guard)."""
    return f"sum by(WorkerId) ({metric}{{{{{_SGLANG_FILTER}}}}})" + _WORKER_JOIN


def _rate_with_join(metric: str, agg_fn: str = "rate") -> str:
    """rate() or increase() of a metric summed by WorkerId, with join."""
    return (
        f"sum by(WorkerId) ({agg_fn}({metric}{{{{{_SGLANG_FILTER}}}}}[$interval]))"
        + _WORKER_JOIN
    )


# ---------------------------------------------------------------------------
# Histogram helper: Mean / P50 / P90 panels for a given metric
# ---------------------------------------------------------------------------
def _histogram_panels(
    metric_base: str,
    label: str,
    ids: tuple,
    y: int,
    unit: str = "s",
    linewidth: int = 2,
    description: str = "",
) -> list:
    """Return [Mean, P50, P90] panels for a histogram metric (3-per-row)."""
    return [
        Panel(
            id=ids[0],
            title=f"{label} -- Mean",
            description=description,
            unit=unit,
            targets=[Target(expr=_mean_with_join(metric_base), legend=_WORKER)],
            fill=1,
            linewidth=linewidth,
            stack=False,
            grid_pos=GridPos(0, y, 8, 8),
        ),
        Panel(
            id=ids[1],
            title=f"{label} -- P50",
            description=description,
            unit=unit,
            targets=[
                Target(
                    expr=_percentile_with_join(metric_base, 0.5), legend=_WORKER
                )
            ],
            fill=1,
            linewidth=linewidth,
            stack=False,
            grid_pos=GridPos(8, y, 8, 8),
        ),
        Panel(
            id=ids[2],
            title=f"{label} -- P90",
            description=description,
            unit=unit,
            targets=[
                Target(
                    expr=_percentile_with_join(metric_base, 0.9), legend=_WORKER
                )
            ],
            fill=1,
            linewidth=linewidth,
            stack=False,
            grid_pos=GridPos(16, y, 8, 8),
        ),
    ]


# ===================================================================
# Row 1: Throughput (3-per-row at y=1)
# ===================================================================
_throughput_panels = [
    Panel(
        id=2,
        title="Requests / s",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"sum by (deployment, replica) (rate(ray_serve_deployment_request_counter_total{{{{{_SERVE_FILTER}}}}}[$interval]))",
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=f"sum(rate(ray_serve_deployment_request_counter_total{{{{{_SERVE_FILTER}}}}}[$interval]))",
                legend="Total QPS",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 1, 8, 8),
    ),
    Panel(
        id=3,
        title="Prompt Tokens/s",
        description="Number of prefill tokens processed per second.",
        unit="tokens/s",
        targets=[
            Target(
                expr=_rate_with_join("ray_sglang_prompt_tokens_total"),
                legend=_WORKER,
            ),
            Target(
                expr=f"sum(rate(ray_sglang_prompt_tokens_total{{{{{_SGLANG_FILTER}}}}}[$interval]))",
                legend="Total",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(8, 1, 8, 8),
    ),
    Panel(
        id=4,
        title="Generation Tokens/s",
        description="Number of generation tokens emitted per second.",
        unit="tokens/s",
        targets=[
            Target(
                expr=_rate_with_join("ray_sglang_generation_tokens_total"),
                legend=_WORKER,
            ),
            Target(
                expr=f"sum(rate(ray_sglang_generation_tokens_total{{{{{_SGLANG_FILTER}}}}}[$interval]))",
                legend="Total",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(16, 1, 8, 8),
    ),
]

# ===================================================================
# Row 2: Latency (3x3 grid at y=18/26/34)
# ===================================================================
_latency_panels_list = [
    *_histogram_panels("ray_sglang_inter_token_latency_seconds", "TPOT", (6, 7, 8), 18),
    *_histogram_panels(
        "ray_sglang_time_to_first_token_seconds", "TTFT", (9, 10, 11), 26
    ),
    *_histogram_panels(
        "ray_sglang_e2e_request_latency_seconds",
        "Request Latency",
        (12, 13, 14),
        34,
        description="End-to-end request latency (in seconds).",
    ),
]

# ===================================================================
# Row 3: Cache (2-per-row at y=59)
# ===================================================================
_cache_panels = [
    Panel(
        id=16,
        title="KV Cache Utilization",
        description="Fraction of KV cache tokens currently in use.",
        unit="percentunit",
        targets=[
            Target(
                expr=_gauge_with_join("ray_sglang_token_usage"),
                legend=_WORKER,
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 59, 12, 8),
    ),
    Panel(
        id=17,
        title="Cache Hit Rate",
        description="Prefix cache hit rate over the selected interval.",
        unit="percentunit",
        targets=[
            Target(
                expr=_gauge_with_join("ray_sglang_cache_hit_rate"),
                legend=_WORKER,
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 59, 12, 8),
    ),
]

# ===================================================================
# Row 4: Request Length (3-per-row at y=68/76)
# ===================================================================
_request_length_panels = [
    *_histogram_panels(
        "ray_sglang_prompt_tokens_histogram",
        "Prompt Length",
        (19, 20, 21),
        68,
        unit="short",
        linewidth=1,
    ),
    *_histogram_panels(
        "ray_sglang_generation_tokens_histogram",
        "Generation Length",
        (22, 23, 24),
        76,
        unit="short",
        linewidth=1,
    ),
]

# ===================================================================
# Row 5: Scheduler / Request State (3-per-row at y=93/101)
# ===================================================================
_scheduler_panels = [
    Panel(
        id=26,
        title="Scheduler: Running",
        description="Requests currently being processed by the scheduler.",
        unit="short",
        targets=[
            Target(
                expr=_gauge_with_join("ray_sglang_num_running_reqs"),
                legend=_WORKER,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 93, 8, 8),
    ),
    Panel(
        id=27,
        title="Scheduler: Queued",
        description="Requests waiting in the scheduler queue.",
        unit="short",
        targets=[
            Target(
                expr=_gauge_with_join("ray_sglang_num_queue_reqs"),
                legend=_WORKER,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 93, 8, 8),
    ),
    Panel(
        id=28,
        title="Scheduler: Paused",
        description="Requests paused (e.g. for async weight reload).",
        unit="short",
        targets=[
            Target(
                expr=_gauge_with_join("ray_sglang_num_paused_reqs"),
                legend=_WORKER,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 93, 8, 8),
    ),
    Panel(
        id=29,
        title="Queue Time -- P50",
        description="Time requests spend waiting before scheduling.",
        unit="s",
        targets=[
            Target(
                expr=_percentile_with_join("ray_sglang_queue_time_seconds", 0.5),
                legend=_WORKER,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 101, 8, 8),
    ),
    Panel(
        id=30,
        title="Queue Time -- P90",
        description="Time requests spend waiting before scheduling.",
        unit="s",
        targets=[
            Target(
                expr=_percentile_with_join("ray_sglang_queue_time_seconds", 0.9),
                legend=_WORKER,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 101, 8, 8),
    ),
    Panel(
        id=31,
        title="Aborted Requests / s",
        description="Aborted requests over the selected interval.",
        unit="short",
        targets=[
            Target(
                expr=_rate_with_join("ray_sglang_num_aborted_requests_total"),
                legend=_WORKER,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 101, 8, 8),
    ),
]

# ===================================================================
# Assemble rows and config
# ===================================================================
_ALL_ROWS = [
    Row(title="Throughput", id=601, panels=_throughput_panels),
    Row(title="Latency", id=602, panels=_latency_panels_list),
    Row(title="Cache", id=603, panels=_cache_panels),
    Row(title="Request Length", id=604, panels=_request_length_panels),
    Row(title="Scheduler", id=605, panels=_scheduler_panels),
]

# Validate uniqueness of panel IDs across all rows
_all_ids = sorted(panel.id for row in _ALL_ROWS for panel in row.panels)
assert len(_all_ids) == len(
    set(_all_ids)
), f"Duplicated id found. Use unique id for each panel. {_all_ids}"

serve_llm_sglang_dashboard_config = DashboardConfig(
    name="SERVE_LLM_SGLANG",
    default_uid="rayServeLlmSglangDashboard",
    standard_global_filters=['ray_io_cluster=~"$Cluster"'],
    base_json_file_name="serve_llm_sglang_grafana_dashboard_base.json",
    rows=_ALL_ROWS,
)
