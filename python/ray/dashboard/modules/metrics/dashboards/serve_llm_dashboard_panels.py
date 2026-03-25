# ruff: noqa: E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    PanelTemplate,
    Row,
    Target,
)

# ---------------------------------------------------------------------------
# Reusable PromQL fragments
# ---------------------------------------------------------------------------
# WorkerId join: attaches deployment + replica labels to vLLM-only metrics
_WORKER_JOIN = (
    '\n* on(WorkerId) group_left(deployment, replica)'
    '\nmax by(WorkerId, deployment, replica) ('
    'ray_serve_deployment_request_counter_total{{{global_filters}, deployment=~"$deployment"}} * 0 + 1)'
)

# Standard vLLM metric filter
_VLLM_FILTER = 'model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}'

# Deployment-scoped filter (for ray_serve metrics)
_SERVE_FILTER = 'model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}, deployment=~"$deployment"'

# Legend used by most per-worker panels
_DEP_REPLICA = "{{deployment}}: {{replica}}"


def _mean_with_join(metric_base: str) -> str:
    """Mean = sum(_sum) / sum(_count) with NaN guard + WorkerId join."""
    return (
        '(\n'
        '  (\n'
        f'    sum by(WorkerId) (rate({metric_base}_sum{{{{{_VLLM_FILTER}}}}}[$interval]))\n'
        '    /\n'
        f'    sum by(WorkerId) (rate({metric_base}_count{{{{{_VLLM_FILTER}}}}}[$interval]))\n'
        '  )\n'
        '  and on(WorkerId)\n'
        '  (\n'
        f'    sum by(WorkerId) (rate({metric_base}_count{{{{{_VLLM_FILTER}}}}}[$interval])) > 0\n'
        '  )\n'
        ')'
        + _WORKER_JOIN
    )


def _percentile_with_join(metric_base: str, quantile: float) -> str:
    """histogram_quantile with NaN guard + WorkerId join."""
    return (
        '(\n'
        '  histogram_quantile(\n'
        f'    {quantile},\n'
        f'    sum by (le, WorkerId) (rate({metric_base}_bucket{{{{{_VLLM_FILTER}}}}}[$interval]))\n'
        '  )\n'
        '  and on(WorkerId)\n'
        '  (\n'
        f'    sum by(WorkerId) (rate({metric_base}_count{{{{{_VLLM_FILTER}}}}}[$interval])) > 0\n'
        '  )\n'
        ')'
        + _WORKER_JOIN
    )


def _gauge_with_join(metric: str) -> str:
    """Simple gauge metric with WorkerId join (no rate, no guard)."""
    return (
        f'sum by(WorkerId) ({metric}{{{{{_VLLM_FILTER}}}}})'
        + _WORKER_JOIN
    )


def _rate_with_join(metric: str, agg_fn: str = "rate") -> str:
    """rate() or increase() of a metric summed by WorkerId, with join."""
    return (
        f'sum by(WorkerId) ({agg_fn}({metric}{{{{{_VLLM_FILTER}}}}}[$interval]))'
        + _WORKER_JOIN
    )


def _ratio_with_join_and_guard(
    numerator_metric: str,
    denominator_metric: str,
    *,
    scale: str = "",
    guard_metric: str | None = None,
) -> str:
    """Ratio of two rate metrics with NaN guard + WorkerId join.

    Optionally applies a scale factor (e.g. '* 1000' or '/ 1024 / 1024 / 1024').
    """
    guard = guard_metric or denominator_metric
    return (
        '(\n'
        '  (\n'
        f'    sum by(WorkerId) (rate({numerator_metric}{{{{{_VLLM_FILTER}}}}}[$interval]))\n'
        '    /\n'
        f'    sum by(WorkerId) (rate({denominator_metric}{{{{{_VLLM_FILTER}}}}}[$interval]))\n'
        + (f'    {scale}\n' if scale else '')
        + '  )\n'
        '  and on(WorkerId)\n'
        '  (\n'
        f'    sum by(WorkerId) (rate({guard}{{{{{_VLLM_FILTER}}}}}[$interval])) > 0\n'
        '  )\n'
        ')'
        + _WORKER_JOIN
    )


# ---------------------------------------------------------------------------
# Latency helper: generates Mean / P50 / P90 panels for a given metric
# ---------------------------------------------------------------------------
def _latency_panels(
    metric_base: str,
    label: str,
    ids: tuple,
    y: int,
    description: str = "",
) -> list:
    """Return [Mean, P50, P90] panels for a histogram metric."""
    return [
        Panel(
            id=ids[0],
            title=f"{label} -- Mean",
            description=description,
            unit="s",
            targets=[Target(expr=_mean_with_join(metric_base), legend=_DEP_REPLICA)],
            fill=1, linewidth=2, stack=False,
            grid_pos=GridPos(0, y, 8, 8),
        ),
        Panel(
            id=ids[1],
            title=f"{label} -- P50",
            description=description,
            unit="s",
            targets=[Target(expr=_percentile_with_join(metric_base, 0.5), legend=_DEP_REPLICA)],
            fill=1, linewidth=2, stack=False,
            grid_pos=GridPos(8, y, 8, 8),
        ),
        Panel(
            id=ids[2],
            title=f"{label} -- P90",
            description=description,
            unit="s",
            targets=[Target(expr=_percentile_with_join(metric_base, 0.9), legend=_DEP_REPLICA)],
            fill=1, linewidth=2, stack=False,
            grid_pos=GridPos(16, y, 8, 8),
        ),
    ]


# ---------------------------------------------------------------------------
# Request Length helper: Mean / P50 / P90 for token-count histograms
# ---------------------------------------------------------------------------
def _length_panels(
    metric_base: str,
    label: str,
    ids: tuple,
    y: int,
) -> list:
    """Return [Mean, P50, P90] panels for a token-count histogram metric."""
    return [
        Panel(
            id=ids[0],
            title=f"{label} -- Mean",
            description="",
            unit="short",
            targets=[Target(expr=_mean_with_join(metric_base), legend=_DEP_REPLICA)],
            fill=1, linewidth=1, stack=False,
            grid_pos=GridPos(0, y, 8, 8),
        ),
        Panel(
            id=ids[1],
            title=f"{label} -- P50",
            description="",
            unit="short",
            targets=[Target(expr=_percentile_with_join(metric_base, 0.5), legend=_DEP_REPLICA)],
            fill=1, linewidth=1, stack=False,
            grid_pos=GridPos(8, y, 8, 8),
        ),
        Panel(
            id=ids[2],
            title=f"{label} -- P90",
            description="",
            unit="short",
            targets=[Target(expr=_percentile_with_join(metric_base, 0.9), legend=_DEP_REPLICA)],
            fill=1, linewidth=1, stack=False,
            grid_pos=GridPos(16, y, 8, 8),
        ),
    ]


# ===================================================================
# Row 1: Throughput
# ===================================================================
_throughput_panels = [
    Panel(
        id=2,
        title="Requests / s",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'sum by (deployment, replica) (rate(ray_serve_deployment_request_counter_total{{{{{_SERVE_FILTER}}}}}[$interval]))',
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=f'sum(rate(ray_serve_deployment_request_counter_total{{{{{_SERVE_FILTER}}}}}[$interval]))',
                legend="Total QPS",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 1, 8, 8),
    ),
    Panel(
        id=3,
        title="Prompt Tokens/s",
        description="Number of tokens processed per second",
        unit="tokens/s",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_request_prompt_tokens_sum"),
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=f'sum(rate(ray_vllm_request_prompt_tokens_sum{{{{{_VLLM_FILTER}}}}}[$interval]))',
                legend="Total",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(8, 1, 8, 8),
    ),
    Panel(
        id=4,
        title="Generation Tokens/s",
        description="Number of tokens processed per second",
        unit="tokens/s",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_generation_tokens_total"),
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=f'sum(rate(ray_vllm_generation_tokens_total{{{{{_VLLM_FILTER}}}}}[$interval]))',
                legend="Total",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(16, 1, 8, 8),
    ),
]

# ===================================================================
# Row 2: Latency (3x3 grid)
# ===================================================================
_latency_panels_list = [
    *_latency_panels("ray_vllm_request_time_per_output_token_seconds", "TPOT", (6, 7, 8), 10),
    *_latency_panels("ray_vllm_time_to_first_token_seconds", "TTFT", (9, 10, 11), 18),
    *_latency_panels("ray_vllm_e2e_request_latency_seconds", "Request Latency", (12, 13, 14), 26, description="Latency from request start to first token returned (in seconds)."),
]

# ===================================================================
# Row 3: Cache
# ===================================================================
_cache_panels = [
    Panel(
        id=16,
        title="Cache Utilization",
        description="Percentage of used cache blocks by vLLM.",
        unit="percentunit",
        targets=[
            Target(
                expr=_gauge_with_join("ray_vllm_kv_cache_usage_perc"),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 35, 12, 8),
    ),
    Panel(
        id=17,
        title="GPU KV Cache Hit Rate",
        description="",
        unit="percent",
        targets=[
            Target(
                expr=(
                    f'100 * ('
                    f'(sum by(WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{{{{_VLLM_FILTER}}}}}[$interval])) '
                    f'/ sum by(WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{{{{_VLLM_FILTER}}}}}[$interval])))'
                    f' and on(WorkerId) '
                    f'(sum by(WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{{{{_VLLM_FILTER}}}}}[$interval])) > 0))'
                    + _WORKER_JOIN
                ),
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=(
                    f'max(100 * (sum by(WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{{{{_VLLM_FILTER}}}}}[$interval])) '
                    f'/ sum by(WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{{{{_VLLM_FILTER}}}}}[$interval]))))'
                ),
                legend="Max Hit Rate",
            ),
            Target(
                expr=(
                    f'min(100 * (sum by(WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{{{{_VLLM_FILTER}}}}}[$interval])) '
                    f'/ sum by(WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{{{{_VLLM_FILTER}}}}}[$interval]))))'
                ),
                legend="Min Hit Rate",
            ),
        ],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(12, 35, 12, 8),
    ),
]

# ===================================================================
# Row 4: Request Length
# ===================================================================
_request_length_panels = [
    *_length_panels("ray_vllm_request_prompt_tokens", "Prompt Length", (19, 20, 21), 44),
    *_length_panels("ray_vllm_request_generation_tokens", "Generation Length", (22, 23, 24), 52),
]

# ===================================================================
# Row 5: Scheduler
# ===================================================================
_scheduler_panels = [
    Panel(
        id=26,
        title="Scheduler: Running",
        description="",
        unit="short",
        targets=[Target(expr=_gauge_with_join("ray_vllm_num_requests_running"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(0, 61, 8, 8),
    ),
    Panel(
        id=27,
        title="Scheduler: Swapped",
        description="",
        unit="short",
        targets=[Target(expr=_gauge_with_join("ray_vllm_num_requests_swapped"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(8, 61, 8, 8),
    ),
    Panel(
        id=28,
        title="Scheduler: Waiting",
        description="",
        unit="short",
        targets=[Target(expr=_gauge_with_join("ray_vllm_num_requests_waiting"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(16, 61, 8, 8),
    ),
    Panel(
        id=29,
        title="Finish Reason",
        description="Number of finished requests by their finish reason.",
        unit="short",
        targets=[
            Target(
                expr=(
                    f'sum by(finished_reason, WorkerId) (increase(ray_vllm_request_success_total{{{{{_VLLM_FILTER}}}}}[$interval]))'
                    + _WORKER_JOIN
                ),
                legend="{{finished_reason}} \u2014 {{deployment}}: {{replica}}",
            ),
        ],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(0, 69, 12, 8),
    ),
    Panel(
        id=30,
        title="Queue Time",
        description="",
        unit="s",
        targets=[Target(expr=_rate_with_join("ray_vllm_request_queue_time_seconds_sum"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(12, 69, 12, 8),
    ),
    Panel(
        id=31,
        title="Prefill Time",
        description="",
        unit="s",
        targets=[Target(expr=_rate_with_join("ray_vllm_request_prefill_time_seconds_sum"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(0, 77, 12, 8),
    ),
    Panel(
        id=32,
        title="Decode Time",
        description="",
        unit="s",
        targets=[Target(expr=_rate_with_join("ray_vllm_request_decode_time_seconds_sum"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(12, 77, 12, 8),
    ),
]

# ===================================================================
# Row 6: NIXL
# ===================================================================
_nixl_panels = [
    Panel(
        id=34,
        title="NIXL: Transfer Latency",
        description="Average NIXL KV cache transfer latency in milliseconds.",
        unit="ms",
        targets=[
            Target(
                expr=_ratio_with_join_and_guard(
                    "ray_vllm_nixl_xfer_time_seconds_sum",
                    "ray_vllm_nixl_xfer_time_seconds_count",
                    scale="* 1000",
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(0, 86, 8, 8),
    ),
    Panel(
        id=35,
        title="NIXL: Transfer Throughput",
        description="NIXL KV cache transfer throughput in GB/s.",
        unit="GBs",
        targets=[
            Target(
                expr=_ratio_with_join_and_guard(
                    "ray_vllm_nixl_bytes_transferred_sum",
                    "ray_vllm_nixl_xfer_time_seconds_sum",
                    scale="/ 1024 / 1024 / 1024",
                    guard_metric="ray_vllm_nixl_xfer_time_seconds_sum",
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(8, 86, 8, 8),
    ),
    Panel(
        id=36,
        title="NIXL: Transfer Rate",
        description="Number of NIXL KV cache transfers per second.",
        unit="ops",
        targets=[Target(expr=_rate_with_join("ray_vllm_nixl_xfer_time_seconds_count"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(16, 86, 8, 8),
    ),
    Panel(
        id=37,
        title="NIXL: Avg Post Time",
        description="Average time to post/initiate a NIXL transfer in milliseconds.",
        unit="ms",
        targets=[
            Target(
                expr=_ratio_with_join_and_guard(
                    "ray_vllm_nixl_post_time_seconds_sum",
                    "ray_vllm_nixl_post_time_seconds_count",
                    scale="* 1000",
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(0, 94, 8, 8),
    ),
    Panel(
        id=38,
        title="NIXL: KV Transfer Failures",
        description="Number of failed NIXL KV cache transfers.",
        unit="short",
        targets=[Target(expr=_rate_with_join("ray_vllm_nixl_num_failed_transfers", agg_fn="increase"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(8, 94, 8, 8),
    ),
    Panel(
        id=39,
        title="NIXL: KV Expired Requests",
        description="Number of requests whose KV blocks expired before decode consumed them.",
        unit="short",
        targets=[Target(expr=_rate_with_join("ray_vllm_nixl_num_kv_expired_reqs_total", agg_fn="increase"), legend=_DEP_REPLICA)],
        fill=1, linewidth=1, stack=False,
        grid_pos=GridPos(16, 94, 8, 8),
    ),
]

# ===================================================================
# Row 7: Token Distribution (collapsed)
# ===================================================================
_WORKERID_FILTER = 'WorkerId=~"$workerid", {global_filters}'

_token_distribution_panels = [
    Panel(
        id=41,
        title="Tokens Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])))',
                legend="Input: {{model_name}}",
            ),
            Target(
                expr=f'(sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])))',
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 103, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=42,
        title="Tokens Last Hour",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1h])',
                legend="Input: {{model_name}}",
            ),
            Target(
                expr=f'delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1h])',
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(12, 103, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=43,
        title="Ratio Input:Generated Tokens Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])) / sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 111, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=44,
        title="Distribution of Requests Per Model Last 24 Hours",
        description="",
        unit="Requests",
        targets=[
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(12, 111, 12, 8),
        template=PanelTemplate.PIE_CHART,
    ),
    Panel(
        id=45,
        title="Peak Tokens Per Second Per Model Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'max_over_time(sum by (model_name) (rate(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[2m]))[24h:1m])',
                legend="{{model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 119, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=46,
        title="Tokens Per Model Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])) + sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(12, 119, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=47,
        title="Avg Total Tokens Per Request Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr=(
                    f'(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) +\n'
                    f'sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])))'
                    f' / sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))'
                ),
                legend="{{ model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 127, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=48,
        title="Requests Per Model Last Week",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))',
                legend="{{ model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(12, 127, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=49,
        title="Tokens Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w]))',
                legend="In: {{ model_name}}",
            ),
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w]))',
                legend="Out: {{ model_name }}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 135, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=50,
        title="Avg Total Tokens Per Request Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr=(
                    f'(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) '
                    f'+ sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])))'
                    f'/ sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))'
                ),
                legend="{{ model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(12, 135, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=51,
        title="Tokens Per Request Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) / sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))',
                legend="In: {{ model_name}}",
            ),
            Target(
                expr=f'sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) / sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))',
                legend="Out: {{ model_name}}",
            ),
        ],
        fill=1, linewidth=2, stack=False,
        grid_pos=GridPos(0, 143, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
]

# ===================================================================
# Assemble rows and config
# ===================================================================
_ALL_ROWS = [
    Row(title="Throughput", id=501, panels=_throughput_panels),
    Row(title="Latency", id=502, panels=_latency_panels_list),
    Row(title="Cache", id=503, panels=_cache_panels),
    Row(title="Request Length", id=504, panels=_request_length_panels),
    Row(title="Scheduler", id=505, panels=_scheduler_panels),
    Row(title="NIXL", id=506, panels=_nixl_panels),
    Row(title="Token Distribution", id=507, collapsed=True, panels=_token_distribution_panels),
]

# Validate uniqueness of panel IDs across all rows
_all_ids = sorted(
    panel.id for row in _ALL_ROWS for panel in row.panels
)
assert len(_all_ids) == len(set(_all_ids)), (
    f"Duplicated id found. Use unique id for each panel. {_all_ids}"
)

serve_llm_dashboard_config = DashboardConfig(
    name="SERVE_LLM",
    default_uid="rayServeLlmDashboard",
    standard_global_filters=[],
    base_json_file_name="serve_llm_grafana_dashboard_base.json",
    panels=[],
    rows=_ALL_ROWS,
)
