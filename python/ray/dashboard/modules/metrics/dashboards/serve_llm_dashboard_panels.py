# ruff: noqa: E501

from collections.abc import Sequence

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
# WorkerId join: attaches deployment + replica labels to vLLM-only metrics.
# The `* 0 + 1` trick turns the counter into a constant-1 lookup table keyed
# by WorkerId so the join resolves deployment + replica labels without
# affecting the numeric value of the left-hand side.
# Keep `{global_filters}` trailing so an empty substitution leaves a tolerated
# trailing comma instead of a leading/double comma that Prometheus rejects.
_WORKER_JOIN = (
    "\n* on(WorkerId) group_left(deployment, replica)"
    "\nmax by(WorkerId, deployment, replica) ("
    'ray_serve_deployment_request_counter_total{{deployment=~"$deployment", {global_filters}}} * 0 + 1)'
)

# Standard vLLM metric filter
_VLLM_FILTER = 'model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}'

_E2E_FILTER = (
    'model_name=~"$vllm_model_name", engine_worker_id=~"$workerid", {global_filters}'
)

# vLLM filter scoped to a specific deployment (used for ray_serve_* metrics
# that also carry model_name / WorkerId labels).
_VLLM_DEPLOYMENT_FILTER = 'model_name=~"$vllm_model_name", WorkerId=~"$workerid", deployment=~"$deployment", {global_filters}'

# Legend used by most per-worker panels
_DEP_REPLICA = "{{deployment}}: {{replica}}"


def _mean_with_join(metric_base: str, *, e2e: bool = False) -> str:
    """Mean = sum(_sum) / sum(_count) with NaN guard + WorkerId join."""
    worker = "engine_worker_id" if e2e else "WorkerId"
    filters = _E2E_FILTER if e2e else _VLLM_FILTER
    expr = (
        "(\n"
        "  (\n"
        f"    sum by({worker}) (rate({metric_base}_sum{{{{{filters}}}}}[$interval]))\n"
        "    /\n"
        f"    sum by({worker}) (rate({metric_base}_count{{{{{filters}}}}}[$interval]))\n"
        "  )\n"
        f"  and on({worker})\n"
        "  (\n"
        f"    sum by({worker}) (rate({metric_base}_count{{{{{filters}}}}}[$interval])) > 0\n"
        "  )\n"
        ")"
    )
    if e2e:
        expr = f'label_replace({expr}, "WorkerId", "$1", "engine_worker_id", "(.+)")'
    return expr + _WORKER_JOIN


def _percentile_with_join(
    metric_base: str, quantile: float, *, e2e: bool = False
) -> str:
    """histogram_quantile with NaN guard + WorkerId join."""
    worker = "engine_worker_id" if e2e else "WorkerId"
    filters = _E2E_FILTER if e2e else _VLLM_FILTER
    expr = (
        "(\n"
        "  histogram_quantile(\n"
        f"    {quantile},\n"
        f"    sum by (le, {worker}) (rate({metric_base}_bucket{{{{{filters}}}}}[$interval]))\n"
        "  )\n"
        f"  and on({worker})\n"
        "  (\n"
        f"    sum by({worker}) (rate({metric_base}_count{{{{{filters}}}}}[$interval])) > 0\n"
        "  )\n"
        ")"
    )
    if e2e:
        expr = f'label_replace({expr}, "WorkerId", "$1", "engine_worker_id", "(.+)")'
    return expr + _WORKER_JOIN


def _gauge_with_join(metric: str) -> str:
    """Simple gauge metric with WorkerId join (no rate, no guard)."""
    return f"sum by(WorkerId) ({metric}{{{{{_VLLM_FILTER}}}}})" + _WORKER_JOIN


def _rate_with_join(metric: str, agg_fn: str = "rate") -> str:
    """rate() or increase() of a metric summed by WorkerId, with join."""
    return (
        f"sum by(WorkerId) ({agg_fn}({metric}{{{{{_VLLM_FILTER}}}}}[$interval]))"
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
        "(\n"
        "  (\n"
        f"    sum by(WorkerId) (rate({numerator_metric}{{{{{_VLLM_FILTER}}}}}[$interval]))\n"
        "    /\n"
        f"    sum by(WorkerId) (rate({denominator_metric}{{{{{_VLLM_FILTER}}}}}[$interval]))\n"
        + (f"    {scale}\n" if scale else "")
        + "  )\n"
        "  and on(WorkerId)\n"
        "  (\n"
        f"    sum by(WorkerId) (rate({guard}{{{{{_VLLM_FILTER}}}}}[$interval])) > 0\n"
        "  )\n"
        ")" + _WORKER_JOIN
    )


def _summed_ratio_with_join_and_guard(
    numerator_metrics: Sequence[str],
    denominator_metric: str,
) -> str:
    """Ratio of added rate metrics over one denominator, NaN guard + WorkerId join.

    The numerator rates are added before the division, so counters that each
    contribute a share of the same denominator chart as a single ratio.
    """
    return (
        "(\n"
        "  (\n"
        "    (\n"
        + "\n      +\n".join(
            f"      sum by(WorkerId) (rate({metric}{{{{{_VLLM_FILTER}}}}}[$interval]))"
            for metric in numerator_metrics
        )
        + "\n    )\n"
        "    /\n"
        f"    sum by(WorkerId) (rate({denominator_metric}{{{{{_VLLM_FILTER}}}}}[$interval]))\n"
        "  )\n"
        "  and on(WorkerId)\n"
        "  (\n"
        f"    sum by(WorkerId) (rate({denominator_metric}{{{{{_VLLM_FILTER}}}}}[$interval])) > 0\n"
        "  )\n"
        ")" + _WORKER_JOIN
    )


# ---------------------------------------------------------------------------
# Histogram helper: generates Mean / P50 / P90 panels for a given metric
# ---------------------------------------------------------------------------
def _histogram_panels(
    metric_base: str,
    label: str,
    ids: tuple,
    y: int,
    unit: str = "s",
    linewidth: int = 2,
    description: str = "",
    e2e: bool = False,
) -> list:
    """Return [Mean, P50, P90] panels for a histogram metric."""
    return [
        Panel(
            id=ids[0],
            title=f"{label} -- Mean",
            description=description,
            unit=unit,
            targets=[
                Target(expr=_mean_with_join(metric_base, e2e=e2e), legend=_DEP_REPLICA)
            ],
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
                    expr=_percentile_with_join(metric_base, 0.5, e2e=e2e),
                    legend=_DEP_REPLICA,
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
                    expr=_percentile_with_join(metric_base, 0.9, e2e=e2e),
                    legend=_DEP_REPLICA,
                )
            ],
            fill=1,
            linewidth=linewidth,
            stack=False,
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
                expr=f"sum by (deployment, replica) (rate(ray_serve_deployment_request_counter_total{{{{{_VLLM_DEPLOYMENT_FILTER}}}}}[$interval]))",
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=f"sum(rate(ray_serve_deployment_request_counter_total{{{{{_VLLM_DEPLOYMENT_FILTER}}}}}[$interval]))",
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
        description="Number of tokens processed per second",
        unit="tokens/s",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_request_prompt_tokens_sum"),
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=f"sum(rate(ray_vllm_request_prompt_tokens_sum{{{{{_VLLM_FILTER}}}}}[$interval]))",
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
        description="Number of tokens processed per second",
        unit="tokens/s",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_generation_tokens_total"),
                legend=_DEP_REPLICA,
            ),
            Target(
                expr=f"sum(rate(ray_vllm_generation_tokens_total{{{{{_VLLM_FILTER}}}}}[$interval]))",
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
# Row 2: Latency (3x3 grid)
# ===================================================================
_latency_panels_list = [
    *_histogram_panels(
        "ray_vllm_request_time_per_output_token_seconds", "TPOT", (6, 7, 8), 10
    ),
    *_histogram_panels("ray_vllm_time_to_first_token_seconds", "TTFT", (9, 10, 11), 18),
    *_histogram_panels(
        "ray_vllm_e2e_request_latency_seconds",
        "Request Latency",
        (12, 13, 14),
        26,
        description="Latency from request start to first token returned (in seconds).",
    ),
]

# E2E observations originate at HAProxy and carry the selected engine worker identity.
_streaming_latency_panels = [
    *_histogram_panels(
        "ray_vllm_inter_token_latency_seconds",
        "Engine ITL",
        (61, 62, 63),
        35,
        description="Engine inter-token latency per output iteration, in seconds.",
    ),
    *_histogram_panels(
        "ray_serve_llm_time_to_first_token_seconds",
        "E2E TTFT",
        (64, 65, 66),
        43,
        description="HAProxy request entry to the first output token at HAProxy, including routing and generation. Direct streaming only.",
        e2e=True,
    ),
    *_histogram_panels(
        "ray_serve_llm_inter_token_latency_seconds",
        "E2E ITL",
        (67, 68, 69),
        51,
        description="Time between output token chunks at HAProxy per choice, covering AsyncLLM through LLMServer to HAProxy. Direct streaming only.",
        e2e=True,
    ),
    *_histogram_panels(
        "ray_serve_llm_router_overhead_seconds",
        "Router Overhead",
        (70, 71, 72),
        59,
        description="HAProxy to the ingress router, replica decision, and return to HAProxy. Once per completed direct stream.",
        e2e=True,
    ),
    *_histogram_panels(
        "ray_serve_llm_request_duration_seconds",
        "Request Duration",
        (73, 74, 75),
        67,
        description="HAProxy request entry to response completion at HAProxy, including routing and the full generated response. Once per completed direct stream.",
        e2e=True,
    ),
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
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 76, 12, 8),
    ),
    Panel(
        id=17,
        title="GPU KV Cache Hit Rate",
        description="",
        unit="percent",
        targets=[
            Target(
                expr=(
                    f"100 * ("
                    f"(sum by(WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{{{{_VLLM_FILTER}}}}}[$interval])) "
                    f"/ sum by(WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{{{{_VLLM_FILTER}}}}}[$interval])))"
                    f" and on(WorkerId) "
                    f"(sum by(WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{{{{_VLLM_FILTER}}}}}[$interval])) > 0))"
                    + _WORKER_JOIN
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(12, 76, 12, 8),
    ),
]

# ===================================================================
# Row 4: Request Length
# ===================================================================
_request_length_panels = [
    *_histogram_panels(
        "ray_vllm_request_prompt_tokens",
        "Prompt Length",
        (19, 20, 21),
        69,
        unit="short",
        linewidth=1,
    ),
    *_histogram_panels(
        "ray_vllm_request_generation_tokens",
        "Generation Length",
        (22, 23, 24),
        77,
        unit="short",
        linewidth=1,
    ),
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
        targets=[
            Target(
                expr=_gauge_with_join("ray_vllm_num_requests_running"),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 102, 8, 8),
    ),
    Panel(
        id=27,
        title="Scheduler: Swapped",
        description="",
        unit="short",
        targets=[
            Target(
                expr=_gauge_with_join("ray_vllm_num_requests_swapped"),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 102, 8, 8),
    ),
    Panel(
        id=28,
        title="Scheduler: Waiting",
        description="",
        unit="short",
        targets=[
            Target(
                expr=_gauge_with_join("ray_vllm_num_requests_waiting"),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 102, 8, 8),
    ),
    Panel(
        id=29,
        title="Finish Reason",
        description="Number of finished requests by their finish reason.",
        unit="short",
        targets=[
            Target(
                expr=(
                    f"sum by(finished_reason, WorkerId) (increase(ray_vllm_request_success_total{{{{{_VLLM_FILTER}}}}}[$interval]))"
                    + _WORKER_JOIN
                ),
                legend="{{finished_reason}} \u2014 {{deployment}}: {{replica}}",
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 110, 12, 8),
    ),
    Panel(
        id=30,
        title="Queue Time",
        description="",
        unit="s",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_request_queue_time_seconds_sum"),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(12, 110, 12, 8),
    ),
    Panel(
        id=31,
        title="Prefill Time",
        description="",
        unit="s",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_request_prefill_time_seconds_sum"),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 118, 12, 8),
    ),
    Panel(
        id=32,
        title="Decode Time",
        description="",
        unit="s",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_request_decode_time_seconds_sum"),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(12, 118, 12, 8),
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
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 127, 8, 8),
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
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 127, 8, 8),
    ),
    Panel(
        id=36,
        title="NIXL: Transfer Rate",
        description="Number of NIXL KV cache transfers per second.",
        unit="ops",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_nixl_xfer_time_seconds_count"),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 127, 8, 8),
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
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 135, 8, 8),
    ),
    Panel(
        id=38,
        title="NIXL: KV Transfer Failures",
        description="Number of failed NIXL KV cache transfers. Any non-zero value is concerning and indicates RDMA transfer errors.",
        unit="short",
        targets=[
            Target(
                expr=_rate_with_join(
                    "ray_vllm_nixl_num_failed_transfers", agg_fn="increase"
                ),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 135, 8, 8),
    ),
    Panel(
        id=39,
        title="NIXL: KV Expired Requests",
        description="Number of requests whose KV blocks expired before decode consumed them. Spikes indicate prefill is outrunning decode or the timeout is too short.",
        unit="short",
        targets=[
            Target(
                expr=_rate_with_join(
                    "ray_vllm_nixl_num_kv_expired_reqs_total", agg_fn="increase"
                ),
                legend=_DEP_REPLICA,
            )
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 135, 8, 8),
    ),
]

# ===================================================================
# Row 7: Native CPU KV Offload (collapsed)
# ===================================================================
_kv_offload_panels = [
    Panel(
        id=52,
        title="KV Offload: Store Throughput",
        description="GPU-to-CPU KV data/s.",
        unit="GBs",
        targets=[
            Target(
                expr=(
                    "("
                    + _rate_with_join("ray_vllm_kv_offload_store_bytes_total")
                    + ") / 1024 / 1024 / 1024"
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 144, 8, 8),
    ),
    Panel(
        id=53,
        title="KV Offload: Reload Throughput",
        description="CPU-to-GPU KV data/s.",
        unit="GBs",
        targets=[
            Target(
                expr=(
                    "("
                    + _rate_with_join("ray_vllm_kv_offload_load_bytes_total")
                    + ") / 1024 / 1024 / 1024"
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 144, 8, 8),
    ),
    Panel(
        id=54,
        title="KV Offload: Store and Reload Operations/s",
        description="KV store and reload operations/s.",
        unit="ops",
        targets=[
            Target(
                expr=_rate_with_join("ray_vllm_kv_offload_store_size_count"),
                legend="store — " + _DEP_REPLICA,
            ),
            Target(
                expr=_rate_with_join("ray_vllm_kv_offload_load_size_count"),
                legend="reload — " + _DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 144, 8, 8),
    ),
    Panel(
        id=55,
        title="KV Offload: Store Bandwidth",
        description="GPU-to-CPU KV transfer bandwidth.",
        unit="GBs",
        targets=[
            Target(
                expr=_ratio_with_join_and_guard(
                    "ray_vllm_kv_offload_store_bytes_total",
                    "ray_vllm_kv_offload_store_time_total",
                    scale="/ 1024 / 1024 / 1024",
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 152, 8, 8),
    ),
    Panel(
        id=56,
        title="KV Offload: Reload Bandwidth",
        description="CPU-to-GPU KV transfer bandwidth.",
        unit="GBs",
        targets=[
            Target(
                expr=_ratio_with_join_and_guard(
                    "ray_vllm_kv_offload_load_bytes_total",
                    "ray_vllm_kv_offload_load_time_total",
                    scale="/ 1024 / 1024 / 1024",
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 152, 8, 8),
    ),
    Panel(
        id=57,
        title="KV Offload: CPU Capacity Pinned by Transfers",
        description="Share of the CPU KV pool pinned by in-flight transfers. Sustained values near 100% mean transfers may be dropped.",
        unit="percentunit",
        targets=[
            Target(
                expr=_gauge_with_join("ray_vllm_kv_offload_cpu_cache_usage_perc"),
                legend="total — " + _DEP_REPLICA,
            ),
            Target(
                expr=_gauge_with_join("ray_vllm_kv_offload_cpu_cache_write_usage_perc"),
                legend="stores — " + _DEP_REPLICA,
            ),
            Target(
                expr=_gauge_with_join("ray_vllm_kv_offload_cpu_cache_read_usage_perc"),
                legend="reloads — " + _DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 152, 8, 8),
    ),
    Panel(
        id=58,
        title="KV Offload: External Prefix Hit Rate",
        description="Connector prefix-cache hit rate.",
        unit="percent",
        targets=[
            Target(
                expr=(
                    "100 * "
                    + _ratio_with_join_and_guard(
                        "ray_vllm_external_prefix_cache_hits_total",
                        "ray_vllm_external_prefix_cache_queries_total",
                    )
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 160, 8, 8),
    ),
    Panel(
        id=60,
        title="KV Offload: Overall Prefix Hit Rate",
        description="Prompt tokens served from either cache tier, GPU or offloaded.",
        unit="percent",
        targets=[
            Target(
                expr=(
                    "100 * "
                    + _summed_ratio_with_join_and_guard(
                        [
                            "ray_vllm_prefix_cache_hits_total",
                            "ray_vllm_external_prefix_cache_hits_total",
                        ],
                        "ray_vllm_prefix_cache_queries_total",
                    )
                ),
                legend="overall — " + _DEP_REPLICA,
            ),
            Target(
                expr=(
                    "100 * "
                    + _ratio_with_join_and_guard(
                        "ray_vllm_prefix_cache_hits_total",
                        "ray_vllm_prefix_cache_queries_total",
                    )
                ),
                legend="GPU only — " + _DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(8, 160, 8, 8),
    ),
    Panel(
        id=59,
        title="KV Offload: Lookup Delay -- P90",
        description="P90 offloaded-prefix lookup latency.",
        unit="s",
        targets=[
            Target(
                expr=_percentile_with_join(
                    "ray_vllm_kv_offload_lookup_sync_delay_seconds", 0.9
                ),
                legend=_DEP_REPLICA,
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(16, 160, 8, 8),
    ),
]

# ===================================================================
# Row 8: Token Distribution (collapsed)
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
                expr=f"(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])))",
                legend="Input: {{model_name}}",
            ),
            Target(
                expr=f"(sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])))",
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 144, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=42,
        title="Tokens Last Hour",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1h]))",
                legend="Input: {{model_name}}",
            ),
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1h]))",
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 144, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=43,
        title="Ratio Input:Generated Tokens Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])) / sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d]))",
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 152, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=44,
        title="Distribution of Requests Per Model Last 24 Hours",
        description="",
        unit="Requests",
        targets=[
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1d]))",
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 152, 12, 8),
        template=PanelTemplate.PIE_CHART,
    ),
    Panel(
        id=45,
        title="Peak Tokens Per Second Per Model Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"max_over_time(sum by (model_name) (rate(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[2m]))[24h:1m])",
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 160, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=46,
        title="Tokens Per Model Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d])) + sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1d]))",
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 160, 12, 8),
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
                    f"(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) +\n"
                    f"sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])))"
                    f" / sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))"
                ),
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 168, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=48,
        title="Requests Per Model Last Week",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))",
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 168, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=49,
        title="Tokens Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w]))",
                legend="In: {{ model_name}}",
            ),
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w]))",
                legend="Out: {{ model_name }}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 176, 12, 8),
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
                    f"(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) "
                    f"+ sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])))"
                    f"/ sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))"
                ),
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 176, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=51,
        title="Tokens Per Request Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) / sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))",
                legend="In: {{ model_name}}",
            ),
            Target(
                expr=f"sum by (model_name) (delta(ray_vllm_generation_tokens_total{{{{{_WORKERID_FILTER}}}}}[1w])) / sum by (model_name) (delta(ray_vllm_request_success_total{{{{{_WORKERID_FILTER}}}}}[1w]))",
                legend="Out: {{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 184, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
]

# ===================================================================
# Assemble rows and config
# ===================================================================
_ALL_ROWS = [
    Row(title="Throughput", id=501, panels=_throughput_panels),
    Row(title="Latency", id=502, panels=_latency_panels_list),
    Row(title="Streaming Latency", id=509, panels=_streaming_latency_panels),
    Row(title="Cache", id=503, panels=_cache_panels),
    Row(title="Request Length", id=504, panels=_request_length_panels),
    Row(title="Scheduler", id=505, panels=_scheduler_panels),
    Row(title="NIXL", id=506, panels=_nixl_panels),
    Row(
        title="KV Cache Offload / Reload",
        id=508,
        panels=_kv_offload_panels,
        collapsed=True,
    ),
    Row(
        title="Token Distribution",
        id=507,
        collapsed=True,
        panels=_token_distribution_panels,
    ),
]

# Validate uniqueness of panel IDs across all rows
_all_ids = sorted(panel.id for row in _ALL_ROWS for panel in row.panels)
assert len(_all_ids) == len(
    set(_all_ids)
), f"Duplicated id found. Use unique id for each panel. {_all_ids}"

serve_llm_dashboard_config = DashboardConfig(
    name="SERVE_LLM",
    default_uid="rayServeLlmDashboard",
    standard_global_filters=[
        'ray_io_cluster=~"$Cluster"',
    ],
    base_json_file_name="serve_llm_grafana_dashboard_base.json",
    rows=_ALL_ROWS,
)
