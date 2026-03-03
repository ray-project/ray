# ruff: noqa: E501
"""vLLM engine metric panels for the unified LLM Grafana dashboard.

All PromQL expressions use the {global_filters} placeholder, which gets
populated from DashboardConfig.standard_global_filters at generation time.
"""

from typing import List, Tuple

from ray.dashboard.modules.metrics.dashboards.common import (
    GridPos,
    Panel,
    Target,
)

PANEL_HEIGHT = 8
PANEL_WIDTH = 12


def build_vllm_engine_panels(
    id_start: int = 1,
    y_start: int = 0,
) -> Tuple[List[Panel], int, int]:
    _id = id_start
    y = y_start
    panels: List[Panel] = []

    def _next_id() -> int:
        nonlocal _id
        result = _id
        _id += 1
        return result

    # --- Row: Token Throughput + TPOT ---
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Token Throughput",
            description="Number of tokens processed per second",
            unit="tokens/s",
            targets=[
                Target(
                    expr='sum by (model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="Prompt Tokens/Sec - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='sum by (model_name, WorkerId) (rate(ray_vllm_generation_tokens_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="Generation Tokens/Sec - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Time Per Output Token Latency",
            description="P50, P90, P95, P99, and Mean TPOT latency",
            unit="s",
            targets=[
                Target(
                    expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P99 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P95 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P90 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P50 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='(sum by(model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="Mean - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # --- Row: Cache Utilization + KV Cache Hit Rate ---
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Cache Utilization",
            description="Percentage of used KV cache blocks by vLLM.",
            unit="percentunit",
            targets=[
                Target(
                    expr='sum by (WorkerId) (ray_vllm_kv_cache_usage_perc{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}})',
                    legend="GPU Cache Usage - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: KV Cache Hit Rate",
            description="Percentage of prefix cache hits. Higher is better for repeated prefixes.",
            unit="percent",
            targets=[
                Target(
                    expr='max(100 * (sum by (WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])) / sum by (WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))))',
                    legend="Max Hit Rate",
                ),
                Target(
                    expr='min(100 * (sum by (WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])) / sum by (WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))))',
                    legend="Min Hit Rate",
                ),
                Target(
                    expr='100 * (sum by (WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])) / sum by (WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="Hit Rate: worker {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # --- Row: TTFT + E2E Request Latency ---
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Time To First Token Latency",
            description="P50, P90, P95, P99, and Mean TTFT latency",
            unit="s",
            targets=[
                Target(
                    expr='(sum by(model_name, WorkerId) (rate(ray_vllm_time_to_first_token_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_time_to_first_token_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="Average - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId)(rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P50 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId)(rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P90 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P95 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId)(rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P99 - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: E2E Request Latency",
            description="End-to-end request latency from arrival to completion.",
            unit="s",
            targets=[
                Target(
                    expr='sum by(model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="Average - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P50 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P90 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P95 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P99 - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # --- Row: Scheduler State + Queue Time ---
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Scheduler State",
            description="Number of requests in RUNNING, WAITING, and SWAPPED state",
            unit="Requests",
            targets=[
                Target(
                    expr='ray_vllm_num_requests_running{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                    legend="Num Running - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='ray_vllm_num_requests_swapped{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                    legend="Num Swapped - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='ray_vllm_num_requests_waiting{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                    legend="Num Waiting - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Queue Time",
            description="P50, P90, P95, P99, and Mean time requests spend waiting in the queue.",
            unit="s",
            targets=[
                Target(
                    expr='(sum by(model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="Mean - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P50 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P90 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P95 - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P99 - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # --- Row: Prompt Length + Generation Length ---
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Prompt Length",
            description="Distribution of prompt token lengths.",
            unit="short",
            targets=[
                Target(
                    expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P50-{{model_name}}-{{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.90, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P90-{{model_name}}-{{WorkerId}}",
                ),
                Target(
                    expr='(sum by(model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="Average-{{model_name}}-{{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Generation Length",
            description="Distribution of generated token lengths.",
            unit="short",
            targets=[
                Target(
                    expr='histogram_quantile(0.50, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P50-{{model_name}}-{{WorkerId}}",
                ),
                Target(
                    expr='histogram_quantile(0.90, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))',
                    legend="P90-{{model_name}}-{{WorkerId}}",
                ),
                Target(
                    expr=(
                        '(sum by(model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))'
                        "\n/\n"
                        '(sum by(model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval])))'
                    ),
                    legend="Average-{{model_name}}-{{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # --- Row: Finish Reason + Prefill and Decode Time ---
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Finish Reason",
            description="Number of finished requests by their finish reason: EOS token or max length reached.",
            unit="Requests",
            targets=[
                Target(
                    expr='sum by(finished_reason, model_name, WorkerId) (increase(ray_vllm_request_success_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="{{finished_reason}} - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Prefill and Decode Time",
            description="Time spent in prefill vs decode phases.",
            unit="s",
            targets=[
                Target(
                    expr='sum by(model_name, WorkerId) (rate(ray_vllm_request_decode_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="Decode - {{model_name}} - {{WorkerId}}",
                ),
                Target(
                    expr='sum by(model_name, WorkerId) (rate(ray_vllm_request_prefill_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="Prefill - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # --- Row: Max Generation Token (single panel) ---
    panels.append(
        Panel(
            id=_next_id(),
            title="vLLM: Max Generation Token in Sequence Group",
            description="",
            unit="none",
            targets=[
                Target(
                    expr='sum by(model_name, WorkerId) (rate(ray_vllm_request_max_num_generation_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="{{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # --- NIXL Panels ---
    # Row: Transfer Latency + Transfer Throughput
    panels.append(
        Panel(
            id=_next_id(),
            title="NIXL: Transfer Latency",
            description="Average NIXL KV cache transfer latency in milliseconds.",
            unit="ms",
            targets=[
                Target(
                    expr='sum by(model_name, WorkerId) (rate(ray_vllm_nixl_xfer_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_nixl_xfer_time_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n* 1000',
                    legend="Avg Latency - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="NIXL: Transfer Throughput",
            description="NIXL KV cache transfer throughput in GB/s (bytes transferred / transfer time).",
            unit="GBs",
            targets=[
                Target(
                    expr='sum by(model_name, WorkerId) (rate(ray_vllm_nixl_bytes_transferred_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_nixl_xfer_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/ 1024 / 1024 / 1024',
                    legend="Throughput - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # Row: Transfer Rate + Avg Post Time
    panels.append(
        Panel(
            id=_next_id(),
            title="NIXL: Transfer Rate",
            description="Number of NIXL KV cache transfers per second.",
            unit="ops",
            targets=[
                Target(
                    expr='sum by (model_name, WorkerId) (rate(ray_vllm_nixl_xfer_time_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="Transfers/s - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="NIXL: Avg Post Time",
            description="Average time to post/initiate a NIXL transfer in milliseconds.",
            unit="ms",
            targets=[
                Target(
                    expr='sum by(model_name, WorkerId) (rate(ray_vllm_nixl_post_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_nixl_post_time_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))\n* 1000',
                    legend="Avg Post Time - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    # Row: KV Transfer Failures + KV Expired Requests
    panels.append(
        Panel(
            id=_next_id(),
            title="NIXL: KV Transfer Failures",
            description="Number of failed NIXL KV cache transfers. Any non-zero value is concerning and indicates RDMA transfer errors.",
            unit="short",
            targets=[
                Target(
                    expr='sum by (model_name, WorkerId) (increase(ray_vllm_nixl_num_failed_transfers{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="Failed Transfers - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(0, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    panels.append(
        Panel(
            id=_next_id(),
            title="NIXL: KV Expired Requests",
            description="Number of requests whose KV blocks expired before decode consumed them. Spikes indicate prefill is outrunning decode or the timeout is too short.",
            unit="short",
            targets=[
                Target(
                    expr='sum by (model_name, WorkerId) (increase(ray_vllm_nixl_num_kv_expired_reqs{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[$interval]))',
                    legend="KV Expired - {{model_name}} - {{WorkerId}}",
                ),
            ],
            fill=1,
            linewidth=2,
            stack=False,
            grid_pos=GridPos(PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT),
        )
    )
    y += PANEL_HEIGHT

    return panels, _id, y
