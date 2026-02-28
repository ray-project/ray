# ruff: noqa: E501
"""Ray Data LLM Dashboard panels for vLLM metrics visualization.

This dashboard provides visibility into vLLM engine metrics when using Ray Data LLM,
including latency metrics (TTFT, TPOT, E2E), throughput, cache utilization,
prefix cache hit rate, and scheduler state.
"""

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    Target,
)

DATA_LLM_GRAFANA_PANELS = [
    Panel(
        id=1,
        title="vLLM: Token Throughput",
        description="Number of tokens processed per second",
        unit="tokens/s",
        targets=[
            Target(
                expr='sum by (model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))',
                legend="Prompt Tokens/Sec - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='sum by (model_name, WorkerId) (rate(ray_vllm_generation_tokens_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))',
                legend="Generation Tokens/Sec - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 0, 12, 8),
    ),
    Panel(
        id=2,
        title="vLLM: Time Per Output Token Latency",
        description="P50, P90, P95, P99, and Mean TPOT latency",
        unit="s",
        targets=[
            Target(
                expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P99 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P95 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P90 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P50 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='(sum by(model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_request_time_per_output_token_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="Mean - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 0, 12, 8),
    ),
    Panel(
        id=3,
        title="vLLM: Cache Utilization",
        description="Percentage of used KV cache blocks by vLLM.",
        unit="percentunit",
        targets=[
            Target(
                expr='sum by (WorkerId) (ray_vllm_kv_cache_usage_perc{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}})',
                legend="GPU Cache Usage - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 8, 12, 8),
    ),
    Panel(
        id=4,
        title="vLLM: Prefix Cache Hit Rate",
        description="Percentage of prefix cache hits. Higher is better for repeated prefixes.",
        unit="percent",
        targets=[
            Target(
                expr='max(100 * (sum by (WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])) / sum by (WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))))',
                legend="Max Hit Rate",
            ),
            Target(
                expr='min(100 * (sum by (WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])) / sum by (WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))))',
                legend="Min Hit Rate",
            ),
            Target(
                expr='100 * (sum by (WorkerId) (rate(ray_vllm_prefix_cache_hits_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])) / sum by (WorkerId) (rate(ray_vllm_prefix_cache_queries_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="Hit Rate: worker {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(12, 8, 12, 8),
    ),
    Panel(
        id=5,
        title="vLLM: Time To First Token Latency",
        description="P50, P90, P95, P99, and Mean TTFT latency",
        unit="s",
        targets=[
            Target(
                expr='(sum by(model_name, WorkerId) (rate(ray_vllm_time_to_first_token_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_time_to_first_token_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="Average - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId)(rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P50 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId)(rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P90 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P95 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId)(rate(ray_vllm_time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P99 - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 16, 12, 8),
    ),
    Panel(
        id=6,
        title="vLLM: E2E Request Latency",
        description="End-to-end request latency from arrival to completion.",
        unit="s",
        targets=[
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))',
                legend="Average - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P50 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P90 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P95 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm_e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P99 - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 16, 12, 8),
    ),
    Panel(
        id=7,
        title="vLLM: Scheduler State",
        description="Number of requests in RUNNING, WAITING, and SWAPPED state",
        unit="Requests",
        targets=[
            Target(
                expr='ray_vllm_num_requests_running{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}',
                legend="Num Running - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='ray_vllm_num_requests_swapped{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}',
                legend="Num Swapped - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='ray_vllm_num_requests_waiting{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}',
                legend="Num Waiting - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 24, 12, 8),
    ),
    Panel(
        id=8,
        title="vLLM: Queue Time",
        description="P50, P90, P95, P99, and Mean time requests spend waiting in the queue.",
        unit="s",
        targets=[
            Target(
                expr='(sum by(model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="Mean - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P50 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P90 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P95 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_queue_time_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P99 - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 24, 12, 8),
    ),
    Panel(
        id=9,
        title="vLLM: Prompt Length",
        description="Distribution of prompt token lengths.",
        unit="short",
        targets=[
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P50-{{model_name}}-{{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.90, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P90-{{model_name}}-{{WorkerId}}",
            ),
            Target(
                expr='(sum by(model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm_request_prompt_tokens_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="Average-{{model_name}}-{{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(0, 32, 12, 8),
    ),
    Panel(
        id=10,
        title="vLLM: Generation Length",
        description="Distribution of generated token lengths.",
        unit="short",
        targets=[
            Target(
                expr='histogram_quantile(0.50, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P50-{{model_name}}-{{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.90, sum by(le, model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))',
                legend="P90-{{model_name}}-{{WorkerId}}",
            ),
            Target(
                expr=(
                    '(sum by(model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))'
                    "\n/\n"
                    '(sum by(model_name, WorkerId) (rate(ray_vllm_request_generation_tokens_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval])))'
                ),
                legend="Average-{{model_name}}-{{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=1,
        stack=False,
        grid_pos=GridPos(12, 32, 12, 8),
    ),
    Panel(
        id=11,
        title="vLLM: Finish Reason",
        description="Number of finished requests by their finish reason: EOS token or max length reached.",
        unit="Requests",
        targets=[
            Target(
                expr='sum by(finished_reason, model_name, WorkerId) (increase(ray_vllm_request_success_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))',
                legend="{{finished_reason}} - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 40, 12, 8),
    ),
    Panel(
        id=12,
        title="vLLM: Prefill and Decode Time",
        description="Time spent in prefill vs decode phases.",
        unit="s",
        targets=[
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm_request_prefill_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))',
                legend="Prefill - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm_request_decode_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", ReplicaId=""}}[$interval]))',
                legend="Decode - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 40, 12, 8),
    ),
]

data_llm_dashboard_config = DashboardConfig(
    name="DATA_LLM",
    default_uid="rayDataLlmDashboard",
    panels=DATA_LLM_GRAFANA_PANELS,
    standard_global_filters=[],
    base_json_file_name="data_llm_grafana_dashboard_base.json",
)