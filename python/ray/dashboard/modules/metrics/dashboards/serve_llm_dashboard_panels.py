# ruff: noqa: E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    PanelTemplate,
    Target,
    TargetTemplate,
)

SERVE_LLM_GRAFANA_PANELS = [
    Panel(
        id=1,
        title="vLLM: Token Throughput",
        description="Number of tokens processed per second",
        unit="tokens/s",
        targets=[
            Target(
                expr='sum by (model_name, WorkerId) (rate(ray_vllm:request_prompt_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="Prompt Tokens/Sec - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='sum by (model_name, WorkerId) (rate(ray_vllm:generation_tokens_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
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
        description="Time per output token latency in milliseconds.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm:time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P99 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm:time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P95 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm:time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P90 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm:time_per_output_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P50 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='(sum by(model_name, WorkerId) (rate(ray_vllm:time_per_output_token_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm:time_per_output_token_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
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
        description="Percentage of used cache blocks by vLLM.",
        unit="percentunit",
        targets=[
            Target(
                expr='ray_vllm:gpu_cache_usage_perc{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                legend="GPU Cache Usage - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='ray_vllm:cpu_cache_usage_perc{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                legend="CPU Cache Usage - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 8, 12, 8),
    ),
    Panel(
        id=5,
        title="vLLM: Time To First Token Latency",
        description="P50, P90, P95, and P99 TTFT latency in milliseconds.",
        unit="ms",
        targets=[
            Target(
                expr='(sum by(model_name, WorkerId) (rate(ray_vllm:time_to_first_token_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm:time_to_first_token_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="Average - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId)(rate(ray_vllm:time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P50 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId)(rate(ray_vllm:time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P90 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm:time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P95 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId)(rate(ray_vllm:time_to_first_token_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P99 - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 8, 12, 8),
    ),
    Panel(
        id=6,
        title="vLLM: E2E Request Latency",
        description="Latency from request start to first token returned (in seconds).",
        unit="s",
        targets=[
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm:e2e_request_latency_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))\n/\nsum by(model_name, WorkerId) (rate(ray_vllm:e2e_request_latency_seconds_count{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="Average - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum by(le, model_name, WorkerId) (rate(ray_vllm:e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P50 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum by(le, model_name, WorkerId) (rate(ray_vllm:e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P90 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.95, sum by(le, model_name, WorkerId) (rate(ray_vllm:e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P95 - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum by(le, model_name, WorkerId) (rate(ray_vllm:e2e_request_latency_seconds_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s])))',
                legend="P99 - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 16, 12, 8),
    ),
    Panel(
        id=7,
        title="vLLM: Scheduler State",
        description="Number of requests in RUNNING, WAITING, and SWAPPED state",
        unit="Requests",
        targets=[
            Target(
                expr='ray_vllm:num_requests_running{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                legend="Num Running - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='ray_vllm:num_requests_swapped{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                legend="Num Swapped - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='ray_vllm:num_requests_waiting{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}',
                legend="Num Waiting - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 16, 12, 8),
    ),
    Panel(
        id=8,
        title="vLLM: Request Prompt Length",
        description="Heatmap of request prompt length",
        unit="Requests",
        targets=[
            Target(
                expr='sum by(le, model_name, WorkerId) (increase(ray_vllm:request_prompt_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="{{le}}",
                template=TargetTemplate.HEATMAP,
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 24, 12, 8),
        template=PanelTemplate.HEATMAP,
    ),
    Panel(
        id=9,
        title="vLLM: Request Generation Length",
        description="Heatmap of request generation length",
        unit="Requests",
        targets=[
            Target(
                expr='sum by(le, model_name, WorkerId) (increase(ray_vllm:request_generation_tokens_bucket{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="{{le}}",
                template=TargetTemplate.HEATMAP,
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 24, 12, 8),
        template=PanelTemplate.HEATMAP,
    ),
    Panel(
        id=10,
        title="vLLM: Finish Reason",
        description="Number of finished requests by their finish reason: either an EOS token was generated or the max sequence length was reached.",
        unit="Requests",
        targets=[
            Target(
                expr='sum by(finished_reason, model_name, WorkerId) (increase(ray_vllm:request_success_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="{{finished_reason}} - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 32, 12, 8),
    ),
    Panel(
        id=11,
        title="vLLM: Queue Time",
        description="",
        unit="s",
        targets=[
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm:request_queue_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="{{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 32, 12, 8),
    ),
    Panel(
        id=12,
        title="vLLM: Requests Prefill and Decode Time",
        description="",
        unit="s",
        targets=[
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm:request_decode_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="Decode - {{model_name}} - {{WorkerId}}",
            ),
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm:request_prefill_time_seconds_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="Prefill - {{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 40, 12, 8),
    ),
    Panel(
        id=13,
        title="vLLM: Max Generation Token in Sequence Group",
        description="",
        unit="none",
        targets=[
            Target(
                expr='sum by(model_name, WorkerId) (rate(ray_vllm:request_max_num_generation_tokens_sum{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", {global_filters}}}[30s]))',
                legend="{{model_name}} - {{WorkerId}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 40, 12, 8),
    ),
    Panel(
        id=14,
        title="Tokens Last 24 Hours",
        description="",
        unit="Tokens",
        targets=[
            Target(
                expr='(sum by (model_name) (delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])))',
                legend="Input: {{model_name}}",
            ),
            Target(
                expr='(sum by (model_name) (delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])))',
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 48, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=15,
        title="Tokens Last Hour",
        description="",
        unit="Tokens",
        targets=[
            Target(
                expr='delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1h])',
                legend="Input: {{model_name}}",
            ),
            Target(
                expr='delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1h])',
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 48, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=16,
        title="Distribution of Requests Per Model Last 24 Hours",
        description="",
        unit="Requests",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm:request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 56, 12, 8),
        template=PanelTemplate.PIE_CHART,
    ),
    Panel(
        id=18,
        title="Ratio Input:Generated Tokens Last 24 Hours",
        description="",
        unit="none",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])) / sum by (model_name) (delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 64, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=19,
        title="Tokens Per Model Last 24 Hours",
        description="",
        unit="Tokens",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])) + sum by (model_name) (delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 64, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=21,
        title="Peak Tokens Per Second Per Model Last 24 Hours",
        description="",
        unit="Tokens/s",
        targets=[
            Target(
                expr='max_over_time(sum by (model_name) (rate(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[2m]))[24h:])',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 72, 12, 8),
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=23,
        title="Requests Per Model Last Week",
        description="",
        unit="Requests",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm:request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 72, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=24,
        title="Avg Total Tokens Per Request Last 7 Days",
        description="",
        unit="Requests",
        targets=[
            Target(
                expr='(sum by (model_name) (delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) +\nsum by (model_name) (delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))) / sum by (model_name) (delta(ray_vllm:request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 80, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=25,
        title="Avg Total Tokens Per Request Per Model Last 7 Days",
        description="",
        unit="Requests",
        targets=[
            Target(
                expr='(sum by (model_name) (delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) + sum by (model_name) (delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])))/ sum by (model_name) (delta(ray_vllm:request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 80, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=26,
        title="Tokens Per Model Last 7 Days",
        description="",
        unit="Tokens",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="In: {{ model_name}}",
            ),
            Target(
                expr='sum by (model_name) (delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="Out: {{ model_name }}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 88, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=27,
        title="Tokens Per Request Per Model Last 7 Days",
        description="",
        unit="Tokens",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm:prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) / sum by (model_name) (delta(ray_vllm:request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="In: {{ model_name}}",
            ),
            Target(
                expr='sum by (model_name) (delta(ray_vllm:generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) / sum by (model_name) (delta(ray_vllm:request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="Out: {{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 88, 12, 8),
        template=PanelTemplate.GAUGE,
    ),
]

ids = []
for panel in SERVE_LLM_GRAFANA_PANELS:
    ids.append(panel.id)
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel. {ids}"

serve_llm_dashboard_config = DashboardConfig(
    name="SERVE_LLM",
    default_uid="rayServeLlmDashboard",
    panels=SERVE_LLM_GRAFANA_PANELS,
    standard_global_filters=[],
    # Base Grafana dashboard template that is injected with panels from this file
    base_json_file_name="serve_llm_grafana_dashboard_base.json",
)
