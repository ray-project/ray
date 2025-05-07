# ruff: noqa: E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    Target,
)

SERVE_LLM_GRAFANA_PANELS = [
    Panel(
        id=1,
        title="vLLM: Total Prompt Tokens",
        description="Cumulative number of prompt (prefill) tokens processed.",
        unit="tokens",
        targets=[
            Target(
                expr="ray_vllm:request_prompt_tokens_sum{{{global_filters}}}",
                legend="Prompt Tokens"
            )
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 0, 12, 8),
    ),
    Panel(
        id=2,
        title="vLLM: Total Generation Tokens",
        description="Cumulative number of generation tokens processed.",
        unit="tokens",
        targets=[
            Target(
                expr="ray_vllm:generation_tokens_total{{{global_filters}}}",
                legend="Generation Tokens"
            )
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(12, 0, 12, 8),
    ),
    Panel(
        id=3,
        title="vLLM: GPU KV Cache Usage",
        description="Current GPU KV-cache usage percentage.",
        unit="%",
        targets=[
            Target(
                expr="(ray_vllm:gpu_cache_usage_perc{{{global_filters}}}) * 100",
                legend="GPU Cache Usage"
            )
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 1, 12, 8),
    ),
    Panel(
        id=5,
        title="vLLM: Successful Requests by Finish Reason",
        description="Total successful requests, grouped by finish reason.",
        unit="reqs",
        targets=[
            Target(
                expr='sum by (finished_reason) (rate(ray_vllm:request_success_total{{{global_filters}}}[5m]))',
                legend="{{finished_reason}}"
            )
        ],
        grid_pos=GridPos(12, 1, 12, 8),
        fill=1,
        linewidth=2,
        stack=False,
    ),
    Panel(
        id=4,
        title="vLLM: Time to First Token (Quantiles)",
        description="Latency from request start to first token returned (in seconds).",
        unit="s",
        targets=[
            Target(
                expr="histogram_quantile(0.5, rate(ray_vllm:time_to_first_token_seconds_bucket{{{global_filters}}}[1m]))",
                legend="P50"
            ),
            Target(
                expr="histogram_quantile(0.90, rate(ray_vllm:time_to_first_token_seconds_bucket{{{global_filters}}}[1m]))",
                legend="P90"
            ),
            Target(
                expr="histogram_quantile(0.99, rate(ray_vllm:time_to_first_token_seconds_bucket{{{global_filters}}}[1m]))",
                legend="P99"
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(0, 2, 24, 8),
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
    standard_global_filters=[

    ],
    # Base Grafana dashboard template that is injected with panels from this file
    base_json_file_name="serve_llm_grafana_dashboard_base.json",
)
