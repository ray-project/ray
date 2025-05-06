# ruff: noqa: E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    Target,
)

SERVE_LLM_GRAFANA_PANELS = [
    Panel(
    id=99,  # Make sure this ID is unique
    title="Dummy Constant Panel",
    description="A dummy panel that shows a constant value (42).",
    unit="number",
    targets=[
        Target(
            expr="vector(42)",
            legend="Constant 42",
        ),
    ],
    grid_pos=GridPos(0, 6, 8, 4),  # Adjust position/size as needed
    )
    # Panel(
    #     id=1,
    #     title="vLLM: Total Prompt Tokens",
    #     description="Cumulative number of prompt (prefill) tokens processed.",
    #     unit="tokens",
    #     targets=[
    #         Target(
    #             expr="sum(rate(ray_vllm_prompt_tokens_total{${{global_filters}}}[5m]))",
    #             legend="Prompt Tokens"
    #         )
    #     ],
    #     grid_pos=GridPos(0, 0, 8, 8),
    # ),
    # Panel(
    #     id=2,
    #     title="vLLM: Total Generation Tokens",
    #     description="Cumulative number of generation tokens processed.",
    #     unit="tokens",
    #     targets=[
    #         Target(
    #             expr="sum(rate(ray_vllm_generation_tokens_total{${{global_filters}}}[5m]))",
    #             legend="Generation Tokens"
    #         )
    #     ],
    #     grid_pos=GridPos(0, 8, 8, 8),
    # ),
    # Panel(
    #     id=3,
    #     title="vLLM: GPU KV Cache Usage",
    #     description="Current GPU KV-cache usage percentage.",
    #     unit="percentunit",
    #     targets=[
    #         Target(
    #             expr="avg(ray_vllm_gpu_cache_usage_perc{${{global_filters}}}) * 100",
    #             legend="GPU Cache Usage"
    #         )
    #     ],
    #     grid_pos=GridPos(8, 0, 8, 8),
    # ),
    # Panel(
    #     id=5,
    #     title="vLLM: Successful Requests by Finish Reason",
    #     description="Total successful requests, grouped by finish reason.",
    #     unit="reqs",
    #     targets=[
    #         Target(
    #             expr='sum(rate(ray_vllm_request_success_total{${{global_filters}}}[5m])) by (finished_reason)',
    #             legend="{{finished_reason}}"
    #         )
    #     ],
    #     grid_pos=GridPos(8, 8, 8, 8),
    # ),
    # Panel(
    #     id=4,
    #     title="vLLM: Time to First Token (Quantiles)",
    #     description="Latency from request start to first token returned (in seconds).",
    #     unit="s",
    #     targets=[
    #         Target(
    #             expr="histogram_quantile(0.5, sum(rate(ray_vllm_time_to_first_token_seconds_bucket{${{global_filters}}}[5m])) by (le))",
    #             legend="P50"
    #         ),
    #         Target(
    #             expr="histogram_quantile(0.9, sum(rate(ray_vllm_time_to_first_token_seconds_bucket{${{global_filters}}}[5m])) by (le))",
    #             legend="P90"
    #         ),
    #         Target(
    #             expr="histogram_quantile(0.99, sum(rate(ray_vllm_time_to_first_token_seconds_bucket{${{global_filters}}}[5m])) by (le))",
    #             legend="P99"
    #         ),
    #     ],
    #     grid_pos=GridPos(16, 0, 16, 8),
    # ),
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
