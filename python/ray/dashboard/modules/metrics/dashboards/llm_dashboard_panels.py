# ruff: noqa: E501
"""Unified LLM Dashboard panels.

Single dashboard for all LLM workloads (Ray Serve LLM and Ray Data LLM),
organized into two sections:

  1. **vLLM Engine Metrics** — applicable to both Serve and Data LLM.
     Latency (TTFT, TPOT, E2E), throughput, cache utilization,
     scheduler state, NIXL, etc.

  2. **Serve Orchestrator Metrics** — row with Serve-specific panels.
     QPS per deployment, token statistics, etc. Only populated when
     Ray Serve is the orchestrator.
"""

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    Panel,
    PanelTemplate,
    Row,
    Target,
)
from ray.dashboard.modules.metrics.dashboards.vllm_engine_panels import (
    build_vllm_engine_panels,
)

# vLLM Engine Metrics: Shared across Serve LLM and Data LLM workloads.
LLM_ENGINE_PANELS, _next_id, _engine_end_y = build_vllm_engine_panels(
    id_start=1, y_start=0
)

# Serve Orchestrator Metrics
_serve_row_id_start = _next_id + 1  # +1 to leave room for the row's own id

_SERVE_ORCHESTRATOR_PANELS = [
    Panel(
        id=_serve_row_id_start,
        title="QPS per vLLM worker",
        description="",
        unit="short",
        targets=[
            Target(
                expr='sum by (model_name, WorkerId, replica) (rate(ray_serve_deployment_request_counter_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", deployment=~"$deployment", {global_filters}}}[$interval]))',
                legend="replica {{replica}}, worker {{WorkerId}}",
            ),
            Target(
                expr='sum(rate(ray_serve_deployment_request_counter_total{{model_name=~"$vllm_model_name", WorkerId=~"$workerid", deployment=~"$deployment", {global_filters}}}[$interval]))',
                legend="Total QPS",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
    ),
    Panel(
        id=_serve_row_id_start + 1,
        title="Tokens Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr='(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])))',
                legend="Input: {{model_name}}",
            ),
            Target(
                expr='(sum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])))',
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=_serve_row_id_start + 2,
        title="Tokens Last Hour",
        description="",
        unit="short",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1h]))',
                legend="Input: {{model_name}}",
            ),
            Target(
                expr='sum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1h]))',
                legend="Generated: {{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=_serve_row_id_start + 3,
        title="Ratio Input: Generated Tokens Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])) / sum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=_serve_row_id_start + 4,
        title="Distribution of Requests Per Model Last 24 Hours",
        description="",
        unit="Requests",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm_request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.PIE_CHART,
    ),
    Panel(
        id=_serve_row_id_start + 5,
        title="Peak Tokens Per Second Per Model Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr='max_over_time(sum by (model_name) (rate(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[2m]))[24h:1m])',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=_serve_row_id_start + 6,
        title="Tokens Per Model Last 24 Hours",
        description="",
        unit="short",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d])) + sum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1d]))',
                legend="{{model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.STAT,
    ),
    Panel(
        id=_serve_row_id_start + 7,
        title="Avg Total Tokens Per Request Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr='(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) +\nsum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))) / sum by (model_name) (delta(ray_vllm_request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=_serve_row_id_start + 8,
        title="Requests Per Model Last Week",
        description="",
        unit="short",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm_request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=_serve_row_id_start + 9,
        title="Tokens Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="In: {{ model_name}}",
            ),
            Target(
                expr='sum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="Out: {{ model_name }}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=_serve_row_id_start + 10,
        title="Avg Total Tokens Per Request Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr='(sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) + sum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])))/ sum by (model_name) (delta(ray_vllm_request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="{{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.GAUGE,
    ),
    Panel(
        id=_serve_row_id_start + 11,
        title="Tokens Per Request Per Model Last 7 Days",
        description="",
        unit="short",
        targets=[
            Target(
                expr='sum by (model_name) (delta(ray_vllm_prompt_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) / sum by (model_name) (delta(ray_vllm_request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="In: {{ model_name}}",
            ),
            Target(
                expr='sum by (model_name) (delta(ray_vllm_generation_tokens_total{{WorkerId=~"$workerid", {global_filters}}}[1w])) / sum by (model_name) (delta(ray_vllm_request_success_total{{WorkerId=~"$workerid", {global_filters}}}[1w]))',
                legend="Out: {{ model_name}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        template=PanelTemplate.GAUGE,
    ),
]

LLM_GRAFANA_ROWS = [
    Row(
        title="Serve Orchestrator Metrics",
        id=_next_id,
        panels=_SERVE_ORCHESTRATOR_PANELS,
        collapsed=False,
    ),
]

ids = [p.id for p in LLM_ENGINE_PANELS]
ids.extend(row.id for row in LLM_GRAFANA_ROWS)
for row in LLM_GRAFANA_ROWS:
    ids.extend(p.id for p in row.panels)
ids.sort()
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel/row. {ids}"

llm_dashboard_config = DashboardConfig(
    name="LLM",
    default_uid="rayLlmDashboard",
    panels=LLM_ENGINE_PANELS,
    rows=LLM_GRAFANA_ROWS,
    standard_global_filters=[],
    base_json_file_name="llm_grafana_dashboard_base.json",
)
