# flake8: noqa E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    Target,
)

SERVE_DEPLOYMENT_GRAFANA_PANELS = [
    Panel(
        id=1,
        title="Replicas per deployment",
        description='Number of replicas per deployment. Ignores "Route" variable.',
        unit="replicas",
        targets=[
            Target(
                expr="sum(ray_serve_deployment_replica_healthy{{{global_filters}}}) by (deployment)",
                legend="{{deployment}}",
            ),
        ],
        grid_pos=GridPos(0, 0, 8, 8),
    ),
    Panel(
        id=2,
        title="QPS per replica",
        description="QPS for each replica.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_deployment_request_counter{{route=~"$Route",route!~"/-/.*",{global_filters}}}[5m])) by (deployment, replica)',
                legend="{{replica}}",
            ),
        ],
        grid_pos=GridPos(8, 0, 8, 8),
    ),
    Panel(
        id=3,
        title="Error QPS per replica",
        description="Error QPS for each replica.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_deployment_error_counter{{route=~"$Route",route!~"/-/.*",{global_filters}}}[5m])) by (deployment, replica)',
                legend="{{replica}}",
            ),
        ],
        grid_pos=GridPos(16, 0, 8, 8),
    ),
    Panel(
        id=4,
        title="P50 latency per replica",
        description="P50 latency per replica.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.5, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",route!~"/-/.*",{global_filters}}}[5m])) by (deployment, replica, le))',
                legend="{{replica}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",route!~"/-/.*",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 1, 8, 8),
    ),
    Panel(
        id=5,
        title="P90 latency per replica",
        description="P90 latency per replica.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.9, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",route!~"/-/.*",{global_filters}}}[5m])) by (deployment, replica, le))',
                legend="{{replica}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",route!~"/-/.*",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(8, 1, 8, 8),
    ),
    Panel(
        id=6,
        title="P99 latency per replica",
        description="P99 latency per replica.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.99, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",route!~"/-/.*",{global_filters}}}[5m])) by (deployment, replica, le))',
                legend="{{replica}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(16, 1, 8, 8),
    ),
    Panel(
        id=7,
        title="Queue size per deployment",
        description='Number of requests queued per deployment. Ignores "Replica" and "Route" variable.',
        unit="requests",
        targets=[
            Target(
                expr="sum(ray_serve_deployment_queued_queries{{{global_filters}}}) by (deployment)",
                legend="{{deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 2, 8, 8),
    ),
    Panel(
        id=8,
        title="Pending requests per replica",
        description="Pending requests for each replica.",
        unit="requests",
        targets=[
            Target(
                expr="sum(ray_serve_replica_pending_queries{{{global_filters}}}) by (deployment, replica)",
                legend="{{replica}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(8, 2, 8, 8),
    ),
    Panel(
        id=9,
        title="Running requests per replica",
        description="Current running requests for each replica.",
        unit="requests",
        targets=[
            Target(
                expr="sum(ray_serve_replica_processing_queries{{{global_filters}}}) by (deployment, replica)",
                legend="{{replica}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(16, 2, 8, 8),
    ),
]

ids = []
for panel in SERVE_DEPLOYMENT_GRAFANA_PANELS:
    ids.append(panel.id)
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel. {ids}"

serve_deployment_dashboard_config = DashboardConfig(
    name="SERVE_DEPLOYMENT",
    default_uid="rayServeDeploymentDashboard",
    panels=SERVE_DEPLOYMENT_GRAFANA_PANELS,
    standard_global_filters=['deployment=~"$Deployment"', 'replica=~"$Replica"'],
    base_json_file_name="serve_deployment_grafana_dashboard_base.json",
)
