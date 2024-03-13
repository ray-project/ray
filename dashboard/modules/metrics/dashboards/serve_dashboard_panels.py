# flake8: noqa E501

from ray.dashboard.modules.metrics.dashboards.common import (
    DashboardConfig,
    GridPos,
    Panel,
    Target,
)

SERVE_GRAFANA_PANELS = [
    Panel(
        id=5,
        title="Cluster Utilization",
        description="Aggregated utilization of all physical resources (CPU, GPU, memory, disk, or etc.) across the cluster. Ignores application variable.",
        unit="%",
        targets=[
            # CPU
            Target(
                expr="avg(ray_node_cpu_utilization{{{global_filters}}})",
                legend="CPU (physical)",
            ),
            # GPU
            Target(
                expr="sum(ray_node_gpus_utilization{{{global_filters}}}) / on() (sum(autoscaler_cluster_resources{{resource='GPU',{global_filters}}}) or vector(0))",
                legend="GPU (physical)",
            ),
            # Memory
            Target(
                expr="sum(ray_node_mem_used{{{global_filters}}}) / on() (sum(ray_node_mem_total{{{global_filters}}})) * 100",
                legend="Memory (RAM)",
            ),
            # GRAM
            Target(
                expr="sum(ray_node_gram_used{{{global_filters}}}) / on() (sum(ray_node_gram_available{{{global_filters}}}) + sum(ray_node_gram_used{{{global_filters}}})) * 100",
                legend="GRAM",
            ),
            # Object Store
            Target(
                expr='sum(ray_object_store_memory{{{global_filters}}}) / on() sum(ray_resources{{Name="object_store_memory",{global_filters}}}) * 100',
                legend="Object Store Memory",
            ),
            # Disk
            Target(
                expr="sum(ray_node_disk_usage{{{global_filters}}}) / on() (sum(ray_node_disk_free{{{global_filters}}}) + sum(ray_node_disk_usage{{{global_filters}}})) * 100",
                legend="Disk",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 0, 12, 8),
    ),
    Panel(
        id=7,
        title="QPS per application",
        description="QPS for each selected application.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_num_http_requests_total{{application=~"$Application",application!~"",route=~"$HTTP_Route",route!~"/-/.*",{global_filters}}}[5m])) by (application, route)',
                legend="{{application, route}}",
            ),
            Target(
                expr='sum(rate(ray_serve_num_grpc_requests_total{{application=~"$Application",application!~"",method=~"$gRPC_Method",{global_filters}}}[5m])) by (application, method)',
                legend="{{application, method}}",
            ),
        ],
        grid_pos=GridPos(12, 0, 12, 8),
    ),
    Panel(
        id=8,
        title="Error QPS per application",
        description="Error QPS for each selected application.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_num_http_error_requests_total{{application=~"$Application",application!~"",route=~"$HTTP_Route",route!~"/-/.*",{global_filters}}}[5m])) by (application, route)',
                legend="{{application, route}}",
            ),
            Target(
                expr='sum(rate(ray_serve_num_grpc_error_requests_total{{application=~"$Application",application!~"",method=~"$gRPC_Method",{global_filters}}}[5m])) by (application, method)',
                legend="{{application, method}}",
            ),
        ],
        grid_pos=GridPos(0, 1, 12, 8),
    ),
    Panel(
        id=17,
        title="Error QPS per application per error code",
        description="Error QPS for each selected application.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_num_http_error_requests_total{{application=~"$Application",application!~"",route=~"$HTTP_Route",route!~"/-/.*",{global_filters}}}[5m])) by (application, route, error_code)',
                legend="{{application, route, error_code}}",
            ),
            Target(
                expr='sum(rate(ray_serve_num_grpc_error_requests_total{{application=~"$Application",application!~"",method=~"$gRPC_Method",{global_filters}}}[5m])) by (application, method, error_code)',
                legend="{{application, method, error_code}}",
            ),
        ],
        grid_pos=GridPos(12, 1, 12, 8),
    ),
    Panel(
        id=12,
        title="P50 latency per application",
        description="P50 latency for selected applications.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.5, sum(rate(ray_serve_http_request_latency_ms_bucket{{application=~"$Application",application!~"",route=~"$HTTP_Route",route!~"/-/.*",{global_filters}}}[5m])) by (application, route, le))',
                legend="{{application, route}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum(rate(ray_serve_grpc_request_latency_ms_bucket{{application=~"$Application",application!~"",method=~"$gRPC_Method",{global_filters}}}[5m])) by (application, method, le))',
                legend="{{application, method}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum(rate({{__name__=~ "ray_serve_(http|grpc)_request_latency_ms_bucket",application=~"$Application",application!~"",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 2, 8, 8),
    ),
    Panel(
        id=15,
        title="P90 latency per application",
        description="P90 latency for selected applications.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.9, sum(rate(ray_serve_http_request_latency_ms_bucket{{application=~"$Application",application!~"",route=~"$HTTP_Route",route!~"/-/.*",{global_filters}}}[5m])) by (application, route, le))',
                legend="{{application, route}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum(rate(ray_serve_grpc_request_latency_ms_bucket{{application=~"$Application",application!~"",method=~"$gRPC_Method",{global_filters}}}[5m])) by (application, method, le))',
                legend="{{application, method}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum(rate({{__name__=~ "ray_serve_(http|grpc)_request_latency_ms_bucket|ray_serve_grpc_request_latency_ms_bucket",application=~"$Application",application!~"",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(8, 2, 8, 8),
    ),
    Panel(
        id=16,
        title="P99 latency per application",
        description="P99 latency for selected applications.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.99, sum(rate(ray_serve_http_request_latency_ms_bucket{{application=~"$Application",application!~"",route=~"$HTTP_Route",route!~"/-/.*",{global_filters}}}[5m])) by (application, route, le))',
                legend="{{application, route}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum(rate(ray_serve_grpc_request_latency_ms_bucket{{application=~"$Application",application!~"",method=~"$gRPC_Method",{global_filters}}}[5m])) by (application, method, le))',
                legend="{{application, method}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum(rate({{__name__=~ "ray_serve_(http|grpc)_request_latency_ms_bucket|ray_serve_grpc_request_latency_ms_bucket",application=~"$Application",application!~"",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(16, 2, 8, 8),
    ),
    Panel(
        id=2,
        title="Replicas per deployment",
        description='Number of replicas per deployment. Ignores "Application" variable.',
        unit="replicas",
        targets=[
            Target(
                expr="sum(ray_serve_deployment_replica_healthy{{{global_filters}}}) by (application, deployment)",
                legend="{{application, deployment}}",
            ),
        ],
        grid_pos=GridPos(0, 3, 8, 8),
    ),
    Panel(
        id=13,
        title="QPS per deployment",
        description="QPS for each deployment.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_deployment_request_counter_total{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (application, deployment)',
                legend="{{application, deployment}}",
            ),
        ],
        grid_pos=GridPos(8, 3, 8, 8),
    ),
    Panel(
        id=14,
        title="Error QPS per deployment",
        description="Error QPS for each deplyoment.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_deployment_error_counter_total{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (application, deployment)',
                legend="{{application, deployment}}",
            ),
        ],
        grid_pos=GridPos(16, 3, 8, 8),
    ),
    Panel(
        id=9,
        title="P50 latency per deployment",
        description="P50 latency per deployment.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.5, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (application, deployment, le))',
                legend="{{application, deployment}}",
            ),
            Target(
                expr='histogram_quantile(0.5, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 4, 8, 8),
    ),
    Panel(
        id=10,
        title="P90 latency per deployment",
        description="P90 latency per deployment.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.9, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (application, deployment, le))',
                legend="{{application, deployment}}",
            ),
            Target(
                expr='histogram_quantile(0.9, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(8, 4, 8, 8),
    ),
    Panel(
        id=11,
        title="P99 latency per deployment",
        description="P99 latency per deployment.",
        unit="ms",
        targets=[
            Target(
                expr='histogram_quantile(0.99, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (application, deployment, le))',
                legend="{{application, deployment}}",
            ),
            Target(
                expr='histogram_quantile(0.99, sum(rate(ray_serve_deployment_processing_latency_ms_bucket{{application=~"$Application",application!~"",{global_filters}}}[5m])) by (le))',
                legend="Total",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(16, 4, 8, 8),
    ),
    Panel(
        id=3,
        title="Queue size per deployment",
        description='Number of requests queued per deployment. Ignores "Application" variable.',
        unit="requests",
        targets=[
            Target(
                expr="sum(ray_serve_deployment_queued_queries{{{global_filters}}}) by (application, deployment)",
                legend="{{application, deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 5, 8, 8),
    ),
    Panel(
        id=4,
        title="Node count",
        description='Number of nodes in this cluster. Ignores "Application" variable.',
        unit="nodes",
        targets=[
            # TODO(aguo): Update this to use autoscaler metrics instead
            Target(
                expr="sum(autoscaler_active_nodes{{{global_filters}}}) by (NodeType)",
                legend="Active Nodes: {{NodeType}}",
            ),
            Target(
                expr="sum(autoscaler_recently_failed_nodes{{{global_filters}}}) by (NodeType)",
                legend="Failed Nodes: {{NodeType}}",
            ),
            Target(
                expr="sum(autoscaler_pending_nodes{{{global_filters}}}) by (NodeType)",
                legend="Pending Nodes: {{NodeType}}",
            ),
        ],
        grid_pos=GridPos(8, 5, 8, 8),
    ),
    Panel(
        id=6,
        title="Node network",
        description='Network speed per node. Ignores "Application" variable.',
        unit="Bps",
        targets=[
            Target(
                expr="sum(ray_node_network_receive_speed{{{global_filters}}}) by (instance)",
                legend="Recv: {{instance}}",
            ),
            Target(
                expr="sum(ray_node_network_send_speed{{{global_filters}}}) by (instance)",
                legend="Send: {{instance}}",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(16, 5, 8, 8),
    ),
    Panel(
        id=20,
        title="Ongoing HTTP Requests",
        description="The number of ongoing requests in the HTTP Proxy.",
        unit="requests",
        targets=[
            Target(
                expr="ray_serve_num_ongoing_http_requests{{{global_filters}}}",
                legend="Ongoing HTTP Requests",
            ),
        ],
        grid_pos=GridPos(0, 6, 8, 8),
    ),
    Panel(
        id=21,
        title="Ongoing gRPC Requests",
        description="The number of ongoing requests in the gRPC Proxy.",
        unit="requests",
        targets=[
            Target(
                expr="ray_serve_num_ongoing_grpc_requests{{{global_filters}}}",
                legend="Ongoing gRPC Requests",
            ),
        ],
        grid_pos=GridPos(8, 6, 8, 8),
    ),
    Panel(
        id=22,
        title="Scheduling Tasks",
        description="The number of request scheduling tasks in the router.",
        unit="tasks",
        targets=[
            Target(
                expr="ray_serve_num_scheduling_tasks{{{global_filters}}}",
                legend="Scheduling Tasks",
            ),
        ],
        grid_pos=GridPos(16, 6, 8, 8),
    ),
    Panel(
        id=23,
        title="Scheduling Tasks in Backoff",
        description="The number of request scheduling tasks in the router that are undergoing backoff.",
        unit="tasks",
        targets=[
            Target(
                expr="ray_serve_num_scheduling_tasks_in_backoff{{{global_filters}}}",
                legend="Scheduling Tasks in Backoff",
            ),
        ],
        grid_pos=GridPos(0, 7, 8, 8),
    ),
    Panel(
        id=24,
        title="Controller Control Loop Duration",
        description="The duration of the last control loop.",
        unit="seconds",
        targets=[
            Target(
                expr="ray_serve_controller_control_loop_duration_s{{{global_filters}}}",
                legend="Control Loop Duration",
            ),
        ],
        grid_pos=GridPos(8, 7, 8, 8),
    ),
    Panel(
        id=25,
        title="Number of Control Loops",
        description="The number of control loops performed by the controller. Increases monotonically over the controller's lifetime.",
        unit="loops",
        targets=[
            Target(
                expr="ray_serve_controller_num_control_loops{{{global_filters}}}",
                legend="Control Loops",
            ),
        ],
        grid_pos=GridPos(16, 7, 8, 8),
    ),
]

ids = []
for panel in SERVE_GRAFANA_PANELS:
    ids.append(panel.id)
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel. {ids}"

serve_dashboard_config = DashboardConfig(
    name="SERVE",
    default_uid="rayServeDashboard",
    panels=SERVE_GRAFANA_PANELS,
    standard_global_filters=[],
    base_json_file_name="serve_grafana_dashboard_base.json",
)
