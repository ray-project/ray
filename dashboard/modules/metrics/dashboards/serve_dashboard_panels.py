# flake8: noqa E501

from ray.dashboard.modules.metrics.dashboards.common import GridPos, Panel, Target

SERVE_GRAFANA_PANELS = [
    Panel(
        id=5,
        title="Cluster Utilization",
        description="Aggregated utilization of all physical resources (CPU, GPU, memory, disk, or etc.) across the cluster. Ignores route variable.",
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
        grid_pos=GridPos(0, 0, 8, 8),
    ),
    Panel(
        id=1,
        title="Request QPS",
        description="Total number of requests and errored requests per second across selected routes.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_num_http_requests{{route=~"$Route",{global_filters}}}[5m]))',
                legend="Total QPS",
            ),
            Target(
                expr='sum(rate(ray_serve_num_http_error_requests{{route=~"$Route", {global_filters}}}[5m]))',
                legend="Error QPS",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(8, 0, 8, 8),
    ),
    # TODO(aguo): replace this with HTTP proxy latency metrics
    Panel(
        id=12,
        title="Request latency",
        description="P50, P90, P99 latency across selected routes.",
        unit="ms",
        targets=[
            Target(
                expr='sum(histogram_quantile(0.5, rate(ray_serve_http_request_latency_ms_bucket{{route=~"$Route",{global_filters}}}[5m])))',
                legend="P50",
            ),
            Target(
                expr='sum(histogram_quantile(0.9, rate(ray_serve_http_request_latency_ms_bucket{{route=~"$Route",{global_filters}}}[5m])))',
                legend="P90",
            ),
            Target(
                expr='sum(histogram_quantile(0.99, rate(ray_serve_http_request_latency_ms_bucket{{route=~"$Route",{global_filters}}}[5m])))',
                legend="P99",
            ),
        ],
        fill=1,
        linewidth=2,
        stack=False,
        grid_pos=GridPos(16, 0, 8, 8),
    ),
    Panel(
        id=7,
        title="QPS per route",
        description="QPS for each selected route.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_num_http_requests{{route=~"$Route",{global_filters}}}[5m])) by (route)',
                legend="{{endpoint}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 1, 12, 8),
    ),
    Panel(
        id=8,
        title="Error QPS per route",
        description="Error QPS for each selected route.",
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_num_http_error_requests{{route=~"$Route",{global_filters}}}[5m])) by (route)',
                legend="{{endpoint}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(12, 1, 12, 8),
    ),
    Panel(
        id=2,
        title="Deployments",
        description='Number of replicas per deployment. Ignores "Route" variable.',
        unit="replicas",
        targets=[
            Target(
                expr="sum(ray_serve_deployment_replica_healthy{{{global_filters}}}) by (deployment)",
                legend="{{deployment}}",
            ),
        ],
        grid_pos=GridPos(0, 2, 8, 8),
    ),
    Panel(
        id=13,
        title="QPS per deployment",
        description='QPS for each deployment. Ignores "Route" variable.',
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_deployment_request_counter{{route=~"$Route",{global_filters}}}[5m])) by (deployment)',
                legend="{{deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(8, 2, 8, 8),
    ),
    Panel(
        id=14,
        title="Error QPS per deployment",
        description='Error QPS for each deplyoment. Ignores "Route" variable.',
        unit="qps",
        targets=[
            Target(
                expr='sum(rate(ray_serve_deployment_error_counter{{route=~"$Route",{global_filters}}}[5m])) by (deployment)',
                legend="{{deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(16, 2, 8, 8),
    ),
    Panel(
        id=3,
        title="Queue size",
        description="Number of requests queued per deployment",
        unit="requests",
        targets=[
            Target(
                expr='sum(ray_serve_deployment_queued_queries{{route=~"$Route",{global_filters}}}) by (deployment)',
                legend="{{deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 3, 8, 8),
    ),
    Panel(
        id=4,
        title="Node count",
        description='Number of nodes in this cluster. Ignores "Route" variable.',
        unit="nodes",
        targets=[
            # TODO(aguo): Update this to use autoscaler metrics instead
            Target(
                expr="sum(ray_cluster_active_nodes{{{global_filters}}}) by (node_type)",
                legend="Active Nodes: {{node_type}}",
            ),
            Target(
                expr="sum(ray_cluster_failed_nodes{{{global_filters}}}) by (node_type)",
                legend="Failed Nodes: {{node_type}}",
            ),
            Target(
                expr="sum(ray_cluster_pending_nodes{{{global_filters}}}) by (node_type)",
                legend="Pending Nodes: {{node_type}}",
            ),
        ],
        grid_pos=GridPos(8, 3, 8, 8),
    ),
    Panel(
        id=6,
        title="Node network",
        description='Network speed per node. Ignores "Route" variable.',
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
        grid_pos=GridPos(16, 3, 8, 8),
    ),
    Panel(
        id=9,
        title="P50 latency",
        description='P50 latency per deployment. Ignores "Route" variable.',
        unit="ms",
        targets=[
            Target(
                expr='sum(histogram_quantile(0.5, rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",{global_filters}}}[5m]))) by (deployment)',
                legend="{{deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(0, 4, 8, 8),
    ),
    Panel(
        id=10,
        title="P90 latency",
        description='P90 latency per deployment. Ignores "Route" variable.',
        unit="ms",
        targets=[
            Target(
                expr='sum(histogram_quantile(0.9, rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",{global_filters}}}[5m]))) by (deployment)',
                legend="{{deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(8, 4, 8, 8),
    ),
    Panel(
        id=11,
        title="P99 latency",
        description='P99 latency per route. Ignores "Route" variable.',
        unit="ms",
        targets=[
            Target(
                expr='sum(histogram_quantile(0.99, rate(ray_serve_deployment_processing_latency_ms_bucket{{route=~"$Route",{global_filters}}}[5m]))) by (deployment)',
                legend="{{deployment}}",
            ),
        ],
        fill=0,
        stack=False,
        grid_pos=GridPos(16, 4, 8, 8),
    ),
]

ids = []
for panel in SERVE_GRAFANA_PANELS:
    ids.append(panel.id)
assert len(ids) == len(
    set(ids)
), f"Duplicated id found. Use unique id for each panel. {ids}"
