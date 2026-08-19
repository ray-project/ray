import sys
import types

import pytest

import ray
import ray._private.gcs_utils as gcs_utils
from ray.autoscaler._private import (
    load_metrics as load_metrics_module,
    monitor as monitor_module,
)
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.monitor import parse_resource_demands
from ray.core.generated import autoscaler_pb2, gcs_service_pb2

ray.experimental.internal_kv.redis = False


def test_parse_resource_demands():
    resource_load_by_shape = gcs_utils.ResourceLoad(
        resource_demands=[
            gcs_utils.ResourceDemand(
                shape={"CPU": 1},
                num_ready_requests_queued=1,
                num_infeasible_requests_queued=0,
                backlog_size=0,
            ),
            gcs_utils.ResourceDemand(
                shape={"CPU": 2},
                num_ready_requests_queued=1,
                num_infeasible_requests_queued=0,
                backlog_size=1,
            ),
            gcs_utils.ResourceDemand(
                shape={"CPU": 3},
                num_ready_requests_queued=0,
                num_infeasible_requests_queued=1,
                backlog_size=2,
            ),
            gcs_utils.ResourceDemand(
                shape={"CPU": 4},
                num_ready_requests_queued=1,
                num_infeasible_requests_queued=1,
                backlog_size=2,
            ),
        ]
    )

    waiting, infeasible = parse_resource_demands(resource_load_by_shape)

    assert waiting.count({"CPU": 1}) == 1
    assert waiting.count({"CPU": 2}) == 2
    assert infeasible.count({"CPU": 3}) == 3
    # The {"CPU": 4} case here is inconsistent, but could happen. Since the
    # heartbeats are eventually consistent, we won't worry about whether it's
    # counted as infeasible or waiting, as long as it's accounted for and
    # doesn't cause an error.
    assert len(waiting + infeasible) == 10


def test_update_load_metrics_uses_cluster_state(monkeypatch):
    """Ensure cluster_resource_state fields flow into LoadMetrics.

    Verify node data comes from cluster_resource_state while demand parsing
    still uses resource_load_by_shape.
    """

    monitor = monitor_module.Monitor.__new__(monitor_module.Monitor)
    monitor.gcs_client = types.SimpleNamespace()
    monitor.load_metrics = LoadMetrics()
    monitor.autoscaler = types.SimpleNamespace(config={"provider": {}})
    monitor.autoscaling_config = None
    monitor.readonly_config = None
    monitor.prom_metrics = None
    monitor.event_summarizer = None

    usage_reply = gcs_service_pb2.GetAllResourceUsageReply()
    demand = (
        usage_reply.resource_usage_data.resource_load_by_shape.resource_demands.add()
    )
    demand.shape["CPU"] = 1.0
    demand.num_ready_requests_queued = 2
    demand.backlog_size = 1

    monitor.gcs_client.get_all_resource_usage = lambda timeout: usage_reply

    cluster_state = autoscaler_pb2.ClusterResourceState()
    node_state = cluster_state.node_states.add()
    node_state.node_id = bytes.fromhex("ab" * 20)
    node_state.node_ip_address = "1.2.3.4"
    node_state.total_resources["CPU"] = 4.0
    node_state.available_resources["CPU"] = 1.5
    node_state.idle_duration_ms = 1500

    monkeypatch.setattr(
        monitor_module, "get_cluster_resource_state", lambda gcs_client: cluster_state
    )

    seen = {}
    orig_parse = monitor_module.parse_resource_demands

    def spy_parse(arg):
        # Spy on the legacy parser to ensure resource_load_by_shape still feeds it.
        seen["arg"] = arg
        return orig_parse(arg)

    monkeypatch.setattr(monitor_module, "parse_resource_demands", spy_parse)

    fixed_time = 1000.0
    monkeypatch.setattr(
        load_metrics_module, "time", types.SimpleNamespace(time=lambda: fixed_time)
    )

    monitor.update_load_metrics()

    resources = monitor.load_metrics.static_resources_by_ip
    assert resources["1.2.3.4"]["CPU"] == pytest.approx(4.0)

    usage = monitor.load_metrics.dynamic_resources_by_ip
    assert usage["1.2.3.4"]["CPU"] == pytest.approx(1.5)

    assert seen["arg"] is usage_reply.resource_usage_data.resource_load_by_shape

    assert monitor.load_metrics.pending_placement_groups == []

    waiting = monitor.load_metrics.waiting_bundles
    infeasible = monitor.load_metrics.infeasible_bundles
    assert waiting.count({"CPU": 1.0}) == 3
    assert not infeasible

    last_used = monitor.load_metrics.ray_nodes_last_used_time_by_ip["1.2.3.4"]
    assert last_used == pytest.approx(fixed_time - 1.5)


def test_update_load_metrics_skips_dead_nodes(monkeypatch):
    """Dead GCS nodes must not be written into LoadMetrics by IP."""

    class FakeReadonlyProvider:
        def __init__(self):
            self.nodes = None

        def _set_nodes(self, nodes):
            self.nodes = nodes

    provider = FakeReadonlyProvider()
    monitor = monitor_module.Monitor.__new__(monitor_module.Monitor)
    monitor.gcs_client = types.SimpleNamespace()
    monitor.load_metrics = LoadMetrics()
    monitor.autoscaler = types.SimpleNamespace(
        config={"provider": {}}, provider=provider
    )
    monitor.autoscaling_config = None
    monitor.readonly_config = {"available_node_types": {}}
    monitor.prom_metrics = None
    monitor.event_summarizer = None
    usage_reply = gcs_service_pb2.GetAllResourceUsageReply()
    monitor.gcs_client.get_all_resource_usage = lambda timeout: usage_reply

    reused_ip = "10.0.0.5"
    live_node_id = bytes.fromhex("cc" * 20)
    cluster_state = autoscaler_pb2.ClusterResourceState()

    # Live node first, DEAD second: without the skip, last-write-wins would
    # replace the live mapping with the dead raylet's empty metrics.
    live_node = cluster_state.node_states.add()
    live_node.node_id = live_node_id
    live_node.node_ip_address = reused_ip
    live_node.status = autoscaler_pb2.NodeStatus.RUNNING
    live_node.total_resources["CPU"] = 2.0
    live_node.available_resources["CPU"] = 1.0

    dead_node = cluster_state.node_states.add()
    dead_node.node_id = bytes.fromhex("aa" * 20)
    dead_node.node_ip_address = reused_ip
    dead_node.status = autoscaler_pb2.NodeStatus.DEAD

    monkeypatch.setattr(
        monitor_module, "get_cluster_resource_state", lambda gcs_client: cluster_state
    )

    monitor.update_load_metrics()

    assert monitor.load_metrics.node_id_by_ip[reused_ip] == live_node_id
    assert monitor.load_metrics.static_resources_by_ip[reused_ip][
        "CPU"
    ] == pytest.approx(2.0)

    readonly_ids = {node_id for node_id, _ in provider.nodes}
    assert live_node_id.hex() in readonly_ids
    assert dead_node.node_id.hex() not in readonly_ids


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
