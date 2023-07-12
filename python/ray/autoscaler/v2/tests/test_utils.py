# coding: utf-8
import os
import sys
from typing import Dict

import pytest  # noqa
from google.protobuf.json_format import ParseDict

from ray.autoscaler.v2.schema import (
    NodeUsage,
    PlacementGroupResourceDemand,
    ResourceRequestByCount,
    ResourceUsage,
    Stats,
)
from ray.autoscaler.v2.utils import ClusterStatusParser
from ray.core.generated.experimental.autoscaler_pb2 import GetClusterStatusReply


def _gen_cluster_status_reply(data: Dict):
    return ParseDict(data, GetClusterStatusReply())


def test_cluster_status_parser_cluster_resource_state():
    test_data = {
        "cluster_resource_state": {
            "node_states": [
                {
                    "node_id": b"1" * 4,
                    "instance_id": "instance1",
                    "ray_node_type_name": "head_node",
                    "available_resources": {
                        "CPU": 0.5,
                        "GPU": 2.0,
                    },
                    "total_resources": {
                        "CPU": 1,
                        "GPU": 2.0,
                    },
                    "status": "RUNNING",
                    "node_ip_address": "10.10.10.10",
                    "instance_type_name": "m5.large",
                },
                {
                    "node_id": b"2" * 4,
                    "instance_id": "instance2",
                    "ray_node_type_name": "head_node",
                    "available_resources": {},
                    "total_resources": {
                        "CPU": 1,
                        "GPU": 2.0,
                    },
                    "status": "DEAD",
                    "node_ip_address": "22.22.22.22",
                    "instance_type_name": "m5.large",
                },
            ],
            "pending_gang_resource_requests": [
                {
                    "requests": [
                        {
                            "resources_bundle": {"CPU": 1, "GPU": 1},
                            "placement_constraints": [
                                {
                                    "anti_affinity": {
                                        "label_name": "_PG_1x1x",
                                        "label_value": "",
                                    }
                                }
                            ],
                        },
                    ],
                    "details": "1x1x:STRICT_SPREAD|PENDING",
                },
                {
                    "requests": [
                        {
                            "resources_bundle": {"GPU": 2},
                            "placement_constraints": [
                                {
                                    "affinity": {
                                        "label_name": "_PG_2x2x",
                                        "label_value": "",
                                    }
                                }
                            ],
                        },
                    ],
                    "details": "2x2x:STRICT_PACK|PENDING",
                },
            ],
            "pending_resource_requests": [
                {
                    "request": {
                        "resources_bundle": {"CPU": 1, "GPU": 1},
                        "placement_constraints": [],
                    },
                    "count": 1,
                },
            ],
            "cluster_resource_constraints": [
                {
                    "min_bundles": [
                        {
                            "resources_bundle": {"GPU": 2, "CPU": 100},
                            "placement_constraints": [],
                        },
                    ]
                }
            ],
            "cluster_resource_state_version": 10,
        },
        "autoscaling_state": {},
    }
    reply = _gen_cluster_status_reply(test_data)
    stats = Stats()
    cluster_status = ClusterStatusParser.from_get_cluster_status_reply(reply, stats)

    # Assert on health nodes
    assert len(cluster_status.healthy_nodes) == 1
    assert cluster_status.healthy_nodes[0].instance_id == "instance1"
    assert cluster_status.healthy_nodes[0].ray_node_type_name == "head_node"
    assert cluster_status.healthy_nodes[0].resource_usage == NodeUsage(
        usage=[
            ResourceUsage(resource_name="CPU", total=1.0, used=0.5),
            ResourceUsage(resource_name="GPU", total=2.0, used=0.0),
        ],
        idle_time_ms=0,
    )

    # Assert on dead nodes
    assert len(cluster_status.failed_nodes) == 1
    assert cluster_status.failed_nodes[0].instance_id == "instance2"
    assert cluster_status.failed_nodes[0].ray_node_type_name == "head_node"
    assert cluster_status.failed_nodes[0].resource_usage is None

    # Assert on resource demands from tasks
    assert len(cluster_status.resource_demands.ray_task_actor_demand) == 1
    assert cluster_status.resource_demands.ray_task_actor_demand[
        0
    ].bundles_by_count == [
        ResourceRequestByCount(
            bundle={"CPU": 1, "GPU": 1},
            count=1,
        )
    ]

    # Assert on resource demands from placement groups
    assert len(cluster_status.resource_demands.placement_group_demand) == 2
    assert cluster_status.resource_demands.placement_group_demand == [
        PlacementGroupResourceDemand(
            bundles_by_count=[
                ResourceRequestByCount(bundle={"CPU": 1, "GPU": 1}, count=1)
            ],
            strategy="STRICT_SPREAD",
            pg_id="1x1x",
            state="PENDING",
            details="1x1x:STRICT_SPREAD|PENDING",
        ),
        PlacementGroupResourceDemand(
            bundles_by_count=[ResourceRequestByCount(bundle={"GPU": 2}, count=1)],
            strategy="STRICT_PACK",
            pg_id="2x2x",
            state="PENDING",
            details="2x2x:STRICT_PACK|PENDING",
        ),
    ]

    # Assert on resource constraints
    assert len(cluster_status.resource_demands.cluster_constraint_demand) == 1
    assert cluster_status.resource_demands.cluster_constraint_demand[
        0
    ].bundles_by_count == [
        ResourceRequestByCount(bundle={"GPU": 2, "CPU": 100}, count=1)
    ]

    # Assert on the node stats
    assert cluster_status.stats.cluster_resource_state_version == "10"


def test_cluster_status_parser_autoscaler_state():
    test_data = {
        "cluster_resource_state": {},
        "autoscaling_state": {
            "pending_instance_requests": [
                {
                    "instance_type_name": "m5.large",
                    "ray_node_type_name": "head_node",
                    "count": 1,
                },
                {
                    "instance_type_name": "m5.large",
                    "ray_node_type_name": "worker_node",
                    "count": 2,
                },
            ],
            "pending_instances": [
                {
                    "instance_type_name": "m5.large",
                    "ray_node_type_name": "head_node",
                    "instance_id": "instance1",
                    "ip_address": "10.10.10.10",
                    "details": "Starting Ray",
                },
            ],
            "autoscaler_state_version": 10,
        },
    }
    reply = _gen_cluster_status_reply(test_data)
    stats = Stats()
    cluster_status = ClusterStatusParser.from_get_cluster_status_reply(reply, stats)

    # Assert on the pending requests
    assert len(cluster_status.pending_launches) == 2
    assert cluster_status.pending_launches[0].instance_type_name == "m5.large"
    assert cluster_status.pending_launches[0].ray_node_type_name == "head_node"
    assert cluster_status.pending_launches[0].count == 1
    assert cluster_status.pending_launches[1].instance_type_name == "m5.large"
    assert cluster_status.pending_launches[1].ray_node_type_name == "worker_node"
    assert cluster_status.pending_launches[1].count == 2

    # Assert on the pending nodes
    assert len(cluster_status.pending_nodes) == 1
    assert cluster_status.pending_nodes[0].instance_type_name == "m5.large"
    assert cluster_status.pending_nodes[0].ray_node_type_name == "head_node"
    assert cluster_status.pending_nodes[0].instance_id == "instance1"
    assert cluster_status.pending_nodes[0].ip_address == "10.10.10.10"
    assert cluster_status.pending_nodes[0].details == "Starting Ray"

    # Assert on stats
    assert cluster_status.stats.autoscaler_version == "10"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
