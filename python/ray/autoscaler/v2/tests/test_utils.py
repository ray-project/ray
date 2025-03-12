# coding: utf-8
import os
import sys
from typing import Dict

import pytest  # noqa
from google.protobuf.json_format import ParseDict

from ray.autoscaler.v2.schema import (
    ClusterConstraintDemand,
    ClusterStatus,
    LaunchRequest,
    NodeInfo,
    NodeUsage,
    PlacementGroupResourceDemand,
    RayTaskActorDemand,
    ResourceDemandSummary,
    ResourceRequestByCount,
    ResourceUsage,
    Stats,
)
from ray.autoscaler.v2.utils import (
    ClusterStatusFormatter,
    ClusterStatusParser,
    ResourceRequestUtil,
)
from ray.core.generated.autoscaler_pb2 import GetClusterStatusReply


def _gen_cluster_status_reply(data: Dict):
    return ParseDict(data, GetClusterStatusReply())


class TestResourceRequestUtil:
    @staticmethod
    def test_combine_requests_with_affinity():

        AFFINITY = ResourceRequestUtil.PlacementConstraintType.AFFINITY
        ANTI_AFFINITY = ResourceRequestUtil.PlacementConstraintType.ANTI_AFFINITY

        rqs = [
            ResourceRequestUtil.make({"CPU": 1}, [(AFFINITY, "1", "1")]),  # 1
            ResourceRequestUtil.make({"CPU": 2}, [(AFFINITY, "1", "1")]),  # 1
            ResourceRequestUtil.make({"CPU": 1}, [(AFFINITY, "2", "2")]),  # 2
            ResourceRequestUtil.make({"CPU": 1}, [(AFFINITY, "2", "2")]),  # 2
            ResourceRequestUtil.make({"CPU": 1}, [(ANTI_AFFINITY, "2", "2")]),  # 3
            ResourceRequestUtil.make({"CPU": 1}, [(ANTI_AFFINITY, "2", "2")]),  # 4
            ResourceRequestUtil.make({"CPU": 1}),  # 5
        ]

        rq_result = ResourceRequestUtil.combine_requests_with_affinity(rqs)
        assert len(rq_result) == 5
        actual = ResourceRequestUtil.to_dict_list(rq_result)
        expected = [
            ResourceRequestUtil.to_dict(
                ResourceRequestUtil.make(
                    {"CPU": 3},  # Combined
                    [
                        (AFFINITY, "1", "1"),
                    ],
                )
            ),
            ResourceRequestUtil.to_dict(
                ResourceRequestUtil.make(
                    {"CPU": 2},  # Combined
                    [
                        (AFFINITY, "2", "2"),
                    ],
                )
            ),
            ResourceRequestUtil.to_dict(
                ResourceRequestUtil.make(
                    {"CPU": 1},
                    [(ANTI_AFFINITY, "2", "2")],
                )
            ),
            ResourceRequestUtil.to_dict(
                ResourceRequestUtil.make(
                    {"CPU": 1},
                    [(ANTI_AFFINITY, "2", "2")],
                )
            ),
            ResourceRequestUtil.to_dict(
                ResourceRequestUtil.make(
                    {"CPU": 1},
                )
            ),
        ]

        actual_str_serialized = [str(x) for x in actual]
        expected_str_serialized = [str(x) for x in expected]

        assert sorted(actual_str_serialized) == sorted(expected_str_serialized)


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
                    "ray_node_type_name": "worker_node",
                    "available_resources": {},
                    "total_resources": {
                        "CPU": 1,
                        "GPU": 2.0,
                    },
                    "status": "DEAD",
                    "node_ip_address": "22.22.22.22",
                    "instance_type_name": "m5.large",
                },
                {
                    "node_id": b"3" * 4,
                    "instance_id": "instance3",
                    "ray_node_type_name": "worker_node",
                    "available_resources": {
                        "CPU": 1.0,
                        "GPU": 2.0,
                    },
                    "total_resources": {
                        "CPU": 1,
                        "GPU": 2.0,
                    },
                    "idle_duration_ms": 100,
                    "status": "IDLE",
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
                    "resource_requests": [
                        {
                            "request": {
                                "resources_bundle": {"GPU": 2, "CPU": 100},
                                "placement_constraints": [],
                            },
                            "count": 1,
                        },
                    ]
                }
            ],
            "cluster_resource_state_version": 10,
        },
        "autoscaling_state": {},
    }
    reply = _gen_cluster_status_reply(test_data)
    stats = Stats(gcs_request_time_s=0.1)
    cluster_status = ClusterStatusParser.from_get_cluster_status_reply(reply, stats)

    # Assert on health nodes
    assert len(cluster_status.idle_nodes) + len(cluster_status.active_nodes) == 2
    assert cluster_status.active_nodes[0].instance_id == "instance1"
    assert cluster_status.active_nodes[0].ray_node_type_name == "head_node"
    cluster_status.active_nodes[0].resource_usage.usage.sort(
        key=lambda x: x.resource_name
    )
    assert cluster_status.active_nodes[0].resource_usage == NodeUsage(
        usage=[
            ResourceUsage(resource_name="CPU", total=1.0, used=0.5),
            ResourceUsage(resource_name="GPU", total=2.0, used=0.0),
        ],
        idle_time_ms=0,
    )

    assert cluster_status.idle_nodes[0].instance_id == "instance3"
    assert cluster_status.idle_nodes[0].ray_node_type_name == "worker_node"
    cluster_status.idle_nodes[0].resource_usage.usage.sort(
        key=lambda x: x.resource_name
    )
    assert cluster_status.idle_nodes[0].resource_usage == NodeUsage(
        usage=[
            ResourceUsage(resource_name="CPU", total=1.0, used=0.0),
            ResourceUsage(resource_name="GPU", total=2.0, used=0.0),
        ],
        idle_time_ms=100,
    )

    # Assert on dead nodes
    assert len(cluster_status.failed_nodes) == 1
    assert cluster_status.failed_nodes[0].instance_id == "instance2"
    assert cluster_status.failed_nodes[0].ray_node_type_name == "worker_node"
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
    assert sorted(
        cluster_status.resource_demands.placement_group_demand, key=lambda x: x.pg_id
    ) == [
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

    # Assert on the cluster_resource_usage
    assert sorted(
        cluster_status.cluster_resource_usage, key=lambda x: x.resource_name
    ) == [
        ResourceUsage(resource_name="CPU", total=2.0, used=0.5),
        ResourceUsage(resource_name="GPU", total=4.0, used=0.0),
    ]

    # Assert on the node stats
    assert cluster_status.stats.cluster_resource_state_version == "10"
    assert cluster_status.stats.gcs_request_time_s == 0.1


def test_cluster_status_parser_autoscaler_state():
    test_data = {
        "cluster_resource_state": {},
        "autoscaling_state": {
            "pending_instance_requests": [
                {
                    "instance_type_name": "m5.large",
                    "ray_node_type_name": "head_node",
                    "count": 1,
                    "request_ts": 29999,
                },
                {
                    "instance_type_name": "m5.large",
                    "ray_node_type_name": "worker_node",
                    "count": 2,
                    "request_ts": 19999,
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
            "failed_instance_requests": [
                {
                    "instance_type_name": "m5.large",
                    "ray_node_type_name": "worker_node",
                    "count": 2,
                    "reason": "Insufficient capacity",
                    "start_ts": 10000,
                    "failed_ts": 20000,
                }
            ],
            "autoscaler_state_version": 10,
        },
    }
    reply = _gen_cluster_status_reply(test_data)
    stats = Stats(gcs_request_time_s=0.1)
    cluster_status = ClusterStatusParser.from_get_cluster_status_reply(reply, stats)

    # Assert on the pending requests
    assert len(cluster_status.pending_launches) == 2
    assert cluster_status.pending_launches[0].instance_type_name == "m5.large"
    assert cluster_status.pending_launches[0].ray_node_type_name == "head_node"
    assert cluster_status.pending_launches[0].count == 1
    assert cluster_status.pending_launches[0].request_ts_s == 29999
    assert cluster_status.pending_launches[1].instance_type_name == "m5.large"
    assert cluster_status.pending_launches[1].ray_node_type_name == "worker_node"
    assert cluster_status.pending_launches[1].count == 2
    assert cluster_status.pending_launches[1].request_ts_s == 19999

    # Assert on the failed requests
    assert len(cluster_status.failed_launches) == 1
    assert cluster_status.failed_launches[0].instance_type_name == "m5.large"
    assert cluster_status.failed_launches[0].ray_node_type_name == "worker_node"
    assert cluster_status.failed_launches[0].count == 2
    assert cluster_status.failed_launches[0].details == "Insufficient capacity"
    assert cluster_status.failed_launches[0].request_ts_s == 10000
    assert cluster_status.failed_launches[0].failed_ts_s == 20000

    # Assert on the pending nodes
    assert len(cluster_status.pending_nodes) == 1
    assert cluster_status.pending_nodes[0].instance_type_name == "m5.large"
    assert cluster_status.pending_nodes[0].ray_node_type_name == "head_node"
    assert cluster_status.pending_nodes[0].instance_id == "instance1"
    assert cluster_status.pending_nodes[0].ip_address == "10.10.10.10"
    assert cluster_status.pending_nodes[0].details == "Starting Ray"

    # Assert on stats
    assert cluster_status.stats.autoscaler_version == "10"
    assert cluster_status.stats.gcs_request_time_s == 0.1


def test_cluster_status_formatter():
    state = ClusterStatus(
        idle_nodes=[
            NodeInfo(
                instance_id="instance1",
                instance_type_name="m5.large",
                ray_node_type_name="head_node",
                ip_address="127.0.0.1",
                node_status="RUNNING",
                node_id="fffffffffffffffffffffffffffffffffffffffffffffffffff00001",
                resource_usage=NodeUsage(
                    usage=[
                        ResourceUsage(resource_name="CPU", total=1.0, used=0.5),
                        ResourceUsage(resource_name="GPU", total=2.0, used=0.0),
                        ResourceUsage(
                            resource_name="object_store_memory",
                            total=10282.0,
                            used=5555.0,
                        ),
                    ],
                    idle_time_ms=0,
                ),
            ),
            NodeInfo(
                instance_id="instance2",
                instance_type_name="m5.large",
                ray_node_type_name="worker_node",
                ip_address="127.0.0.2",
                node_status="RUNNING",
                node_id="fffffffffffffffffffffffffffffffffffffffffffffffffff00002",
                resource_usage=NodeUsage(
                    usage=[
                        ResourceUsage(resource_name="CPU", total=1.0, used=0),
                        ResourceUsage(resource_name="GPU", total=2.0, used=0),
                    ],
                    idle_time_ms=0,
                ),
            ),
            NodeInfo(
                instance_id="instance3",
                instance_type_name="m5.large",
                ray_node_type_name="worker_node",
                ip_address="127.0.0.2",
                node_status="RUNNING",
                node_id="fffffffffffffffffffffffffffffffffffffffffffffffffff00003",
                resource_usage=NodeUsage(
                    usage=[
                        ResourceUsage(resource_name="CPU", total=1.0, used=0.0),
                    ],
                    idle_time_ms=0,
                ),
            ),
        ],
        pending_launches=[
            LaunchRequest(
                instance_type_name="m5.large",
                count=2,
                ray_node_type_name="worker_node",
                state=LaunchRequest.Status.PENDING,
                request_ts_s=10000,
            ),
            LaunchRequest(
                instance_type_name="g5n.large",
                count=1,
                ray_node_type_name="worker_node_gpu",
                state=LaunchRequest.Status.PENDING,
                request_ts_s=20000,
            ),
        ],
        failed_launches=[
            LaunchRequest(
                instance_type_name="m5.large",
                count=2,
                ray_node_type_name="worker_node",
                state=LaunchRequest.Status.FAILED,
                details="Insufficient capacity",
                request_ts_s=10000,
                failed_ts_s=20000,
            ),
        ],
        pending_nodes=[
            NodeInfo(
                instance_id="instance4",
                instance_type_name="m5.large",
                ray_node_type_name="worker_node",
                ip_address="127.0.0.3",
                details="Starting Ray",
            ),
        ],
        failed_nodes=[
            NodeInfo(
                instance_id="instance5",
                instance_type_name="m5.large",
                ray_node_type_name="worker_node",
                ip_address="127.0.0.5",
                node_status="DEAD",
            ),
        ],
        cluster_resource_usage=[
            ResourceUsage(resource_name="CPU", total=3.0, used=0.5),
            ResourceUsage(resource_name="GPU", total=4.0, used=0.0),
            ResourceUsage(
                resource_name="object_store_memory", total=10282.0, used=5555.0
            ),
        ],
        resource_demands=ResourceDemandSummary(
            placement_group_demand=[
                PlacementGroupResourceDemand(
                    pg_id="1x1x",
                    strategy="STRICT_SPREAD",
                    state="PENDING",
                    details="1x1x:STRICT_SPREAD|PENDING",
                    bundles_by_count=[
                        ResourceRequestByCount(bundle={"CPU": 1, "GPU": 1}, count=1)
                    ],
                ),
                PlacementGroupResourceDemand(
                    pg_id="2x2x",
                    strategy="STRICT_PACK",
                    state="PENDING",
                    details="2x2x:STRICT_PACK|PENDING",
                    bundles_by_count=[
                        ResourceRequestByCount(bundle={"GPU": 2}, count=1)
                    ],
                ),
                PlacementGroupResourceDemand(
                    pg_id="3x3x",
                    strategy="STRICT_PACK",
                    state="PENDING",
                    details="3x3x:STRICT_PACK|PENDING",
                    bundles_by_count=[
                        ResourceRequestByCount(bundle={"GPU": 2}, count=1)
                    ],
                ),
            ],
            ray_task_actor_demand=[
                RayTaskActorDemand(
                    bundles_by_count=[
                        ResourceRequestByCount(bundle={"CPU": 1, "GPU": 1}, count=1)
                    ]
                ),
                RayTaskActorDemand(
                    bundles_by_count=[
                        ResourceRequestByCount(bundle={"CPU": 1, "GPU": 1}, count=10)
                    ]
                ),
            ],
            cluster_constraint_demand=[
                ClusterConstraintDemand(
                    bundles_by_count=[
                        ResourceRequestByCount(bundle={"GPU": 2, "CPU": 100}, count=2)
                    ]
                ),
            ],
        ),
        stats=Stats(
            gcs_request_time_s=0.1,
            none_terminated_node_request_time_s=0.2,
            autoscaler_iteration_time_s=0.3,
            autoscaler_version="10",
            cluster_resource_state_version="20",
            request_ts_s=775303535,
        ),
    )
    actual = ClusterStatusFormatter.format(state, verbose=True)

    expected = """======== Autoscaler status: 1994-07-27 10:05:35 ========
GCS request time: 0.100000s
Node Provider non_terminated_nodes time: 0.200000s
Autoscaler iteration time: 0.300000s

Node status
--------------------------------------------------------
Active:
 (no active nodes)
Idle:
 1 head_node
 2 worker_node
Pending:
 worker_node, 1 launching
 worker_node_gpu, 1 launching
 instance4: worker_node, starting ray
Recent failures:
 worker_node: LaunchFailed (latest_attempt: 02:46:40) - Insufficient capacity
 worker_node: NodeTerminated (instance_id: instance5)

Resources
--------------------------------------------------------
Total Usage:
 0.5/3.0 CPU
 0.0/4.0 GPU
 5.42KiB/10.04KiB object_store_memory

Total Demands:
 {'CPU': 1, 'GPU': 1}: 11+ pending tasks/actors
 {'CPU': 1, 'GPU': 1} * 1 (STRICT_SPREAD): 1+ pending placement groups
 {'GPU': 2} * 1 (STRICT_PACK): 2+ pending placement groups
 {'GPU': 2, 'CPU': 100}: 2+ from request_resources()

Node: instance1 (head_node)
 Id: fffffffffffffffffffffffffffffffffffffffffffffffffff00001
 Usage:
  0.5/1.0 CPU
  0.0/2.0 GPU
  5.42KiB/10.04KiB object_store_memory
 Activity:
  (no activity)

Node: instance2 (worker_node)
 Id: fffffffffffffffffffffffffffffffffffffffffffffffffff00002
 Usage:
  0/1.0 CPU
  0/2.0 GPU
 Activity:
  (no activity)

Node: instance3 (worker_node)
 Id: fffffffffffffffffffffffffffffffffffffffffffffffffff00003
 Usage:
  0.0/1.0 CPU
 Activity:
  (no activity)"""
    assert actual == expected


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
