import json
import os
import sys

# coding: utf-8
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pytest
from google.protobuf.json_format import ParseDict

from ray.autoscaler.v2.scheduler import (
    SimpleResourceScheduler,
    flatten_requests_by_count,
    to_map,
)
from ray.core.generated.instance_manager_pb2 import ScheduleResourceBundlesRequest

ResourceMap = Dict[str, float]


def sched_req(
    schedule_config: Dict[str, Any],
    resource_requests: Optional[List[ResourceMap]] = None,
    gang_requests: Optional[
        List[List[Tuple[ResourceMap, List[Dict[str, str]]]]]
    ] = None,
    resource_constraints: Optional[List[ResourceMap]] = None,
    nodes: Optional[List[Tuple[str, ResourceMap, ResourceMap]]] = None,
):

    # group resources request by shape
    resource_requests_by_count = defaultdict(int)
    for request in resource_requests or []:
        bundle = json.dumps(request)
        resource_requests_by_count[bundle] += 1

    # group cluster resource constraints by shape
    cluster_resource_constraints_by_count = defaultdict(int)
    for constraint in resource_constraints or []:
        bundle = json.dumps(constraint)
        cluster_resource_constraints_by_count[bundle] += 1

    node_states = []
    if nodes:
        node_states = [
            {
                "node_id": f"{node_type}-{i}",
                "instance_id": f"{node_type}-{i}",
                "ray_node_type_name": node_type,
                "total_resources": total,
                "available_resources": available,
            }
            for i, (node_type, total, available) in enumerate(nodes)
        ]

    constraints = (
        [
            {
                "min_bundles": [
                    {
                        "request": {
                            "resources_bundle": json.loads(bundles),
                        },
                        "count": count,
                    }
                    for bundles, count in cluster_resource_constraints_by_count.items()
                ],
            }
        ]
        if cluster_resource_constraints_by_count
        else []
    )

    request = ParseDict(
        {
            "resource_requests": [
                {
                    "request": {
                        "resources_bundle": json.loads(bundles),
                    },
                    "count": count,
                }
                for bundles, count in resource_requests_by_count.items()
            ],
            "gang_resource_requests": [
                {
                    "requests": [
                        {
                            "resources_bundle": bundles,
                            "placement_constraints": constraints,
                        }
                        for bundles, constraints in req
                    ]
                }
                for req in gang_requests or []
            ],
            "cluster_resource_constraints": constraints,
            "schedule_config": schedule_config,
            "node_states": node_states,
        },
        ScheduleResourceBundlesRequest(),
    )
    return request


def test_only_resource_requests():
    sched_config = {
        "node_type_configs": {
            "type_1": {
                "max_workers": 10,
                "min_workers": 0,
                "name": "type_1",
                "resources": {"CPU": 1},
            }
        }
    }
    scheduler = SimpleResourceScheduler()
    request = sched_req(
        resource_requests=[{"CPU": 1}],
        schedule_config=sched_config,
    )
    reply = scheduler.schedule_resource_bundles(request)

    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 1}

    request = sched_req(
        resource_requests=[{"CPU": 1}, {"CPU": 1}],
        schedule_config=sched_config,
    )

    reply = scheduler.schedule_resource_bundles(request)
    assert reply.to_launch_nodes == {"type_1": 2}

    # Infeasible
    request = sched_req(
        resource_requests=[{"CPU": 2}, {"CPU": 1}],
        schedule_config=sched_config,
    )

    reply = scheduler.schedule_resource_bundles(request)
    assert reply.to_launch_nodes == {"type_1": 1}
    assert to_map(flatten_requests_by_count(reply.infeasible_resource_requests)) == [
        {"CPU": 2}
    ]

    # multiple node types
    sched_config = {
        "node_type_configs": {
            "type_1": {
                "max_workers": 10,
                "min_workers": 0,
                "name": "type_1",
                "resources": {"CPU": 1},
            },
            "type_2": {
                "max_workers": 10,
                "min_workers": 0,
                "name": "type_2",
                "resources": {"CPU": 2},
            },
        }
    }

    request = sched_req(
        resource_requests=[{"CPU": 2}, {"CPU": 1}],
        schedule_config=sched_config,
    )

    reply = scheduler.schedule_resource_bundles(request)
    assert reply.to_launch_nodes == {"type_1": 1, "type_2": 1}

    # multiple node types but infeasible resource requests
    request = sched_req(
        resource_requests=[{"CPU": 2}, {"CPU": 1}, {"CPU": 1, "GPU": 1}],
        schedule_config=sched_config,
    )

    reply = scheduler.schedule_resource_bundles(request)
    assert reply.to_launch_nodes == {"type_1": 1, "type_2": 1}
    assert to_map(flatten_requests_by_count(reply.infeasible_resource_requests)) == [
        {"CPU": 1, "GPU": 1}
    ]


def test_placement_constraints():
    sched_config = {
        "node_type_configs": {
            "type_1": {
                "max_workers": 10,
                "min_workers": 0,
                "name": "type_1",
                "resources": {"CPU": 3},
            },
        }
    }

    # No constraints, should be packing onto a single node.
    scheduler = SimpleResourceScheduler()
    request = sched_req(
        schedule_config=sched_config,
        resource_requests=[],
        gang_requests=[[({"CPU": 1}, []), ({"CPU": 1}, []), ({"CPU": 1}, [])]],
    )
    reply = scheduler.schedule_resource_bundles(request)

    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 1}

    # With anti-affinity constraints, it should be spread out.
    scheduler = SimpleResourceScheduler()
    anti_pc = {
        "anti_affinity": {
            "label_name": "ak",
            "label_value": "av",
        }
    }
    request = sched_req(
        schedule_config=sched_config,
        resource_requests=[],
        gang_requests=[
            [
                ({"CPU": 1}, [anti_pc]),
                ({"CPU": 1}, [anti_pc]),
                ({"CPU": 1}, [anti_pc]),
            ]
        ],
    )
    reply = scheduler.schedule_resource_bundles(request)
    assert reply.to_launch_nodes == {"type_1": 3}

    # With affinity constraints, it should be packed, thus
    # not schedulable with the current node type.
    affinity_pc = {
        "affinity": {
            "label_name": "c",
            "label_value": "c1",
        }
    }
    request = sched_req(
        schedule_config=sched_config,
        resource_requests=[],
        gang_requests=[
            [
                ({"CPU": 3}, [affinity_pc]),
                ({"CPU": 3}, [affinity_pc]),
            ]
        ],
    )
    reply = scheduler.schedule_resource_bundles(request)
    assert len(reply.to_launch_nodes) == 0
    assert len(reply.infeasible_gang_resource_requests) == 1


def test_cluster_constraints():
    sched_config = {
        "node_type_configs": {
            "type_1": {
                "max_workers": 4,
                "min_workers": 2,
                "name": "type_1",
                "resources": {"CPU": 2},
            },
        }
    }
    request = sched_req(
        schedule_config=sched_config,
    )
    scheduler = SimpleResourceScheduler()
    reply = scheduler.schedule_resource_bundles(request)
    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 2}

    # With the min workers, resources should be fulfilled.
    request = sched_req(
        resource_requests=[{"CPU": 2}] * 2,
        schedule_config=sched_config,
    )
    reply = scheduler.schedule_resource_bundles(request)
    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 2}

    # With extra load, it should launch more nodes on top of the min workers.
    request = sched_req(
        resource_requests=[{"CPU": 2}] * 3,
        schedule_config=sched_config,
    )
    reply = scheduler.schedule_resource_bundles(request)
    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 3}

    # With the max workers, it should not launch more nodes.
    # Instead, it should return the extra resources as infeasible.
    request = sched_req(
        resource_requests=[{"CPU": 2}] * 5,
        schedule_config=sched_config,
    )
    reply = scheduler.schedule_resource_bundles(request)
    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 4}
    assert to_map(flatten_requests_by_count(reply.infeasible_resource_requests)) == [
        {"CPU": 2}
    ]

    # Cluster resource constraints should be applied on top of the min
    # worker counts.
    request = sched_req(
        schedule_config=sched_config,
        resource_requests=[],
        resource_constraints=[{"CPU": 2}] * 3,
    )
    reply = scheduler.schedule_resource_bundles(request)
    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 3}

    # Cluster resource constraints should not exceed the max worker counts.
    request = sched_req(
        schedule_config=sched_config,
        resource_requests=[],
        resource_constraints=[{"CPU": 2}] * 5,
    )
    reply = scheduler.schedule_resource_bundles(request)
    assert len(reply.to_launch_nodes) == 1
    assert reply.to_launch_nodes == {"type_1": 2}  # just the min workers.


def test_mix_demand():
    """
    Test that a mix of the various demands:
    - resource requests from tasks/actors
    - gang requests from placement groups
    - cluster resource constraints
    - min/max worker counts
    - existing nodes.
    """

    sched_config = {
        "node_type_configs": {
            "type_1": {
                "max_workers": 4,
                "min_workers": 2,
                "name": "type_1",
                "resources": {"CPU": 4},
            },
            "type_2": {
                "max_workers": 10,
                "min_workers": 0,
                "name": "type_2",
                "resources": {"CPU": 1, "GPU": 1},
            },
        }
    }

    # Placement constraints
    anti_pc = {
        "anti_affinity": {
            "label_name": "ak",
            "label_value": "av",
        }
    }
    affinity_pc = {
        "affinity": {
            "label_name": "c",
            "label_value": "c1",
        }
    }
    gang_requests = [
        [
            ({"CPU": 2}, [anti_pc]),
            ({"CPU": 2}, [anti_pc]),
            ({"CPU": 2}, [anti_pc]),
            ({"CPU": 2}, [anti_pc]),
        ],
        [
            ({"CPU": 3}, [affinity_pc]),
            ({"CPU": 3}, [affinity_pc]),  # Cannot fit
        ],
        [
            ({"CPU": 1}, []),
            ({"CPU": 1}, []),
            ({"CPU": 1}, []),
        ],
    ]

    # Resource requests
    resource_requests = [{"CPU": 2}, {"GPU": 1, "CPU": 1}, {"GPU": 1}]

    # Cluster constraints
    cluster_constraints = [{"CPU": 1}] * 10

    request = sched_req(
        schedule_config=sched_config,
        resource_requests=resource_requests,
        gang_requests=gang_requests,
        resource_constraints=cluster_constraints,
        nodes=[
            ("type_1", {"CPU": 4}, {"CPU": 2}),
            ("type_2", {"CPU": 1, "GPU": 1}, {"CPU": 1, "GPU": 1}),
        ],
    )

    scheduler = SimpleResourceScheduler()
    reply = scheduler.schedule_resource_bundles(request)

    # Calculate the expected number of nodes to launch:
    # - 1 type_1, 1 type_2 to start with => CPU: 2/5, GPU: 1/1
    # - added 1 type_1 for minimal request -> +1 type_1
    # ==> 2 type_1, 1 type_2 (CPU: 6/9, GPU: 1/1)
    # - enforce cluster constraint (10 CPU) -> +1 type_1, CPU: 10/13, GPU: 1/1
    # ==> 3 type_1, 1 type_2 (CPU: 10/13, GPU: 1/1)
    # - sched gang requests:
    #   - anti affinity (8CPU) => +1 type_1, CPU: 6/17, GPU: 1/1
    #   - no constraint (3CPU) => CPU: 3/17, GPU: 1/1
    #   - affinity (not feasible)
    # ==> 4 type_1, 1 type_2 (CPU: 3/17, GPU: 1/1)
    # - sched resource requests:
    #   - 2CPU => CPU: 1/17, GPU: 1/1
    #   - 1GPU, 1CPU => CPU: 0/17, GPU: 0/1
    #   - 1GPU => not schedulable
    # ==> 4 type_1, 1 type_2 (CPU: 0/17, GPU: 0/1)
    # Therefore:
    # - added nodes: 3 type_1, 1 type_2
    # - infeasible: 1 gang request, 1 resource request
    assert len(reply.to_launch_nodes) == 2
    assert sorted(reply.to_launch_nodes.items()) == sorted(
        {"type_1": 3, "type_2": 1}.items()
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
