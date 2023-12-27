import os
import sys

# coding: utf-8
from typing import Dict, List, Optional

import pytest

import ray.autoscaler.v2.scheduler
from ray.autoscaler.v2.scheduler import (
    NodeTypeConfig,
    ResourceDemandScheduler,
    SchedulingRequest,
    logger,
)
from ray.autoscaler.v2.schema import NodeType
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    NodeState,
    ResourceRequest,
)
from ray.core.generated.instance_manager_pb2 import Instance

logger.setLevel("DEBUG")


ResourceMap = Dict[str, float]


def sched_request(
    node_type_configs: Dict[NodeType, NodeTypeConfig],
    max_num_worker_nodes: Optional[int] = None,
    resource_requests: Optional[List[ResourceRequest]] = None,
    gang_resource_requests: Optional[List[GangResourceRequest]] = None,
    cluster_resource_constraints: Optional[List[ClusterResourceConstraint]] = None,
    current_nodes: Optional[List[NodeState]] = None,
    current_instances: Optional[List[Instance]] = None,
) -> SchedulingRequest:

    if resource_requests is None:
        resource_requests = []
    if gang_resource_requests is None:
        gang_resource_requests = []
    if cluster_resource_constraints is None:
        cluster_resource_constraints = []
    if current_nodes is None:
        current_nodes = []
    if current_instances is None:
        current_instances = []

    return SchedulingRequest(
        resource_requests=ResourceRequestUtil.group_by_count(resource_requests),
        gang_resource_requests=gang_resource_requests,
        cluster_resource_constraints=cluster_resource_constraints,
        current_nodes=current_nodes,
        current_instances=current_instances,
        node_type_configs=node_type_configs,
        max_num_worker_nodes=max_num_worker_nodes,
    )


def test_min_worker_nodes():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=1,
            max_worker_nodes=10,
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
        "type_3": NodeTypeConfig(
            name="type_3",
            resources={"CPU": 1},
            min_worker_nodes=2,
            max_worker_nodes=10,
        ),
    }
    # With empty cluster
    request = sched_request(
        node_type_configs=node_type_configs,
    )

    reply = scheduler.schedule(request)

    target_cluster_shape = {"type_1": 1, "type_3": 2}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)

    # With existing ray nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        current_nodes=[
            NodeState(ray_node_type_name="type_1"),
            NodeState(ray_node_type_name="type_1"),
        ],
    )

    reply = scheduler.schedule(request)
    target_cluster_shape = {"type_1": 2, "type_3": 2}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)

    # With existing instances pending.
    request = sched_request(
        node_type_configs=node_type_configs,
        current_instances=[
            Instance(instance_type="type_1", status=Instance.REQUESTED),
            Instance(instance_type="type_1", status=Instance.ALLOCATED),
            Instance(
                instance_type="type_1", status=Instance.RAY_STOPPING
            ),  # should not count
            Instance(
                instance_type="type_1", status=Instance.RAY_STOPPED
            ),  # should not count
            Instance(
                instance_type="type_1", status=Instance.STOPPING
            ),  # should not count
            Instance(instance_type="type_no_longer_exists", status=Instance.REQUESTED),
        ],
    )
    target_cluster_shape = {"type_1": 2, "type_3": 2}
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)


def test_single_resources():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }

    # Request 1 CPU should start a node.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
    )
    reply = scheduler.schedule(request)
    target_cluster_shape = {"type_1": 1}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)

    # Request multiple CPUs should start multiple nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * 3,
    )
    reply = scheduler.schedule(request)
    target_cluster_shape = {"type_1": 3}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)

    # Request resources with already existing nodes should not launch new nodes.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
        current_nodes=[
            NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 1},
                total_resources={"CPU": 1},
            ),
        ],
    )
    reply = scheduler.schedule(request)
    target_cluster_shape = {"type_1": 1}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)

    # Request resources with already existing nodes not sufficient should launch
    # new nodes.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
        current_nodes=[
            NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 0.9},
                total_resources={"CPU": 1},
            ),
        ],
    )
    reply = scheduler.schedule(request)
    target_cluster_shape = {"type_1": 2}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)

    # Request resources with already pending nodes should NOT launch new nodes
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
        current_instances=[
            Instance(
                instance_type="type_1",
                status=Instance.REQUESTED,
                total_resources={"CPU": 1},
            ),
        ],
    )
    reply = scheduler.schedule(request)
    target_cluster_shape = {"type_1": 1}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)


def test_max_worker_num_enforce():
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=10,
        ),
    }
    max_num_worker_nodes = 2

    # Request 10 CPUs should start at most 2 nodes.
    request = sched_request(
        node_type_configs=node_type_configs,
        max_num_worker_nodes=max_num_worker_nodes,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})] * 3,
        current_nodes=[
            NodeState(ray_node_type_name="type_1"),  # head node
        ],
    )
    reply = scheduler.schedule(request)
    target_cluster_shape = {"type_1": 3}
    assert sorted(reply.target_cluster_shape) == sorted(target_cluster_shape)


def test_multi_requests_fittable():
    """
    Test multiple requests can be fit into a single node.
    """
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            resources={"CPU": 1, "GPU": 1},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            resources={"CPU": 3},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1, "GPU": 1}),
        ],
    )
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted({"type_1": 1, "type_2": 1})
    assert reply.infeasible_resource_requests == []

    # Change the ordering of requests should not affect the result.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 1, "GPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
        ],
    )
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted({"type_1": 1, "type_2": 1})
    assert reply.infeasible_resource_requests == []

    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 2}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 0.5, "GPU": 0.5}),
            ResourceRequestUtil.make({"CPU": 0.5, "GPU": 0.5}),
        ],
    )
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted({"type_1": 1, "type_2": 1})
    assert reply.infeasible_resource_requests == []

    # However, if we already have fragmentation. We should not be able
    # to fit more requests.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1}),
            ResourceRequestUtil.make({"CPU": 1, "GPU": 1}),
        ],
        current_nodes=[
            NodeState(
                ray_node_type_name="type_1",
                available_resources={"CPU": 0, "GPU": 1},  # fragmentation
                total_resources={"CPU": 1, "GPU": 1},
            ),
        ],
    )
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted({"type_1": 1, "type_2": 1})
    assert len(reply.infeasible_resource_requests) == 1


def test_multi_node_types_score():
    """
    Test that when multiple node types are possible, choose the best scoring ones:
    1. The number of resources utilized.
    2. The amount of utilization.
    """
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_large": NodeTypeConfig(
            name="type_large",
            resources={"CPU": 10},  # Large machines
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_small": NodeTypeConfig(
            name="type_small",
            resources={"CPU": 5},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_gpu": NodeTypeConfig(
            name="type_gpu",
            resources={"CPU": 2, "GPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }

    # Request 1 CPU should just start the small machine and not the GPU machine
    # since it has more types of resources.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
    )
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted({"type_small": 1})

    # type_small should be preferred over type_large.
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 2})],
    )
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted({"type_small": 1})


def test_multi_node_types_score_with_gpu(monkeypatch):
    """
    Test that when multiple node types are possible, choose the best scoring ones:
    - The GPU scoring.
    """
    scheduler = ResourceDemandScheduler()
    node_type_configs = {
        "type_gpu": NodeTypeConfig(
            name="type_gpu",
            resources={"CPU": 1, "GPU": 2},
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
        "type_multi": NodeTypeConfig(
            name="type_multi",
            resources={"CPU": 2, "XXX": 2},  # Some random resource.
            min_worker_nodes=0,
            max_worker_nodes=1,
        ),
    }
    request = sched_request(
        node_type_configs=node_type_configs,
        resource_requests=[ResourceRequestUtil.make({"CPU": 1})],
    )
    reply = scheduler.schedule(request)
    assert sorted(reply.target_cluster_shape) == sorted({"type_multi": 1})

    with monkeypatch.context() as m:
        m.setattr(ray.autoscaler.v2.scheduler, "AUTOSCALER_CONSERVE_GPU_NODES", 0)
        # type_multi should now be preferred over type_gpu.
        reply = scheduler.schedule(request)
        assert sorted(reply.target_cluster_shape) == sorted({"type_gpu": 1})


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
