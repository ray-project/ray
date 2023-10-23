from collections import defaultdict
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
import json
from unittest.mock import MagicMock

import pytest
from ray.autoscaler.v2.sdk import AutoscalerV2
from ray.autoscaler.v2.tests.util import (
    make_cluster_resource_state,
    make_ray_node,
    make_instance,
)
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceState,
    DrainNodeReason,
    NodeStatus,
    RayNodeKind,
)
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceManagerState,
    GetInstanceManagerStateReply,
)

#######################################################################
# Integration test for the autoscaler with resource scheduler and
# the instance manager.
#######################################################################
def test_scaling_up():
    instance_manager = MagicMock()

    # Min worker upscaling
    autoscaler = AutoscalerV2(
        config={
            "idle_timeout_minutes": "1",
            "available_node_types": {
                "type-1": {
                    "resources": {"CPU": 2},
                    "node_config": {},
                    "min_workers": 2,
                    "max_workers": 20,
                },
            },
        },
        instance_manager=instance_manager,
        gcs_client=MagicMock(),
    )

    autoscaler.get_autoscaling_state(
        cluster_resource_state=make_cluster_resource_state([])
    )

    im_update = instance_manager.update_instance_manager_state.call_args.kwargs[
        "request"
    ]
    print(im_update)
    assert len(im_update.launch_requests) == 1
    assert im_update.launch_requests[0].instance_type == "type-1"
    assert im_update.launch_requests[0].count == 2

    # Simple resource requests
    autoscaler.get_autoscaling_state(
        cluster_resource_state=make_cluster_resource_state(
            resource_requests=[{"CPU": 2}] * 4
        )
    )
    im_update = instance_manager.update_instance_manager_state.call_args.kwargs[
        "request"
    ]
    print(im_update)
    assert len(im_update.launch_requests) == 1
    assert im_update.launch_requests[0].instance_type == "type-1"
    assert im_update.launch_requests[0].count == 4


def test_downscaling_max_workers():
    instance_manager = MagicMock()
    gcs_client = MagicMock()
    idle_timeout_minutes = 1
    autoscaler = AutoscalerV2(
        config={
            "idle_timeout_minutes": idle_timeout_minutes,
            "available_node_types": {
                "type-1": {
                    "resources": {"CPU": 2},
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 1,
                },
            },
        },
        instance_manager=instance_manager,
        gcs_client=gcs_client,
    )

    # Max workers enforced.
    autoscaler.get_autoscaling_state(
        cluster_resource_state=make_cluster_resource_state(
            node_states=[
                make_ray_node(
                    "type-1",
                    "node-1",
                    {"CPU": 2},
                    {"CPU": 1},
                    NodeStatus.RUNNING,
                    RayNodeKind.HEAD,
                ),
                make_ray_node(
                    "type-1",
                    "node-2",
                    {"CPU": 2},
                    {"CPU": 1},
                    NodeStatus.RUNNING,
                    RayNodeKind.WORKER,
                ),
                make_ray_node(
                    "type-1",
                    "node-3",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.IDLE,
                    RayNodeKind.WORKER,
                    idle_duration_ms=1,
                ),
            ],
        )
    )

    im_update = instance_manager.update_instance_manager_state.call_args.kwargs[
        "request"
    ]
    print(im_update)
    assert len(im_update.updates) == 1
    assert im_update.updates[0].instance_id == "node-3"
    assert im_update.updates[0].new_ray_status == Instance.RAY_STOPPING

    drain_node_args = gcs_client.drain_node.call_args.kwargs
    assert drain_node_args["node_id"] == b"node-3"
    assert drain_node_args["reason"] == DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION


def test_downscaling_idle_nodes():
    """
    Testing idle termination:
        1. Head node should not be terminated
        2. Idle node only terminated if idle for more than idle_timeout_minutes
        3. Idle node should not be terminated if min workers would be violated.
    """
    # Idle nodes.
    instance_manager = MagicMock()
    gcs_client = MagicMock()
    idle_timeout_minutes = 1
    autoscaler = AutoscalerV2(
        config={
            "idle_timeout_minutes": idle_timeout_minutes,
            "available_node_types": {
                "type-1": {
                    "resources": {"CPU": 2},
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 10,
                },
            },
        },
        instance_manager=instance_manager,
        gcs_client=gcs_client,
    )

    autoscaler.get_autoscaling_state(
        cluster_resource_state=make_cluster_resource_state(
            node_states=[
                make_ray_node(
                    "type-1",
                    "node-1",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.IDLE,
                    RayNodeKind.HEAD,
                    idle_duration_ms=1000 * 60 * 99,
                ),
                make_ray_node(
                    "type-1",
                    "node-2",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.IDLE,
                    RayNodeKind.WORKER,
                    idle_duration_ms=1000 * 60 * 99,
                ),
                make_ray_node(
                    "type-1",
                    "node-3",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.IDLE,
                    RayNodeKind.WORKER,
                    idle_duration_ms=idle_timeout_minutes * 60_000 - 1,  # < 60 seconds
                ),
            ],
        )
    )
    im_update = instance_manager.update_instance_manager_state.call_args.kwargs[
        "request"
    ]
    drain_node_args_list = gcs_client.drain_node.call_args_list
    print(im_update)
    print(drain_node_args_list)

    assert len(im_update.updates) == 1
    assert im_update.updates[0].instance_id == "node-2"
    assert im_update.updates[0].new_ray_status == Instance.RAY_STOPPING
    assert len(drain_node_args_list) == 1
    drain_node_args = drain_node_args_list[0].kwargs
    assert drain_node_args["node_id"] == b"node-2"
    assert (
        drain_node_args["reason"] == DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION
    )


def test_stop_ray_dead_nodes():
    """
    Test that dead ray nodes should be stopped if not already so.
    """
    instance_manager = MagicMock()
    gcs_client = MagicMock()
    idle_timeout_minutes = 1
    autoscaler = AutoscalerV2(
        config={
            "idle_timeout_minutes": idle_timeout_minutes,
            "available_node_types": {
                "type-1": {
                    "resources": {"CPU": 2},
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 10,
                },
            },
        },
        instance_manager=instance_manager,
        gcs_client=gcs_client,
    )

    # Mock the IM to return:
    #   node_1: a running ray instance
    #   node_2: a stopping ray instance (e.g. idle terminated)
    #   node_3: a running ray instance
    #   node_4: a dead instance

    instance_manager.get_instance_manager_state.return_value = (
        GetInstanceManagerStateReply(
            state=InstanceManagerState(
                version=1,
                instances=[
                    make_instance(
                        instance_id="node-1",
                        status=Instance.ALLOCATED,
                        ray_status=Instance.RAY_RUNNING,
                    ),
                    make_instance(
                        instance_id="node-2",
                        status=Instance.ALLOCATED,
                        ray_status=Instance.RAY_STOPPING,
                    ),
                    make_instance(
                        instance_id="node-3",
                        status=Instance.ALLOCATED,
                        ray_status=Instance.RAY_RUNNING,
                    ),
                    make_instance(
                        instance_id="node-4",
                        status=Instance.ALLOCATED,
                        ray_status=Instance.RAY_STOPPED,
                    ),
                ],
            )
        )
    )

    autoscaler.get_autoscaling_state(
        cluster_resource_state=make_cluster_resource_state(
            node_states=[
                make_ray_node(
                    "type-1",
                    "node-1",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.IDLE,
                    RayNodeKind.HEAD,
                    idle_duration_ms=1000 * 60 * 99,
                ),
                make_ray_node(
                    "type-1",
                    "node-2",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.DEAD,
                    RayNodeKind.WORKER,
                ),
                make_ray_node(
                    "type-1",
                    "node-3",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.DEAD,
                    RayNodeKind.WORKER,
                ),
                make_ray_node(
                    "type-1",
                    "node-4",
                    {"CPU": 2},
                    {"CPU": 2},
                    NodeStatus.DEAD,
                    RayNodeKind.WORKER,
                ),
            ],
        )
    )

    im_update = instance_manager.update_instance_manager_state.call_args.kwargs[
        "request"
    ]
    print(im_update)
    assert len(im_update.updates) == 2
    # node 2 and node 3 should be marked as ray_stopped.
    assert {"node-2", "node-3"} == {update.instance_id for update in im_update.updates}
    assert {Instance.RAY_STOPPED} == {update.new_ray_status for update in im_update.updates}




if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
