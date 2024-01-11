import os
import sys

import pytest

from mock import MagicMock

from ray.autoscaler.v2.autoscaler import DefaultAutoscaler
from ray.autoscaler.v2.instance_manager.config import IConfigReader, NodeTypeConfig
from ray.autoscaler.v2.tests.util import create_instance
import unittest

# coding: utf-8
from typing import Dict, Set
from unittest.mock import patch, call
from mock import MagicMock

import pytest
from ray import available_resources
from ray.autoscaler.v2.tests.util import create_instance
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig, IConfigReader

from ray.autoscaler.v2.autoscaler import DefaultAutoscaler

from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceState,
    NodeState,
    NodeStatus,
)
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateReply,
    Instance,
    InstanceManagerState,
    Status,
    StatusCode,
    UpdateInstanceManagerStateReply,
)

# coding: utf-8


class MockConfigReader(IConfigReader):
    def __init__(self, mock_config):
        self._mock_config = mock_config

    def get_autoscaling_config(self):
        return self._mock_config


def test_ray_updates_new_nodes():
    mock_config = MagicMock()
    mock_config.get_node_type_configs.return_value = {
        "type_1": NodeTypeConfig(
            name="type_1",
            max_worker_nodes=10,
            min_worker_nodes=0,
            resources={"CPU": 1},
        )
    }
    mock_config.get_max_num_worker_nodes.return_value = 100
    mock_config.get_idle_terminate_threshold_s.return_value = 1000

    mock_config_reader = MockConfigReader(mock_config)
    mock_im = MagicMock()
    mock_scheduler = MagicMock()
    mock_gcs_client = MagicMock()

    autoscaler = DefaultAutoscaler(
        config_reader=mock_config_reader,
        instance_manager=mock_im,
        scheduler=mock_scheduler,
        gcs_client=mock_gcs_client,
    )

    mock_im.get_instance_manager_state.return_value = GetInstanceManagerStateReply(
        status=Status(code=StatusCode.OK),
        state=InstanceManagerState(
            version=2,
            instances=[
                # Should be updated to running.
                create_instance(
                    instance_id="i-1",
                    status=Instance.ALLOCATED,
                    instance_type="type_1",
                    cloud_instance_id="c-1",
                ),
                # Should be updated to running.
                create_instance(
                    instance_id="i-2",
                    status=Instance.RAY_INSTALLING,
                    instance_type="type_1",
                    cloud_instance_id="c-2",
                ),
                # Shouldn't be updated no matching ray node.
                create_instance(
                    instance_id="i-3",
                    status=Instance.ALLOCATED,
                    instance_type="type_1",
                    cloud_instance_id="c-3",
                ),
                # Shouldn't be updated no matching ray node.
                create_instance(
                    instance_id="i-4",
                    status=Instance.QUEUED,
                    instance_type="type_1",
                ),
            ],
        ),
    )
    mock_im.update_instance_manager_state.return_value = (
        UpdateInstanceManagerStateReply(
            status=Status(code=StatusCode.OK),
        )
    )

    autoscaler.get_autoscaling_state(
        cluster_resource_state=ClusterResourceState(
            node_states=[
                NodeState(
                    node_id=b"11111",
                    instance_id="c-1",
                    status=NodeStatus.RUNNING,  # A new ray node.
                    ray_node_type_name="type_1",
                ),
                NodeState(
                    node_id=b"22222",
                    instance_id="c-2",
                    status=NodeStatus.IDLE,  # A new ray node.
                    ray_node_type_name="type_1",
                ),
                NodeState(
                    node_id=b"33333",
                    # Non-existent cloud instance undiscovered by autoscaler yet
                    instance_id="c-new",
                    status=NodeStatus.RUNNING,
                    ray_node_type_name="type_1",
                ),
            ]
        )
    )

    assert mock_im.update_instance_manager_state.call_count == 1
    request = mock_im.update_instance_manager_state.call_args_list[0].kwargs["request"]
    assert len(request.updates) == 2
    instance_ids = {update.instance_id for update in request.updates}
    status = {update.new_instance_status for update in request.updates}
    assert instance_ids == {"i-1", "i-2"}
    assert status == {Instance.RAY_RUNNING}


def test_ray_updates_dead_nodes():
    mock_config = MagicMock()
    mock_config.get_node_type_configs.return_value = {
        "type_1": NodeTypeConfig(
            name="type_1",
            max_worker_nodes=10,
            min_worker_nodes=0,
            resources={"CPU": 1},
        )
    }
    mock_config.get_max_num_worker_nodes.return_value = 100
    mock_config.get_idle_terminate_threshold_s.return_value = 1000

    mock_config_reader = MockConfigReader(mock_config)
    mock_im = MagicMock()
    mock_scheduler = MagicMock()
    mock_gcs_client = MagicMock()

    autoscaler = DefaultAutoscaler(
        config_reader=mock_config_reader,
        instance_manager=mock_im,
        scheduler=mock_scheduler,
        gcs_client=mock_gcs_client,
    )

    mock_im.get_instance_manager_state.return_value = GetInstanceManagerStateReply(
        status=Status(code=StatusCode.OK),
        state=InstanceManagerState(
            version=2,
            instances=[
                # Should be updated to running.
                create_instance(
                    instance_id="i-1",
                    status=Instance.RAY_RUNNING,
                    instance_type="type_1",
                    cloud_instance_id="c-1",
                ),
                # Should be updated to running.
                create_instance(
                    instance_id="i-2",
                    status=Instance.RAY_STOPPING,
                    instance_type="type_1",
                    cloud_instance_id="c-2",
                ),
                # Shouldn't be updated no matching ray node.
                create_instance(
                    instance_id="i-3",
                    status=Instance.RAY_STOPPED,
                    instance_type="type_1",
                    cloud_instance_id="c-3",
                ),
                create_instance(
                    instance_id="i-4",
                    status=Instance.STOPPING,
                    instance_type="type_1",
                    cloud_instance_id="c-4",
                ),
                create_instance(
                    instance_id="i-5",
                    status=Instance.QUEUED,
                    instance_type="type_1",
                ),
            ],
        ),
    )
    mock_im.update_instance_manager_state.return_value = (
        UpdateInstanceManagerStateReply(
            status=Status(code=StatusCode.OK),
        )
    )

    autoscaler.get_autoscaling_state(
        cluster_resource_state=ClusterResourceState(
            node_states=[
                NodeState(
                    node_id=b"11111",
                    instance_id="c-1",
                    status=NodeStatus.DEAD,  # A dead ray node.
                    ray_node_type_name="type_1",
                ),
                NodeState(
                    node_id=b"22222",
                    instance_id="c-2",
                    status=NodeStatus.DEAD,  # A dead ray node.
                    ray_node_type_name="type_1",
                ),
                NodeState(
                    node_id=b"33333",
                    instance_id="c-3",
                    status=NodeStatus.DEAD,  # An already stopped ray node.
                    ray_node_type_name="type_1",
                ),
                NodeState(
                    node_id=b"33333",
                    instance_id="c-4",
                    status=NodeStatus.RUNNING,  # A still running node.
                    ray_node_type_name="type_1",
                ),
            ]
        )
    )

    assert mock_im.update_instance_manager_state.call_count == 1
    request = mock_im.update_instance_manager_state.call_args_list[0].kwargs["request"]
    print(request)
    # i-1, c-1 is a newly dead ray instance
    # i-2, c-2 is a newly dead ray instance
    # i-3, c-3 is an already dead ray instance => no update
    # i-4, c-4 is still running ray (late reporting) but actually being preempted
    # (could be due to node provider) => no update
    assert len(request.updates) == 2
    instance_ids = set([update.instance_id for update in request.updates])
    status = set([update.new_instance_status for update in request.updates])
    assert instance_ids == {"i-1", "i-2"}
    assert status == {Instance.RAY_STOPPED}


def test_im_state_pass_through():
    mock_config = MagicMock()
    mock_config.get_node_type_configs.return_value = {
        "type_1": NodeTypeConfig(
            name="type_1",
            max_worker_nodes=10,
            min_worker_nodes=0,
            resources={"CPU": 1},
        )
    }
    mock_config.get_max_num_worker_nodes.return_value = 100
    mock_config.get_idle_terminate_threshold_s.return_value = 1000

    mock_config_reader = MockConfigReader(mock_config)
    mock_im = MagicMock()
    mock_scheduler = MagicMock()
    mock_gcs_client = MagicMock()

    autoscaler = DefaultAutoscaler(
        config_reader=mock_config_reader,
        instance_manager=mock_im,
        scheduler=mock_scheduler,
        gcs_client=mock_gcs_client,
    )

    mock_im.get_instance_manager_state.return_value = GetInstanceManagerStateReply(
        status=Status(code=StatusCode.OK),
        state=InstanceManagerState(
            version=2,
            instances=[
                create_instance(
                    instance_id="i-1",
                    status=Instance.RAY_RUNNING,
                    instance_type="type_1",
                ),
                create_instance(
                    instance_id="i-1",
                    status=Instance.ALLOCATED,
                    instance_type="type_1",
                ),
                create_instance(
                    instance_id="i-2",
                    status=Instance.QUEUED,
                    instance_type="type_1",
                ),
                create_instance(
                    instance_id="i-3",
                    status=Instance.ALLOCATION_FAILED,
                    instance_type="type_1",
                    status_history=[
                        Instance.StatusHistory(
                            instance_status=Instance.QUEUED,
                            timestamp_ns=1,
                        ),
                        Instance.StatusHistory(
                            instance_status=Instance.ALLOCATION_FAILED,
                            timestamp_ns=2,
                        ),
                    ],
                ),
            ],
        ),
    )
    mock_im.update_instance_manager_state.return_value = (
        UpdateInstanceManagerStateReply(
            status=Status(code=StatusCode.OK),
        )
    )

    state = autoscaler.get_autoscaling_state(
        cluster_resource_state=ClusterResourceState(cluster_resource_state_version=10)
    )

    assert state.last_seen_cluster_resource_state_version == 10
    assert state.autoscaler_state_version == 2
    assert len(state.pending_instances) == 1
    assert len(state.pending_instance_requests) == 1
    assert len(state.failed_instance_requests) == 1


def test_static_cluster():
    mock_config = MagicMock()
    mock_config.get_max_num_worker_nodes.return_value = 100

    mock_config_reader = MockConfigReader(mock_config)
    mock_im = MagicMock()
    mock_scheduler = MagicMock()
    mock_gcs_client = MagicMock()

    autoscaler = DefaultAutoscaler(
        config_reader=mock_config_reader,
        instance_manager=mock_im,
        scheduler=mock_scheduler,
        gcs_client=mock_gcs_client,
    )

    # Dummy reply
    mock_im.get_instance_manager_state.return_value = GetInstanceManagerStateReply(
        status=Status(code=StatusCode.OK),
        state=InstanceManagerState(version=1, instances=[]),
    )
    mock_im.update_instance_manager_state.return_value = (
        UpdateInstanceManagerStateReply(
            status=Status(code=StatusCode.OK),
        )
    )

    state = autoscaler.get_autoscaling_state(
        cluster_resource_state=ClusterResourceState(
            node_states=[
                NodeState(
                    node_id=b"11111",
                    instance_id="i-11111",
                    ray_node_type_name="head",
                    available_resources={"CPU": 1},
                    total_resources={"CPU": 1},
                    status=NodeStatus.IDLE,
                )
            ]
        )
    )

    assert state is not None


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
