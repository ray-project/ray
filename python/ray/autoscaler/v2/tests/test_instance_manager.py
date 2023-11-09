# coding: utf-8
import os
import sys
from typing import Any, Dict

import pytest
from google.protobuf.json_format import ParseDict

from ray.autoscaler.v2.instance_manager.config import InstancesConfigReader
from ray.autoscaler.v2.instance_manager.instance_manager import (  # noqa
    InMemoryInstanceManager,
)
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateRequest,
    Instance,
    InstancesConfig,
    StatusCode,
    UpdateInstanceManagerStateRequest,
)


class DummyConfigReader(InstancesConfigReader):
    def __init__(self, config_dict: Dict[str, Any]):
        self._config = ParseDict(config_dict, InstancesConfig())

    def get_instances_config(self) -> InstancesConfig:
        return self._config


class DummySubscriber(InstanceUpdatedSubscriber):
    def __init__(self):
        self.events = []

    def notify(self, events):
        self.events.extend(events)


def test_update_instance_states():
    subscriber = DummySubscriber()
    config_reader = DummyConfigReader(
        {
            "available_node_type_configs": {
                "type_1": {
                    "resources": {"CPU": 1},
                }
            },
        }
    )
    ins_storage = InstanceStorage(
        cluster_id="test-cluster",
        storage=InMemoryStorage(),
    )
    manager = InMemoryInstanceManager(
        instance_storage=ins_storage,
        instances_config_reader=config_reader,
        status_change_subscribers=[subscriber],
    )

    # Start with no instances
    reply = manager.get_instance_manager_state(GetInstanceManagerStateRequest())
    assert reply.state.version == 0
    assert len(reply.state.instances) == 0

    # Add an instance
    request = ParseDict(
        {
            "expected_version": 0,
            "launch_requests": [{"instance_type": "type_1", "count": 1, "id": "id1"}],
            "updates": [],
        },
        UpdateInstanceManagerStateRequest(),
    )

    reply = manager.update_instance_manager_state(request)

    assert reply.status.code == StatusCode.OK
    version = reply.version

    # Get the instances.
    reply = manager.get_instance_manager_state(GetInstanceManagerStateRequest())
    assert reply.state.version == version
    assert len(reply.state.instances) == 1
    assert reply.state.instances[0].instance_type == "type_1"
    assert reply.state.instances[0].status == Instance.UNKNOWN
    assert reply.state.instances[0].ray_status == Instance.RAY_STATUS_UNKNOWN

    # Update the instance
    request = ParseDict(
        {
            "expected_version": 1,
            "launch_requests": [],
            "updates": [
                {
                    "instance_id": reply.state.instances[0].instance_id,
                    "new_ray_status": Instance.RAY_RUNNING,
                    "new_instance_status": Instance.ALLOCATED,
                }
            ],
        },
        UpdateInstanceManagerStateRequest(),
    )

    reply = manager.update_instance_manager_state(request)

    assert reply.status.code == StatusCode.OK
    version = reply.version

    # Get the instances.
    reply = manager.get_instance_manager_state(GetInstanceManagerStateRequest())
    assert reply.state.version == version
    assert len(reply.state.instances) == 1
    assert reply.state.instances[0].ray_status == Instance.RAY_RUNNING
    assert reply.state.instances[0].status == Instance.ALLOCATED


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
