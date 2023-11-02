# coding: utf-8
import copy
import os
import sys
from unittest import mock

import pytest

from google.protobuf.json_format import ParseDict
from ray.autoscaler.v2.instance_manager.instance_manager import (
    SimpleInstanceManager,
)  # noqa

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    UpdateInstanceManagerStateRequest,
)


class DummySubscriber(InstanceUpdatedSubscriber):
    def __init__(self):
        self.events = []

    def notify(self, events):
        self.events.extend(events)


def test_update_instance_states():
    subscriber = DummySubscriber()
    instance_configs = {
        "type_1": {
            "resources": {"CPU": 1},
        }
    }

    manager = SimpleInstanceManager(
        cluster_id="test_cluster",
        storage=InMemoryStorage(),
        instance_configs=instance_configs,
        status_change_subscribers=[subscriber],
    )

    # Start with no instances
    reply = manager.get_instance_manager_state()
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

    assert reply.success
    assert reply.state.version == 1
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
                }
            ],
        },
        UpdateInstanceManagerStateRequest(),
    )

    reply = manager.update_instance_manager_state(request)

    assert reply.success
    assert reply.state.version == 2
    assert len(reply.state.instances) == 1
    assert reply.state.instances[0].ray_status == Instance.RAY_RUNNING


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
