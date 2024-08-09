import sys
from typing import Any

import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.actor import ActorHandle
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.test_utils import check_num_alive_nodes
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeDeploySchema


@serve.deployment
class Noop:
    pass


app_A = Noop.bind()


def check_telemetry(tag: ServeUsageTag, storage_handle: ActorHandle, expected: Any):
    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert tag.get_value_from_report(report) is expected
    return True


@pytest.mark.skipif(
    not RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Need compact strategy."
)
def test_node_compactions(autoscaling_cluster_with_telemetry):
    storage_handle = autoscaling_cluster_with_telemetry
    client = _get_global_client()

    check_telemetry(ServeUsageTag.NUM_NODE_COMPACTIONS, storage_handle, expected=None)
    import_path = "ray.anyscale.serve.tests.test_telemetry.app_A"
    config = {
        "applications": [
            {
                "name": "A",
                "import_path": import_path,
                "route_prefix": "/a",
                "deployments": [{"name": "Noop"}],
            },
            {
                "name": "B",
                "import_path": import_path,
                "route_prefix": "/b",
                "deployments": [{"name": "Noop", "ray_actor_options": {"num_cpus": 2}}],
            },
        ]
    }

    client.deploy_apps(ServeDeploySchema(**config))
    client._wait_for_application_running("A")
    client._wait_for_application_running("B")
    wait_for_condition(check_num_alive_nodes, target=2)  # 1 head + 1 worker node

    config["applications"][0]["deployments"][0]["num_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**config))
    client._wait_for_application_running("A")
    wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker node

    # Delete app 'B'. One node should be compacted.
    del config["applications"][1]
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(check_num_alive_nodes, target=2)  # 1 head + 1 worker node

    wait_for_condition(
        check_telemetry,
        tag=ServeUsageTag.NUM_NODE_COMPACTIONS,
        storage_handle=storage_handle,
        expected="1",
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
