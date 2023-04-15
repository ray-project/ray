import pytest
import time
import copy

import ray

from ray import serve
from ray.serve._private.common import DeploymentInfo
from ray.serve.generated.serve_pb2 import DeploymentRoute
from ray.serve.controller import _generate_deployment_config_versions


def test_redeploy_start_time(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = serve.context._global_client._controller

    @serve.deployment
    def test(_):
        return "1"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test"))
    )
    deployment_info_1 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_1 = deployment_info_1.start_time_ms

    time.sleep(0.1)

    @serve.deployment
    def test(_):
        return "2"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test"))
    )
    deployment_info_2 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_2 = deployment_info_2.start_time_ms

    assert start_time_ms_1 == start_time_ms_2


@pytest.mark.parametrize("field_to_update", ["import_path", "runtime_env", "both"])
def test_config_versions_non_deployments_update(field_to_update: str):
    """
    Check that controller._generate_deployment_config_versions() has correct behavior
    when the the ``import_path`` and ``runtime_env`` fields are updated.
    """

    old_config = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [
            {
                "name": "f",
                "num_replicas": 1,
                "ray_actor_options": {"num_cpus": 0.1},
            }
        ],
    }

    new_config = copy.deepcopy(old_config)
    if field_to_update == "import_path":
        new_config["import_path"] = "ray.serve.tests.test_config_files.pid.bnode"
    elif field_to_update == "runtime_env":
        new_config["runtime_env"] = {"env_vars": {"test_var": "test_val"}}
    elif field_to_update == "both":
        new_config["import_path"] = "ray.serve.tests.test_config_files.pid.bnode"
        new_config["runtime_env"] = {"env_vars": {"test_var": "test_val"}}

    versions = {"f": "v1"}
    new_versions = _generate_deployment_config_versions(
        new_config, old_config, versions
    )
    assert new_versions.get("f") is not None and (
        new_versions.get("f") != versions.get("f")
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
