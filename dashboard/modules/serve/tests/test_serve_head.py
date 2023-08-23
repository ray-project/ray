import sys
from typing import Dict

import pytest
import requests

from ray._private.test_utils import wait_for_condition
from ray.serve.config import DeploymentMode
from ray.serve.tests.conftest import *  # noqa: F401 F403
from ray.serve.schema import ServeInstanceDetails
from ray.serve._private.common import ApplicationStatus, DeploymentStatus

GET_OR_PUT_URL_V2 = "http://localhost:52365/api/serve/applications/"


def deploy_config_multi_app(config: Dict):
    put_response = requests.put(GET_OR_PUT_URL_V2, json=config, timeout=30)
    assert put_response.status_code == 200
    print("PUT request sent successfully.")


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_get_serve_instance_details(ray_start_stop):
    """
    This test is a simplified version of `test_get_serve_instance_details`
    in test_serve_agent.py because the behavior in serve_head just proxies
    to the serve_agent endpoint.
    """

    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    fastapi_import_path = "ray.serve.tests.test_config_files.fastapi_deployment.node"
    config1 = {
        "proxy_location": "HeadOnly",
        "http_options": {
            "host": "127.0.0.1",
            "port": 8005,
        },
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": world_import_path,
                "deployments": [
                    {
                        "name": "f",
                        "ray_actor_options": {"num_cpus": 0.2},
                    },
                ],
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": fastapi_import_path,
            },
        ],
    }

    deploy_config_multi_app(config1)

    def applications_running():
        response = requests.get(GET_OR_PUT_URL_V2, timeout=15)
        assert response.status_code == 200

        serve_details = ServeInstanceDetails(**response.json())
        return (
            serve_details.applications["app1"].status == ApplicationStatus.RUNNING
            and serve_details.applications["app2"].status == ApplicationStatus.RUNNING
        )

    wait_for_condition(applications_running, timeout=15)
    print("All applications are in a RUNNING state.")

    serve_details = ServeInstanceDetails(
        **requests.get("http://localhost:8265/api/serve_head/applications/").json()
    )
    # CHECK: host and port
    assert serve_details.http_options.host == "127.0.0.1"
    assert serve_details.http_options.port == 8005
    assert serve_details.proxy_location == DeploymentMode.HeadOnly
    print('Confirmed fetched host and port metadata are "127.0.0.1" and "8000".')

    app_details = serve_details.applications

    # CHECK: app configs are equal
    assert (
        app_details["app1"].deployed_app_config.dict(exclude_unset=True)
        == config1["applications"][0]
    )
    assert (
        app_details["app2"].deployed_app_config.dict(exclude_unset=True)
        == config1["applications"][1]
    )
    print("Confirmed the deployed app configs from the fetched metadata is correct.")

    # CHECK: deployment timestamp
    assert app_details["app1"].last_deployed_time_s > 0
    assert app_details["app2"].last_deployed_time_s > 0
    print("Confirmed deployment timestamps are nonzero.")

    # CHECK: docs path
    assert app_details["app1"].docs_path is None
    assert app_details["app2"].docs_path == "/my_docs"
    print("Confirmed docs paths are correct.")

    # CHECK: all deployments are present
    assert app_details["app1"].deployments.keys() == {"f", "BasicDriver"}
    assert app_details["app2"].deployments.keys() == {"FastAPIDeployment"}
    print("Metadata for all deployed deployments are present.")

    # CHECK: application details
    for app in ["app1", "app2"]:
        assert app_details[app].route_prefix == f"/{app}"
        for dep_details in app_details[app].deployments.values():
            assert dep_details.status == DeploymentStatus.HEALTHY

            # Route prefix should be app level options eventually
            assert "route_prefix" not in dep_details.deployment_config.dict(
                exclude_unset=True
            )
    print("Finished checking application details.")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
