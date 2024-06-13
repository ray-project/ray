import json
import time

import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.constants import (
    DEFAULT_AUTOSCALING_POLICY,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve.autoscaling_policy import default_autoscaling_policy
from ray.serve.context import _get_global_client
from ray.serve.generated.serve_pb2 import DeploymentRoute
from ray.serve.schema import ServeDeploySchema
from ray.serve.tests.conftest import TEST_GRPC_SERVICER_FUNCTIONS


def test_redeploy_start_time(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = _get_global_client()._controller

    @serve.deployment
    def test(_):
        return "1"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test", SERVE_DEFAULT_APP_NAME))
    )
    deployment_info_1 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_1 = deployment_info_1.start_time_ms

    time.sleep(0.1)

    @serve.deployment
    def test(_):
        return "2"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test", SERVE_DEFAULT_APP_NAME))
    )
    deployment_info_2 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_2 = deployment_info_2.start_time_ms

    assert start_time_ms_1 == start_time_ms_2


def test_deploy_app_custom_exception(serve_instance):
    """Check that controller doesn't deserialize an exception from deploy_app."""

    controller = _get_global_client()._controller

    config = {
        "applications": [
            {
                "name": "broken_app",
                "route_prefix": "/broken",
                "import_path": "ray.serve.tests.test_config_files.broken_app:app",
            }
        ]
    }

    ray.get(controller.apply_config.remote(config=ServeDeploySchema.parse_obj(config)))

    def check_custom_exception() -> bool:
        status = serve.status().applications["broken_app"]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        assert "custom exception info" in status.message
        return True

    wait_for_condition(check_custom_exception, timeout=10)


@pytest.mark.parametrize(
    "policy", [None, DEFAULT_AUTOSCALING_POLICY, default_autoscaling_policy]
)
def test_get_serve_instance_details_json_serializable(serve_instance, policy):
    """Test the result from get_serve_instance_details is json serializable."""

    controller = _get_global_client()._controller

    autoscaling_config = {
        "min_replicas": 1,
        "max_replicas": 10,
        "_policy": policy,
    }
    if policy is None:
        autoscaling_config.pop("_policy")

    @serve.deployment(autoscaling_config=autoscaling_config)
    def autoscaling_app():
        return "1"

    serve.run(autoscaling_app.bind())
    details = ray.get(controller.get_serve_instance_details.remote())
    details_json = json.dumps(details)
    controller_details = ray.get(controller.get_actor_details.remote())
    node_id = controller_details.node_id
    node_ip = controller_details.node_ip
    proxy_details = ray.get(controller.get_proxy_details.remote(node_id=node_id))
    deployment_timestamp = ray.get(
        controller.get_deployment_timestamps.remote(app_name="default")
    )
    deployment_details = ray.get(
        controller.get_deployment_details.remote("default", "autoscaling_app")
    )
    replica = deployment_details.replicas[0]

    expected_json = json.dumps(
        {
            "controller_info": {
                "node_id": node_id,
                "node_ip": node_ip,
                "actor_id": controller_details.actor_id,
                "actor_name": controller_details.actor_name,
                "worker_id": controller_details.worker_id,
                "log_file_path": controller_details.log_file_path,
            },
            "proxy_location": "HeadOnly",
            "http_options": {"host": "0.0.0.0"},
            "grpc_options": {
                "port": 9000,
                "grpc_servicer_functions": TEST_GRPC_SERVICER_FUNCTIONS,
            },
            "proxies": {
                node_id: {
                    "node_id": node_id,
                    "node_ip": node_ip,
                    "actor_id": proxy_details.actor_id,
                    "actor_name": proxy_details.actor_name,
                    "worker_id": proxy_details.worker_id,
                    "log_file_path": proxy_details.log_file_path,
                    "status": proxy_details.status,
                }
            },
            "applications": {
                "default": {
                    "name": "default",
                    "route_prefix": "/",
                    "docs_path": None,
                    "status": "RUNNING",
                    "message": "",
                    "last_deployed_time_s": deployment_timestamp,
                    "deployed_app_config": None,
                    "deployments": {
                        "autoscaling_app": {
                            "name": "autoscaling_app",
                            "status": "HEALTHY",
                            "status_trigger": "CONFIG_UPDATE_COMPLETED",
                            "message": "",
                            "deployment_config": {
                                "name": "autoscaling_app",
                                "max_concurrent_queries": 100,
                                "max_ongoing_requests": 100,
                                "max_queued_requests": -1,
                                "user_config": None,
                                "autoscaling_config": {
                                    "min_replicas": 1,
                                    "initial_replicas": None,
                                    "max_replicas": 10,
                                    "target_num_ongoing_requests_per_replica": 1.0,
                                    "target_ongoing_requests": None,
                                    "metrics_interval_s": 10.0,
                                    "look_back_period_s": 30.0,
                                    "smoothing_factor": 1.0,
                                    "upscale_smoothing_factor": None,
                                    "downscale_smoothing_factor": None,
                                    "upscaling_factor": None,
                                    "downscaling_factor": None,
                                    "downscale_delay_s": 600.0,
                                    "upscale_delay_s": 30.0,
                                },
                                "graceful_shutdown_wait_loop_s": 2.0,
                                "graceful_shutdown_timeout_s": 20.0,
                                "health_check_period_s": 10.0,
                                "health_check_timeout_s": 30.0,
                                "ray_actor_options": {
                                    "runtime_env": {},
                                    "num_cpus": 1.0,
                                },
                            },
                            "target_num_replicas": 1,
                            "replicas": [
                                {
                                    "node_id": node_id,
                                    "node_ip": node_ip,
                                    "actor_id": replica.actor_id,
                                    "actor_name": replica.actor_name,
                                    "worker_id": replica.worker_id,
                                    "log_file_path": replica.log_file_path,
                                    "replica_id": replica.replica_id,
                                    "state": "RUNNING",
                                    "pid": replica.pid,
                                    "start_time_s": replica.start_time_s,
                                }
                            ],
                        }
                    },
                }
            },
            "target_capacity": None,
        }
    )
    assert details_json == expected_json

    # ensure internal field, serialized_policy_def, is not exposed
    application = details["applications"]["default"]
    deployment = application["deployments"]["autoscaling_app"]
    autoscaling_config = deployment["deployment_config"]["autoscaling_config"]
    assert "_serialized_policy_def" not in autoscaling_config


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
