import argparse
import os
import subprocess

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.test_utils import (
    TelemetryStorage,
    check_ray_started,
    check_ray_stopped,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeDeploySchema

parser = argparse.ArgumentParser(
    description="Example Python script taking command line arguments."
)
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
args = parser.parse_args()

os.environ["RAY_USAGE_STATS_ENABLED"] = "1"
os.environ["RAY_USAGE_STATS_REPORT_URL"] = "http://127.0.0.1:8000/telemetry"
os.environ["RAY_USAGE_STATS_REPORT_INTERVAL_S"] = "1"


if args.use_image_uri_api:
    runtime_env = {"image_uri": args.image}
else:
    runtime_env = {"container": {"image": args.image}}


def check_app(app_name: str, expected: str):
    app_handle = serve.get_app_handle(app_name)
    ref = app_handle.remote()
    assert ref.result() == expected
    return True


def check_telemetry_app():
    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert (
        ServeUsageTag.APP_CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report)
        == "1"
    )
    return True


def check_telemetry_deployment():
    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert (
        ServeUsageTag.DEPLOYMENT_CONTAINER_RUNTIME_ENV_USED.get_value_from_report(
            report
        )
        == "1"
    )
    return True


subprocess.check_output(["ray", "start", "--head"])
wait_for_condition(check_ray_started, timeout=5)
serve.start()

# Start TelemetryStorage and perform initial checks
storage_handle = TelemetryStorage.remote()
client = _get_global_client()
config = {
    "applications": [
        {
            "name": "telemetry",
            "route_prefix": "/telemetry",
            "import_path": "ray.serve._private.test_utils.receiver_app",
        },
    ],
}
client.deploy_apps(ServeDeploySchema.parse_obj(config))
wait_for_condition(
    lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
)
report = ray.get(storage_handle.get_report.remote())
assert (
    ServeUsageTag.APP_CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report) is None
)
assert (
    ServeUsageTag.DEPLOYMENT_CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report)
    is None
)

# Deploy with container runtime env set at application level
config["applications"].append(
    {
        "name": "app1",
        "import_path": "serve_application:app",
        "route_prefix": "/app1",
        "runtime_env": runtime_env,
    },
)
client.deploy_apps(ServeDeploySchema.parse_obj(config))
wait_for_condition(check_app, app_name="app1", expected="helloworldalice", timeout=300)
wait_for_condition(check_telemetry_app)

deployment_runtime_env = dict(runtime_env)
deployment_runtime_env["working_dir"] = None
# Deploy with container runtime env set at deployment level
config["applications"].append(
    {
        "name": "app2",
        "import_path": "read_file:app",
        "route_prefix": "/app2",
        "runtime_env": {
            "working_dir": "https://github.com/ray-project/test_dag/archive/4d2c9a59d9eabfd4c8a9e04a7aae44fc8f5b416f.zip"  # noqa
        },
        "deployments": [
            {
                "name": "Model",
                "ray_actor_options": {
                    "runtime_env": deployment_runtime_env,
                },
            }
        ],
    }
)
client.deploy_apps(ServeDeploySchema.parse_obj(config))
wait_for_condition(check_app, app_name="app2", expected="helloworldalice", timeout=300)
wait_for_condition(check_telemetry_deployment)
print("Telemetry checks passed!")

# Stop ray
subprocess.check_output(["ray", "stop", "--force"])
wait_for_condition(check_ray_stopped, timeout=15)
