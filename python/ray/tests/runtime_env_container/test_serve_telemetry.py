import argparse
import os
import subprocess

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeDeploySchema
from ray.serve._private.test_utils import (
    TelemetryStorage,
    check_ray_started,
)

parser = argparse.ArgumentParser(
    description="Example Python script taking command line arguments."
)
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
args = parser.parse_args()
worker_pth = "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"  # noqa

os.environ["RAY_USAGE_STATS_ENABLED"] = "1"
os.environ["RAY_USAGE_STATS_REPORT_URL"] = "http://127.0.0.1:8000/telemetry"
os.environ["RAY_USAGE_STATS_REPORT_INTERVAL_S"] = "1"


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

# Start test
config["applications"].append(
    {
        "name": "default",
        "import_path": "serve_application:app",
        "runtime_env": {"container": {"image": args.image, "worker_path": worker_pth}},
    },
)
client.deploy_apps(ServeDeploySchema.parse_obj(config))


# def check_application(expected: str):
#     app_handle = serve.get_app_handle("default")
#     ref = app_handle.remote()
#     assert ref.result() == expected
#     return True


# wait_for_condition(
#     check_application,
#     expected="helloworldalice",
#     timeout=300,
# )


def check_telemetry():
    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert (
        ServeUsageTag.APP_CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report)
        == "1"
    )
    return True


wait_for_condition(check_telemetry)
print("Telemetry check passed!")
