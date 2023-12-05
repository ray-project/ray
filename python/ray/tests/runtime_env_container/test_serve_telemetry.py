import os
import requests
import subprocess

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from typing import Dict
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeDeploySchema

CONTAINER_SPEC = {
    # "image": "rayproject/ray:runtime_env_container_nested",
    "image": "rayproject/ray:nightly-py38-cpu",
    "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
}
CONTAINER_RUNTIME_ENV = {"container": CONTAINER_SPEC}


def check_ray_started():
    return requests.get("http://localhost:52365/api/ray/version").status_code == 200


@ray.remote(name="storage", namespace="serve", num_cpus=0)
class TelemetryStorage:
    def __init__(self):
        self.reports_received = 0
        self.current_report = dict()

    def store_report(self, report: Dict) -> None:
        self.reports_received += 1
        self.current_report = report

    def get_report(self) -> Dict:
        return self.current_report

    def get_reports_received(self) -> int:
        return self.reports_received


@serve.deployment(ray_actor_options={"num_cpus": 0})
class TelemetryReceiver:
    def __init__(self):
        self.storage = ray.get_actor(name="storage", namespace="serve")

    async def __call__(self, request) -> bool:
        report = await request.json()
        ray.get(self.storage.store_report.remote(report))
        return True


receiver_app = TelemetryReceiver.bind()


def start_telemetry_app():
    storage = TelemetryStorage.remote()
    serve.run(receiver_app, name="telemetry", route_prefix="/telemetry")
    return storage


os.environ["RAY_USAGE_STATS_ENABLED"] = "1"
os.environ["RAY_USAGE_STATS_REPORT_URL"] = "http://127.0.0.1:8000/telemetry"
os.environ["RAY_USAGE_STATS_REPORT_INTERVAL_S"] = "1"

subprocess.check_output(["ray", "start", "--head"])
wait_for_condition(check_ray_started, timeout=5)

storage_handle = start_telemetry_app()
wait_for_condition(
    lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
)
report = ray.get(storage_handle.get_report.remote())
assert ServeUsageTag.CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report) is None

# Start test
client = _get_global_client()
worker_path = "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"  # noqa
new_config = ServeDeploySchema.parse_obj(
    {
        "applications": [
            {
                "route_prefix": "/app3",
                "import_path": "serve_application:app",
                "deployments": [
                    {
                        "name": "Model",
                        "ray_actor_options": {"runtime_env": CONTAINER_RUNTIME_ENV},
                    },
                ],
            },
        ],
    }
)
client.deploy_apps(new_config)

handle = serve.get_app_handle("default")
assert handle.remote().result() == "helloworldalice"


def check_telemetry():
    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert ServeUsageTag.CONTAINER_RUNTIME_ENV_USED.get_value_from_report(report) == "1"
    return True


wait_for_condition(check_telemetry)
print("Telemetry check passed!")
