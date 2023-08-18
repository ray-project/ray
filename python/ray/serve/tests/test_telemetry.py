import sys
import yaml
import pytest
import requests
import subprocess
from typing import Dict
from fastapi import FastAPI
from starlette.requests import Request

import ray
from ray._private.test_utils import wait_for_condition

from ray import serve
from ray.dag.input_node import InputNode
from ray.serve.drivers import DefaultgRPCDriver, DAGDriver
from ray.serve.http_adapters import json_request
from ray.serve._private.constants import SERVE_NAMESPACE, SERVE_DEFAULT_APP_NAME
from ray.serve.context import get_global_client
from ray.serve._private.common import ApplicationStatus
from ray._private.usage.usage_lib import get_extra_usage_tags_to_report
from ray.serve.schema import ServeDeploySchema


TELEMETRY_ROUTE_PREFIX = "/telemetry"
STORAGE_ACTOR_NAME = "storage"


def check_ray_stopped():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


def check_ray_started():
    return requests.get("http://localhost:52365/api/ray/version").status_code == 200


@pytest.fixture
def manage_ray(monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv(
            "RAY_USAGE_STATS_REPORT_URL",
            f"http://127.0.0.1:8000{TELEMETRY_ROUTE_PREFIX}",
        )
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        subprocess.check_output(["ray", "stop", "--force"])
        wait_for_condition(check_ray_stopped, timeout=5)
        yield

        # Call Python API shutdown() methods to clear global variable state
        serve.shutdown()
        ray.shutdown()

        # Shut down Ray cluster with CLI
        subprocess.check_output(["ray", "stop", "--force"])
        wait_for_condition(check_ray_stopped, timeout=5)


@ray.remote(name=STORAGE_ACTOR_NAME, namespace=SERVE_NAMESPACE, num_cpus=0)
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
        self.storage = ray.get_actor(name=STORAGE_ACTOR_NAME, namespace=SERVE_NAMESPACE)

    async def __call__(self, request: Request) -> bool:
        report = await request.json()
        ray.get(self.storage.store_report.remote(report))
        return True


receiver_app = TelemetryReceiver.bind()


def start_telemetry_app():
    """Start a telemetry Serve app.

    Ray should be initialized before calling this method.

    NOTE: If you're running the TelemetryReceiver Serve app to check telemetry,
    remember that the receiver itself is counted in the telemetry. E.g. if you
    deploy a Serve app other than the receiver, the number of apps in the
    cluster is 2- not 1– since the receiver is also running.

    Returns a handle to a TelemetryStorage actor. You can use this actor
    to access the latest telemetry reports.
    """

    storage = TelemetryStorage.remote()
    serve.run(receiver_app, name="telemetry", route_prefix=TELEMETRY_ROUTE_PREFIX)
    return storage


def test_fastapi_detected(manage_ray):
    """
    Check that FastAPI is detected by telemetry.
    """

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class FastAPIDeployment:
        @app.get("/route1")
        async def route1(self):
            return "Welcome to route 1!"

        @app.get("/route2")
        async def app2(self):
            return "Welcome to route 2!"

    fastapi_app = FastAPIDeployment.bind()
    serve.run(fastapi_app, name="fastapi_app", route_prefix="/fastapi")

    storage_handle = start_telemetry_app()

    wait_for_condition(
        lambda: serve.status().applications["fastapi_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    report = ray.get(storage_handle.get_report.remote())

    # Check all telemetry relevant to the Serve apps on this cluster
    assert int(report["extra_usage_tags"]["serve_fastapi_used"]) == 1
    assert report["extra_usage_tags"]["serve_api_version"] == "v2"
    assert int(report["extra_usage_tags"]["serve_num_apps"]) == 2
    assert int(report["extra_usage_tags"]["serve_num_deployments"]) == 2
    assert int(report["extra_usage_tags"]["serve_num_gpu_deployments"]) == 0

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert "serve_dag_driver_used" not in report
    assert "serve_http_adapter_used" not in report
    assert "serve_grpc_ingress_used" not in report
    assert "serve_rest_api_version" not in report


def test_grpc_detected(manage_ray):
    """
    Check that gRPCIngress is detected by telemetry.
    """

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    @serve.deployment(ray_actor_options={"num_cpus": 0})
    def greeter(inputs: Dict[str, bytes]):
        return "Hello!"

    with InputNode() as grpc_input:
        greeter_node = greeter.bind(grpc_input)
        grpc_app = DefaultgRPCDriver.bind(greeter_node)

    serve.run(grpc_app, name="grpc_app", route_prefix="/grpc")

    storage_handle = start_telemetry_app()

    wait_for_condition(
        lambda: serve.status().applications["grpc_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    report = ray.get(storage_handle.get_report.remote())

    # Check all telemetry relevant to the Serve apps on this cluster
    assert int(report["extra_usage_tags"]["serve_grpc_ingress_used"]) == 1
    assert report["extra_usage_tags"]["serve_api_version"] == "v2"
    assert int(report["extra_usage_tags"]["serve_num_apps"]) == 2
    assert int(report["extra_usage_tags"]["serve_num_deployments"]) == 3
    assert int(report["extra_usage_tags"]["serve_num_gpu_deployments"]) == 0

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert "serve_dag_driver_used" not in report
    assert "serve_http_adapter_used" not in report
    assert "serve_fastapi_used" not in report
    assert "serve_rest_api_version" not in report


@pytest.mark.parametrize("use_adapter", [True, False])
def test_graph_detected(manage_ray, use_adapter):
    """
    Check that DAGDriver and HTTP adapters are detected by telemetry.
    """

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    @serve.deployment(ray_actor_options={"num_cpus": 0})
    def greeter(input):
        return "Hello!"

    with InputNode() as input:
        greeter_node = greeter.bind(input)

    if use_adapter:
        graph_app = DAGDriver.bind(greeter_node, http_adapter=json_request)
    else:
        graph_app = DAGDriver.bind(greeter_node)

    serve.run(graph_app, name="graph_app", route_prefix="/graph")

    wait_for_condition(
        lambda: serve.status().applications["graph_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    storage_handle = start_telemetry_app()

    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    report = ray.get(storage_handle.get_report.remote())

    # Check all telemetry relevant to the Serve apps on this cluster
    assert int(report["extra_usage_tags"]["serve_dag_driver_used"]) == 1
    assert report["extra_usage_tags"]["serve_api_version"] == "v2"
    assert int(report["extra_usage_tags"]["serve_num_apps"]) == 2
    assert int(report["extra_usage_tags"]["serve_num_deployments"]) == 3
    assert int(report["extra_usage_tags"]["serve_num_gpu_deployments"]) == 0
    if use_adapter:
        assert int(report["extra_usage_tags"]["serve_http_adapter_used"]) == 1

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert "serve_fastapi_used" not in report
    assert "serve_grpc_ingress_used" not in report
    assert "serve_rest_api_version" not in report
    if not use_adapter:
        assert "serve_http_adapter_used" not in report


@serve.deployment
class Stub:
    pass


stub_app = Stub.bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize("version", ["v1", "v2"])
def test_rest_api(manage_ray, tmp_dir, version):
    """
    Check that telemetry works with REST API.
    """

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage = TelemetryStorage.remote()

    if version == "v1":
        config = {"import_path": "ray.serve.tests.test_telemetry.receiver_app"}
    elif version == "v2":
        config = {
            "applications": [
                {
                    "name": "receiver_app",
                    "import_path": "ray.serve.tests.test_telemetry.receiver_app",
                    "route_prefix": TELEMETRY_ROUTE_PREFIX,
                },
                {
                    "name": "stub_app",
                    "import_path": "ray.serve.tests.test_telemetry.stub_app",
                    "route_prefix": "/stub",
                },
            ]
        }
    config_file_path = f"{tmp_dir}/config.yaml"
    with open(config_file_path, "w+") as f:
        yaml.safe_dump(config, f)

    subprocess.check_output(["serve", "deploy", config_file_path])

    client = get_global_client()
    if version == "v2":
        # Make sure the applications are RUNNING.
        wait_for_condition(
            lambda: serve.status().applications["receiver_app"].status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )
        wait_for_condition(
            lambda: serve.status().applications["stub_app"].status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )
    else:
        wait_for_condition(
            lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )

    wait_for_condition(
        lambda: ray.get(storage.get_reports_received.remote()) > 10, timeout=15
    )
    report = ray.get(storage.get_report.remote())

    # Check all telemetry relevant to the Serve apps on this cluster
    assert report["extra_usage_tags"]["serve_rest_api_version"] == version
    assert report["extra_usage_tags"]["serve_api_version"] == "v2"
    assert int(report["extra_usage_tags"]["serve_num_gpu_deployments"]) == 0
    if version == "v1":
        assert int(report["extra_usage_tags"]["serve_num_apps"]) == 1
        assert int(report["extra_usage_tags"]["serve_num_deployments"]) == 1
    elif version == "v2":
        # Assert num of deployments from controller
        assert len(client.get_all_deployment_statuses()) == 2
        result = get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert int(result["serve_num_deployments"]) == 2
        assert int(report["extra_usage_tags"]["serve_num_apps"]) == 2
        assert int(report["extra_usage_tags"]["serve_num_deployments"]) == 2

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert "serve_fastapi_used" not in report
    assert "serve_grpc_ingress_used" not in report
    assert "serve_http_adapter_used" not in report
    assert "serve_dag_driver_used" not in report

    # Check that app deletions are tracked in v2
    if version == "v2":
        new_config = {
            "applications": [
                {
                    "name": "receiver_app",
                    "import_path": "ray.serve.tests.test_telemetry.receiver_app",
                    "route_prefix": TELEMETRY_ROUTE_PREFIX,
                },
            ]
        }

        with open(config_file_path, "w+") as f:
            yaml.safe_dump(new_config, f)

        subprocess.check_output(["serve", "deploy", config_file_path])

        wait_for_condition(
            lambda: int(
                ray.get(storage.get_report.remote())["extra_usage_tags"][
                    "serve_num_apps"
                ]
            )
            == 1,
            timeout=15,
        )
        report = ray.get(storage.get_report.remote())
        assert int(report["extra_usage_tags"]["serve_num_apps"]) == 1
        assert int(report["extra_usage_tags"]["serve_num_deployments"]) == 1


@serve.deployment(ray_actor_options={"num_cpus": 0})
class Tester:
    def __call__(self, *args):
        pass

    def reconfigure(self, *args):
        pass


tester = Tester.bind()


@pytest.mark.parametrize(
    "lightweight_option,value",
    [
        ("num_replicas", 2),
        ("user_config", {"some_setting": 10}),
        ("autoscaling_config", {"max_replicas": 5}),
    ],
)
def test_lightweight_config_options(manage_ray, lightweight_option, value):
    """
    Check that lightweight config options are detected by telemetry.
    """

    lightweight_tagkeys = [
        "serve_num_replicas_lightweight_updated",
        "serve_user_config_lightweight_updated",
        "serve_autoscaling_config_lightweight_updated",
    ]

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)
    storage = TelemetryStorage.remote()

    config = {
        "applications": [
            {
                "name": "receiver_app",
                "import_path": "ray.serve.tests.test_telemetry.receiver_app",
                "route_prefix": TELEMETRY_ROUTE_PREFIX,
            },
            {
                "name": "test_app",
                "import_path": "ray.serve.tests.test_telemetry.tester",
                "deployments": [{"name": "Tester"}],
            },
        ]
    }

    # Deploy first config
    client = serve.start(detached=True)
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(
        lambda: serve.status().applications["receiver_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )
    wait_for_condition(
        lambda: serve.status().applications["test_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )
    wait_for_condition(
        lambda: ray.get(storage.get_reports_received.remote()) > 0, timeout=5
    )
    report = ray.get(storage.get_report.remote())

    # Check
    assert int(report["extra_usage_tags"]["serve_num_apps"]) == 2
    assert report["extra_usage_tags"]["serve_api_version"] == "v2"
    for tagkey in lightweight_tagkeys:
        assert tagkey not in report["extra_usage_tags"]

    # Change config and deploy again
    config["applications"][1]["deployments"][0][lightweight_option] = value
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(
        lambda: serve.status().applications["receiver_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )
    wait_for_condition(
        lambda: serve.status().applications["test_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    # Check again
    wait_for_condition(
        lambda: ray.get(storage.get_report.remote())["extra_usage_tags"][
            f"serve_{lightweight_option}_lightweight_updated"
        ]
        == "True",
        timeout=5,
    )
    report = ray.get(storage.get_report.remote())
    assert int(report["extra_usage_tags"]["serve_num_apps"]) == 2
    assert report["extra_usage_tags"]["serve_api_version"] == "v2"
    for tagkey in lightweight_tagkeys:
        if not tagkey == f"serve_{lightweight_option}_lightweight_updated":
            assert tagkey not in report["extra_usage_tags"]


@pytest.mark.parametrize("use_new_handle_api", [False, True])
@pytest.mark.parametrize("call_in_deployment", [False, True])
def test_handle_apis_detected(manage_ray, use_new_handle_api, call_in_deployment):
    """Check that the various handles are detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )

    @serve.deployment
    class Downstream:
        def __call__(self):
            return "hi"

    @serve.deployment
    class Caller:
        def __init__(self, h):
            self._h = h.options(use_new_handle_api=use_new_handle_api)

        async def __call__(self, call_downstream=True):
            if call_downstream:
                await self._h.remote()
            return "ok"

    handle = serve.run(Caller.bind(Downstream.bind()))

    if call_in_deployment:
        result = requests.get("http://localhost:8000").text
    elif use_new_handle_api:
        result = (
            handle.options(use_new_handle_api=True)
            .remote(call_downstream=False)
            .result()
        )
    else:
        result = ray.get(handle.remote(call_downstream=False))

    assert result == "ok"

    def check_telemetry():
        report = ray.get(storage_handle.get_report.remote())
        print(report["extra_usage_tags"])
        if use_new_handle_api:
            assert (
                report["extra_usage_tags"].get("serve_deployment_handle_api_used", "0")
                == "1"
            )
        elif call_in_deployment:
            assert (
                report["extra_usage_tags"].get("serve_ray_serve_handle_api_used", "0")
                == "1"
            )
        else:
            assert (
                report["extra_usage_tags"].get(
                    "serve_ray_serve_sync_handle_api_used", "0"
                )
                == "1"
            )

        assert (
            report["extra_usage_tags"].get(
                "serve_deployment_handle_to_object_ref_api_used", "0"
            )
            == "1"
        )
        return True

    wait_for_condition(check_telemetry)


@pytest.mark.parametrize("call_in_deployment", [False, True])
def test_deployment_handle_to_obj_ref_detected(manage_ray, call_in_deployment):
    """Check that the handle to_object_ref API is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )

    @serve.deployment
    class Downstream:
        def __call__(self):
            return "hi"

    @serve.deployment
    class Caller:
        def __init__(self, h):
            self._h = h.options(use_new_handle_api=True)

        async def __call__(self, call_downstream=True):
            if call_downstream:
                await self._h.remote()._to_object_ref()
            return "ok"

    handle = serve.run(Caller.bind(Downstream.bind()))

    if call_in_deployment:
        result = requests.get("http://localhost:8000").text
    else:
        result = ray.get(
            handle.options(use_new_handle_api=True)
            .remote(call_downstream=False)
            ._to_object_ref_sync()
        )

    assert result == "ok"

    def check_telemetry():
        report = ray.get(storage_handle.get_report.remote())
        print(report["extra_usage_tags"])
        assert (
            report["extra_usage_tags"]["serve_deployment_handle_to_object_ref_api_used"]
            == "1"
        )
        return True

    wait_for_condition(check_telemetry)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
