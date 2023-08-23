import sys
import yaml
import pytest
import requests
import subprocess
import time
from typing import Dict
from fastapi import FastAPI

import ray
from ray.dag.input_node import InputNode
from ray._private.test_utils import wait_for_condition

from ray import serve
from ray.serve.context import get_global_client
from ray.serve.drivers import DefaultgRPCDriver, DAGDriver
from ray.serve.http_adapters import json_request
from ray.serve.schema import ServeDeploySchema
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    SERVE_MULTIPLEXED_MODEL_ID,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve.tests.utils import (
    check_ray_started,
    start_telemetry_app,
    check_telemetry_recorded,
    check_telemetry_not_recorded,
    TelemetryStorage,
    TELEMETRY_ROUTE_PREFIX,
)


def test_fastapi_detected(manage_ray_with_telemetry):
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

    # Check all telemetry relevant to the Serve apps on this cluster.
    assert int(ServeUsageTag.FASTAPI_USED.get_value_from_report(report)) == 1
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 2
    assert int(ServeUsageTag.NUM_GPU_DEPLOYMENTS.get_value_from_report(report)) == 0

    # Check that Serve telemetry not relevant to the running apps is omitted.
    assert ServeUsageTag.DAG_DRIVER_USED.get_value_from_report(report) is None
    assert ServeUsageTag.HTTP_ADAPTER_USED.get_value_from_report(report) is None
    assert ServeUsageTag.GRPC_INGRESS_USED.get_value_from_report(report) is None
    assert ServeUsageTag.REST_API_VERSION.get_value_from_report(report) is None


def test_grpc_detected(manage_ray_with_telemetry):
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
    assert ServeUsageTag.GRPC_INGRESS_USED.get_value_from_report(report) == "1"
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 3
    assert int(ServeUsageTag.NUM_GPU_DEPLOYMENTS.get_value_from_report(report)) == 0

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert ServeUsageTag.DAG_DRIVER_USED.get_value_from_report(report) is None
    assert ServeUsageTag.HTTP_ADAPTER_USED.get_value_from_report(report) is None
    assert ServeUsageTag.FASTAPI_USED.get_value_from_report(report) is None
    assert ServeUsageTag.REST_API_VERSION.get_value_from_report(report) is None


@pytest.mark.parametrize("use_adapter", [True, False])
def test_graph_detected(manage_ray_with_telemetry, use_adapter):
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
    assert ServeUsageTag.DAG_DRIVER_USED.get_value_from_report(report) == "1"
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 3
    assert int(ServeUsageTag.NUM_GPU_DEPLOYMENTS.get_value_from_report(report)) == 0
    if use_adapter:
        assert ServeUsageTag.HTTP_ADAPTER_USED.get_value_from_report(report) == "1"

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert ServeUsageTag.FASTAPI_USED.get_value_from_report(report) is None
    assert ServeUsageTag.GRPC_INGRESS_USED.get_value_from_report(report) is None
    assert ServeUsageTag.REST_API_VERSION.get_value_from_report(report) is None
    if not use_adapter:
        assert ServeUsageTag.HTTP_ADAPTER_USED.get_value_from_report(report) is None


@serve.deployment
class Stub:
    pass


stub_app = Stub.bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize("version", ["v1", "v2"])
def test_rest_api(manage_ray_with_telemetry, tmp_dir, version):
    """
    Check that telemetry works with REST API.
    """

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage = TelemetryStorage.remote()

    if version == "v1":
        config = {"import_path": "ray.serve.tests.utils.receiver_app"}
    elif version == "v2":
        config = {
            "applications": [
                {
                    "name": "receiver_app",
                    "import_path": "ray.serve.tests.utils.receiver_app",
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
    assert ServeUsageTag.REST_API_VERSION.get_value_from_report(report) == version
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    assert int(ServeUsageTag.NUM_GPU_DEPLOYMENTS.get_value_from_report(report)) == 0
    if version == "v1":
        assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 1
        assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 1
    elif version == "v2":
        # Assert num of deployments from controller.
        assert len(client.get_all_deployment_statuses()) == 2
        assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
        assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 2

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert ServeUsageTag.FASTAPI_USED.get_value_from_report(report) is None
    assert ServeUsageTag.GRPC_INGRESS_USED.get_value_from_report(report) is None
    assert ServeUsageTag.HTTP_ADAPTER_USED.get_value_from_report(report) is None
    assert ServeUsageTag.DAG_DRIVER_USED.get_value_from_report(report) is None

    # Check that app deletions are tracked in v2
    if version == "v2":
        new_config = {
            "applications": [
                {
                    "name": "receiver_app",
                    "import_path": "ray.serve.tests.utils.receiver_app",
                    "route_prefix": TELEMETRY_ROUTE_PREFIX,
                },
            ]
        }

        with open(config_file_path, "w+") as f:
            yaml.safe_dump(new_config, f)

        subprocess.check_output(["serve", "deploy", config_file_path])

        wait_for_condition(
            lambda: int(
                ServeUsageTag.NUM_APPS.get_value_from_report(
                    ray.get(storage.get_report.remote())
                )
            )
            == 1,
            timeout=15,
        )
        report = ray.get(storage.get_report.remote())
        assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 1
        assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 1


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
def test_lightweight_config_options(
    manage_ray_with_telemetry, lightweight_option, value
):
    """
    Check that lightweight config options are detected by telemetry.
    """

    lightweight_tagkeys = {
        "num_replicas": ServeUsageTag.NUM_REPLICAS_LIGHTWEIGHT_UPDATED,
        "user_config": ServeUsageTag.USER_CONFIG_LIGHTWEIGHT_UPDATED,
        "autoscaling_config": ServeUsageTag.AUTOSCALING_CONFIG_LIGHTWEIGHT_UPDATED,
    }

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)
    storage = TelemetryStorage.remote()

    config = {
        "applications": [
            {
                "name": "receiver_app",
                "import_path": "ray.serve.tests.utils.receiver_app",
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
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    for tagkey in lightweight_tagkeys.values():
        assert tagkey.get_value_from_report(report) is None

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
        lambda: lightweight_tagkeys[lightweight_option].get_value_from_report(
            ray.get(storage.get_report.remote())
        )
        == "True",
        timeout=5,
    )
    report = ray.get(storage.get_report.remote())
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    for tagkey in lightweight_tagkeys.values():
        if tagkey != lightweight_tagkeys[lightweight_option]:
            assert tagkey.get_value_from_report(report) is None


@pytest.mark.parametrize("use_new_handle_api", [False, True])
@pytest.mark.parametrize("call_in_deployment", [False, True])
def test_handle_apis_detected(
    manage_ray_with_telemetry, use_new_handle_api, call_in_deployment
):
    """Check that the various handles are detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )

    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert (
        ServeUsageTag.DEPLOYMENT_HANDLE_API_USED.get_value_from_report(report) is None
    )
    assert ServeUsageTag.RAY_SERVE_HANDLE_API_USED.get_value_from_report(report) is None
    assert (
        ServeUsageTag.RAY_SERVE_SYNC_HANDLE_API_USED.get_value_from_report(report)
        is None
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
                ServeUsageTag.DEPLOYMENT_HANDLE_API_USED.get_value_from_report(report)
                == "1"
            )
        elif call_in_deployment:
            assert (
                ServeUsageTag.RAY_SERVE_HANDLE_API_USED.get_value_from_report(report)
                == "1"
            )
        else:
            assert (
                ServeUsageTag.RAY_SERVE_SYNC_HANDLE_API_USED.get_value_from_report(
                    report
                )
                == "1"
            )
        return True

    wait_for_condition(check_telemetry)


@pytest.mark.parametrize("mode", ["http", "outside_deployment", "inside_deployment"])
def test_deployment_handle_to_obj_ref_detected(manage_ray_with_telemetry, mode):
    """Check that the handle to_object_ref API is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )

    report = ray.get(storage_handle.get_report.remote())
    print(report["extra_usage_tags"])
    assert (
        ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.get_value_from_report(
            report
        )
        is None
    )

    @serve.deployment
    class Downstream:
        def __call__(self):
            return "hi"

    @serve.deployment
    class Caller:
        def __init__(self, h):
            self._h = h.options(use_new_handle_api=True)

        async def __call__(self, call_downstream=False):
            if call_downstream:
                await self._h.remote()._to_object_ref()
            return "ok"

    handle = serve.run(Caller.bind(Downstream.bind()))

    if mode == "http":
        result = requests.get("http://localhost:8000").text
    elif mode == "outside_deployment":
        result = ray.get(
            handle.options(use_new_handle_api=True).remote()._to_object_ref_sync()
        )
    else:
        result = (
            handle.options(
                use_new_handle_api=True,
            )
            .remote(call_downstream=True)
            .result()
        )

    assert result == "ok"

    def check_telemetry(tag_should_be_set: bool):
        report = ray.get(storage_handle.get_report.remote())
        print(report["extra_usage_tags"])
        tag = ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED
        if tag_should_be_set:
            assert tag.get_value_from_report(report) == "1"
        else:
            assert tag.get_value_from_report(report) is None

        return True

    if mode == "http":
        for _ in range(20):
            check_telemetry(tag_should_be_set=False)
    else:
        wait_for_condition(check_telemetry, tag_should_be_set=True)


def test_multiplexed_detect(manage_ray_with_telemetry):
    """Check that multiplexed api is detected by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    @serve.deployment
    class Model:
        @serve.multiplexed(max_num_models_per_replica=1)
        async def get_model(self, tag):
            return tag

        async def __call__(self, request):
            tag = serve.get_multiplexed_model_id()
            await self.get_model(tag)
            return tag

    serve.run(Model.bind(), name="app", route_prefix="/app")

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    report = ray.get(storage_handle.get_report.remote())
    assert ServeUsageTag.MULTIPLEXED_API_USED.get_value_from_report(report) is None

    client = get_global_client()
    wait_for_condition(
        lambda: client.get_serve_status("app").app_status.status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )

    headers = {SERVE_MULTIPLEXED_MODEL_ID: "1"}
    resp = requests.get("http://localhost:8000/app", headers=headers)
    assert resp.status_code == 200

    wait_for_condition(
        lambda: int(
            ServeUsageTag.MULTIPLEXED_API_USED.get_value_from_report(
                ray.get(storage_handle.get_report.remote())
            )
        )
        == 1,
        timeout=5,
    )


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_status_api_detected(manage_ray_with_telemetry, location):
    """Check that serve.status is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    # Check telemetry is not recorded before test starts
    check_telemetry_not_recorded(storage_handle, "serve_status_api_used")

    @serve.deployment
    class Model:
        async def __call__(self):
            return serve.status()

    if location:
        if location == "deployment":
            handle = serve.run(Model.bind(), route_prefix="/model")
            handle.remote()
        elif location == "driver":
            serve.status()

        wait_for_condition(
            check_telemetry_recorded,
            storage_handle=storage_handle,
            key="serve_status_api_used",
            expected_value="1",
        )
    else:
        for _ in range(3):
            check_telemetry_not_recorded(storage_handle, "serve_status_api_used")
            time.sleep(1)


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_get_app_handle_api_detected(manage_ray_with_telemetry, location):
    """Check that serve.get_app_handle is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    # Check telemetry is not recorded before test starts
    check_telemetry_not_recorded(storage_handle, "serve_get_app_handle_api_used")

    @serve.deployment
    class Model:
        async def __call__(self):
            serve.get_app_handle("telemetry")

    if location:
        if location == "deployment":
            handle = serve.run(Model.bind(), route_prefix="/model")
            handle.remote()
        elif location == "driver":
            serve.get_app_handle("telemetry")

        wait_for_condition(
            check_telemetry_recorded,
            storage_handle=storage_handle,
            key="serve_get_app_handle_api_used",
            expected_value="1",
        )
    else:
        for _ in range(3):
            check_telemetry_not_recorded(
                storage_handle, "serve_get_app_handle_api_used"
            )
            time.sleep(1)


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_get_deployment_handle_api_detected(manage_ray_with_telemetry, location):
    """Check that serve.get_deployment_handle is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    # Check telemetry is not recorded before test starts
    check_telemetry_not_recorded(storage_handle, "serve_get_deployment_handle_api_used")

    @serve.deployment
    class Model:
        async def __call__(self):
            serve.get_deployment_handle("TelemetryReceiver", "telemetry")

    if location:
        if location == "deployment":
            handle = serve.run(Model.bind(), route_prefix="/model")
            handle.remote()
        elif location == "driver":
            serve.get_deployment_handle("TelemetryReceiver", "telemetry")

        wait_for_condition(
            check_telemetry_recorded,
            storage_handle=storage_handle,
            key="serve_get_deployment_handle_api_used",
            expected_value="1",
        )
    else:
        for _ in range(3):
            check_telemetry_not_recorded(
                storage_handle, "serve_get_deployment_handle_api_used"
            )
            time.sleep(1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
