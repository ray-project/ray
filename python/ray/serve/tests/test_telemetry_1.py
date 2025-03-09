import subprocess
import sys
import time

import pytest
import requests
import yaml
from fastapi import FastAPI

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray._private.usage.usage_lib import get_extra_usage_tags_to_report
from ray.serve._private.constants import SERVE_MULTIPLEXED_MODEL_ID
from ray.serve._private.test_utils import (
    check_apps_running,
    check_telemetry,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client
from ray.serve.schema import ApplicationStatus, ServeDeploySchema


def test_fastapi_detected(manage_ray_with_telemetry):
    """
    Check that FastAPI is detected by telemetry.
    """

    storage_handle = manage_ray_with_telemetry

    # Check that telemetry related to FastAPI app is not set
    def check_report_before_fastapi():
        report = ray.get(storage_handle.get_report.remote())
        assert ServeUsageTag.FASTAPI_USED.get_value_from_report(report) is None
        assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
        assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 1
        assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 1
        assert int(ServeUsageTag.NUM_GPU_DEPLOYMENTS.get_value_from_report(report)) == 0
        return True

    wait_for_condition(check_report_before_fastapi)

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

    wait_for_condition(
        lambda: serve.status().applications["fastapi_app"].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    current_num_reports = ray.get(storage_handle.get_reports_received.remote())

    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote())
        > current_num_reports,
        timeout=5,
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


@serve.deployment
class Stub:
    pass


stub_app = Stub.bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_rest_api(manage_ray_with_telemetry, tmp_dir):
    """
    Check that telemetry works with REST API.
    """

    storage = manage_ray_with_telemetry

    # Check that REST API telemetry is not set
    check_telemetry(ServeUsageTag.REST_API_VERSION, expected=None)

    config = {
        "applications": [
            {
                "name": "stub_app",
                "import_path": "ray.serve.tests.test_telemetry_1.stub_app",
                "route_prefix": "/stub",
            },
        ]
    }
    config_file_path = f"{tmp_dir}/config.yaml"
    with open(config_file_path, "w+") as f:
        yaml.safe_dump(config, f)

    subprocess.check_output(["serve", "deploy", config_file_path])

    client = _get_global_client()
    # Make sure the applications are RUNNING.
    wait_for_condition(check_apps_running, apps=["stub_app"], timeout=15)

    current_num_reports = ray.get(storage.get_reports_received.remote())

    wait_for_condition(
        lambda: ray.get(storage.get_reports_received.remote()) > current_num_reports,
        timeout=5,
    )
    report = ray.get(storage.get_report.remote())

    # Check all telemetry relevant to the Serve apps on this cluster
    assert ServeUsageTag.REST_API_VERSION.get_value_from_report(report) == "v2"
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    assert int(ServeUsageTag.NUM_GPU_DEPLOYMENTS.get_value_from_report(report)) == 0

    # Assert num of deployments from controller.
    assert len(client.get_all_deployment_statuses()) == 2
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert int(ServeUsageTag.NUM_DEPLOYMENTS.get_value_from_report(report)) == 2

    # Check that Serve telemetry not relevant to the running apps is omitted
    assert ServeUsageTag.FASTAPI_USED.get_value_from_report(report) is None
    assert ServeUsageTag.GRPC_INGRESS_USED.get_value_from_report(report) is None
    assert ServeUsageTag.HTTP_ADAPTER_USED.get_value_from_report(report) is None
    assert ServeUsageTag.DAG_DRIVER_USED.get_value_from_report(report) is None
    assert ServeUsageTag.AUTO_NUM_REPLICAS_USED.get_value_from_report(report) is None

    # Check that app deletions are tracked.
    new_config = {"applications": []}

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
    "lightweight_option,value,new_value",
    [
        ("num_replicas", 1, 2),
        ("user_config", {}, {"some_setting": 10}),
        ("autoscaling_config", {"max_replicas": 3}, {"max_replicas": 5}),
    ],
)
def test_lightweight_config_options(
    manage_ray_with_telemetry, lightweight_option, value, new_value
):
    """Check that lightweight config options are detected by telemetry."""

    storage = manage_ray_with_telemetry

    lightweight_tagkeys = {
        "num_replicas": ServeUsageTag.NUM_REPLICAS_LIGHTWEIGHT_UPDATED,
        "user_config": ServeUsageTag.USER_CONFIG_LIGHTWEIGHT_UPDATED,
        "autoscaling_config": ServeUsageTag.AUTOSCALING_CONFIG_LIGHTWEIGHT_UPDATED,
    }

    # Check that REST API telemetry is not set
    check_telemetry(ServeUsageTag.REST_API_VERSION, expected=None)

    config = {
        "applications": [
            {
                "name": "test_app",
                "import_path": "ray.serve.tests.test_telemetry_1.tester",
                "deployments": [{"name": "Tester"}],
            },
        ]
    }
    config["applications"][0]["deployments"][0][lightweight_option] = value

    # Deploy first config
    client = _get_global_client()
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(check_apps_running, apps=["test_app"], timeout=15)

    current_num_reports = ray.get(storage.get_reports_received.remote())

    wait_for_condition(
        lambda: ray.get(storage.get_reports_received.remote()) > current_num_reports,
        timeout=10,
    )
    report = ray.get(storage.get_report.remote())

    # Check
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    for tagkey in lightweight_tagkeys.values():
        assert tagkey.get_value_from_report(report) is None

    # Change config and deploy again
    config["applications"][0]["deployments"][0][lightweight_option] = new_value
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(check_apps_running, apps=["test_app"], timeout=15)

    # Check again
    wait_for_condition(
        check_telemetry,
        tag=lightweight_tagkeys[lightweight_option],
        expected="True",
        timeout=5,
    )
    report = ray.get(storage.get_report.remote())
    assert int(ServeUsageTag.NUM_APPS.get_value_from_report(report)) == 2
    assert ServeUsageTag.API_VERSION.get_value_from_report(report) == "v2"
    for tagkey in lightweight_tagkeys.values():
        if tagkey != lightweight_tagkeys[lightweight_option]:
            assert tagkey.get_value_from_report(report) is None


@pytest.mark.parametrize("call_in_deployment", [False, True])
def test_handle_apis_detected(manage_ray_with_telemetry, call_in_deployment):
    """Check that the various handles are detected correctly by telemetry."""

    check_telemetry(ServeUsageTag.DEPLOYMENT_HANDLE_API_USED, expected=None)

    @serve.deployment
    class Downstream:
        def __call__(self):
            return "hi"

    @serve.deployment
    class Caller:
        def __init__(self, h):
            self._h = h

        async def __call__(self, call_downstream=True):
            if call_downstream:
                await self._h.remote()
            return "ok"

    handle = serve.run(Caller.bind(Downstream.bind()))

    if call_in_deployment:
        result = requests.get("http://localhost:8000").text
    else:
        result = handle.remote(call_downstream=False).result()

    assert result == "ok"

    wait_for_condition(
        check_telemetry, tag=ServeUsageTag.DEPLOYMENT_HANDLE_API_USED, expected="1"
    )


@pytest.mark.parametrize("mode", ["http", "outside_deployment", "inside_deployment"])
def test_deployment_handle_to_obj_ref_detected(manage_ray_with_telemetry, mode):
    """Check that the handle to_object_ref API is detected correctly by telemetry."""

    check_telemetry(
        ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED, expected=None
    )

    @serve.deployment
    class Downstream:
        def __call__(self):
            return "hi"

    @serve.deployment
    class Caller:
        def __init__(self, h):
            self._h = h

        async def get(self, call_downstream=False):
            if call_downstream:
                await self._h.remote()._to_object_ref()
            return "ok"

        async def __call__(self):
            return await self.get()

    handle = serve.run(Caller.bind(Downstream.bind()))

    if mode == "http":
        result = requests.get("http://localhost:8000").text
    elif mode == "outside_deployment":
        result = ray.get(handle.get.remote()._to_object_ref_sync())
    else:
        result = handle.get.remote(call_downstream=True).result()

    assert result == "ok"

    if mode == "http":
        for _ in range(20):
            check_telemetry(
                ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED, expected=None
            )
            time.sleep(0.1)
    else:
        wait_for_condition(
            check_telemetry,
            tag=ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED,
            expected="1",
        )


def test_multiplexed_detect(manage_ray_with_telemetry):
    """Check that multiplexed api is detected by telemetry."""

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
    wait_for_condition(check_apps_running, apps=["app"])

    check_telemetry(ServeUsageTag.MULTIPLEXED_API_USED, expected=None)

    headers = {SERVE_MULTIPLEXED_MODEL_ID: "1"}
    resp = requests.get("http://localhost:8000/app", headers=headers)
    assert resp.status_code == 200

    wait_for_condition(
        check_telemetry, tag=ServeUsageTag.MULTIPLEXED_API_USED, expected="1"
    )


class TestProxyTelemetry:
    def test_both_proxies_detected(manage_ray, ray_shutdown):
        """Test that both HTTP and gRPC proxies are detected by telemetry.

        When both HTTP and gRPC proxies are used, both telemetry should be detected.
        """
        result = get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        report = {"extra_usage_tags": result}

        # Ensure neither the HTTP nor gRPC proxy telemetry exist.
        assert ServeUsageTag.HTTP_PROXY_USED.get_value_from_report(report) is None
        assert ServeUsageTag.GRPC_PROXY_USED.get_value_from_report(report) is None

        grpc_servicer_functions = [
            "ray.serve.generated.serve_pb2_grpc."
            "add_UserDefinedServiceServicer_to_server",
        ]
        serve.start(grpc_options={"grpc_servicer_functions": grpc_servicer_functions})

        result = get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        report = {"extra_usage_tags": result}

        # Ensure both HTTP and gRPC proxy telemetry exist.
        assert int(ServeUsageTag.HTTP_PROXY_USED.get_value_from_report(report)) == 1
        assert int(ServeUsageTag.GRPC_PROXY_USED.get_value_from_report(report)) == 1

    def test_only_http_proxy_detected(manage_ray, ray_shutdown):
        """Test that only HTTP proxy is detected by telemetry.

        When only HTTP proxy is used, only the http proxy telemetry should be detected.
        """
        result = get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        report = {"extra_usage_tags": result}

        # Ensure the telemetry does not yet exist.
        assert ServeUsageTag.HTTP_PROXY_USED.get_value_from_report(report) is None
        assert ServeUsageTag.GRPC_PROXY_USED.get_value_from_report(report) is None

        serve.start()

        result = get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        report = {"extra_usage_tags": result}

        # Ensure only the HTTP proxy telemetry exist.
        assert int(ServeUsageTag.HTTP_PROXY_USED.get_value_from_report(report)) == 1
        assert ServeUsageTag.GRPC_PROXY_USED.get_value_from_report(report) is None

    def test_no_proxy_detected(manage_ray, ray_shutdown):
        """Test that no proxy is detected by telemetry.

        When neither HTTP nor gRPC proxy is used, no proxy telemetry should be detected.
        """
        result = get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        report = {"extra_usage_tags": result}

        # Ensure neither the HTTP nor gRPC proxy telemetry exist.
        assert ServeUsageTag.HTTP_PROXY_USED.get_value_from_report(report) is None
        assert ServeUsageTag.GRPC_PROXY_USED.get_value_from_report(report) is None

        serve.start(http_options={"location": "NoServer"})

        result = get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        report = {"extra_usage_tags": result}

        # Ensure neither the HTTP nor gRPC proxy telemetry exist.
        assert ServeUsageTag.HTTP_PROXY_USED.get_value_from_report(report) is None
        assert ServeUsageTag.GRPC_PROXY_USED.get_value_from_report(report) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
