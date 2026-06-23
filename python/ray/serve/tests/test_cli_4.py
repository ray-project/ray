import os
import subprocess
import sys
from tempfile import NamedTemporaryFile

import grpc
import httpx
import pytest

from ray._common.test_utils import wait_for_condition
from ray.serve._private.test_utils import (
    get_application_url,
    ping_fruit_stand,
    ping_grpc_another_method,
    ping_grpc_call_method,
    ping_grpc_healthz,
    ping_grpc_list_applications,
    ping_grpc_model_multiplexing,
    ping_grpc_streaming,
)
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.tests.test_cli_2 import ping_endpoint


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_build_multi_app(ray_start_stop):
    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:
        print('Building nodes "TestApp1Node" and "TestApp2Node".')
        # Build an app
        grpc_servicer_func_root = "ray.serve.generated.serve_pb2_grpc"
        subprocess.check_output(
            [
                "serve",
                "build",
                "ray.serve.tests.test_cli_3.TestApp1Node",
                "ray.serve.tests.test_cli_3.TestApp2Node",
                "ray.serve.tests.test_config_files.grpc_deployment.g",
                "--grpc-servicer-functions",
                f"{grpc_servicer_func_root}.add_UserDefinedServiceServicer_to_server",
                "-o",
                tmp.name,
            ]
        )
        print("Build succeeded! Deploying node.")

        subprocess.check_output(["serve", "deploy", tmp.name])
        print("Deploy succeeded!")
        wait_for_condition(
            lambda: ping_endpoint("app1") == "wonderful world", timeout=15
        )
        print("App 1 is live and reachable over HTTP.")
        wait_for_condition(
            lambda: ping_endpoint("app2") == "wonderful world", timeout=15
        )
        print("App 2 is live and reachable over HTTP.")

        app_name = "app3"
        channel = grpc.insecure_channel(get_application_url("gRPC", app_name=app_name))
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        metadata = (("application", app_name),)
        response = stub.__call__(request=request, metadata=metadata)
        assert response.greeting == "Hello foo from bar"
        print("App 3 is live and reachable over gRPC.")

        print("Deleting applications.")
        app_urls = [
            get_application_url("HTTP", app_name=app) for app in ["app1", "app2"]
        ]
        subprocess.check_output(["serve", "shutdown", "-y"])

        def check_no_apps():
            for url in app_urls:
                with pytest.raises(httpx.HTTPError):
                    _ = httpx.get(url).text
            return True

        wait_for_condition(check_no_apps, timeout=15)
        print("Delete succeeded! Node is no longer reachable over HTTP.")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_serving_request_through_grpc_proxy(ray_start_stop):
    """Test serving request through gRPC proxy

    When Serve runs with a gRPC deployment, the app should be deployed successfully,
    both ListApplications and Healthz methods returning success response, and registered
    gRPC methods are routing to the correct replica and return the correct response.
    """
    config_file = os.path.join(
        os.path.dirname(__file__),
        "test_config_files",
        "deploy_grpc_app.yaml",
    )

    subprocess.check_output(["serve", "deploy", config_file], stderr=subprocess.STDOUT)

    app1 = "app1"
    app_names = [app1]

    channel = grpc.insecure_channel(get_application_url("gRPC", app_name=app1))

    # Ensures ListApplications method succeeding.
    wait_for_condition(
        ping_grpc_list_applications, channel=channel, app_names=app_names
    )

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)

    # Ensures a custom defined method is responding correctly.
    ping_grpc_call_method(channel, app1)

    # Ensures another custom defined method is responding correctly.
    ping_grpc_another_method(channel, app1)

    # Ensures model multiplexing is responding correctly.
    ping_grpc_model_multiplexing(channel, app1)

    # Ensure Streaming method is responding correctly.
    ping_grpc_streaming(channel, app1)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_grpc_proxy_model_composition(ray_start_stop):
    """Test serving request through gRPC proxy

    When Serve runs with a gRPC deployment, the app should be deployed successfully,
    both ListApplications and Healthz methods returning success response, and model
    composition should work correctly.
    """
    config_file = os.path.join(
        os.path.dirname(__file__),
        "test_config_files",
        "deploy_grpc_model_composition.yaml",
    )

    subprocess.check_output(["serve", "deploy", config_file], stderr=subprocess.STDOUT)

    app = "app1"
    app_names = [app]

    channel = grpc.insecure_channel(get_application_url("gRPC", app_name=app))

    # Ensures ListApplications method succeeding.
    wait_for_condition(
        ping_grpc_list_applications, channel=channel, app_names=app_names
    )

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)

    # Ensure model composition is responding correctly.
    ping_fruit_stand(channel, app)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
