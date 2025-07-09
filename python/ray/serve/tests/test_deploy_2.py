import functools
import os
import sys
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentStatus
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve._private.test_utils import (
    check_deployment_status,
    check_num_replicas_eq,
    get_application_url,
)
from ray.serve._private.utils import get_component_file_name
from ray.serve.schema import ApplicationStatus
from ray.util.state import list_actors


def test_deploy_zero_cpus(serve_instance):
    @serve.deployment(ray_actor_options={"num_cpus": 0})
    class D:
        def hello(self):
            return "hello"

    h = serve.run(D.bind())
    assert h.hello.remote().result() == "hello"


def test_deployment_error_handling(serve_instance):
    @serve.deployment
    def f():
        pass

    with pytest.raises(RuntimeError):
        # This is an invalid configuration since dynamic upload of working
        # directories is not supported. The error this causes in the controller
        # code should be caught and reported back to the `deploy` caller.

        serve.run(
            f.options(ray_actor_options={"runtime_env": {"working_dir": "."}}).bind()
        )


def test_json_serialization_user_config(serve_instance):
    """See https://github.com/ray-project/ray/issues/25345.

    See https://github.com/ray-project/ray/pull/26235 for additional context
    about this test.
    """

    @serve.deployment(name="simple-deployment")
    class SimpleDeployment:
        value: str
        nested_value: str

        def reconfigure(self, config: Dict) -> None:
            self.value = config["value"]
            self.nested_value = config["nested"]["value"]

        def get_value(self) -> None:
            return self.value

        def get_nested_value(self) -> None:
            return self.nested_value

    app = SimpleDeployment.options(
        user_config={
            "value": "Success!",
            "nested": {"value": "Success!"},
        }
    ).bind()
    handle = serve.run(app)

    assert handle.get_value.remote().result() == "Success!"
    assert handle.get_nested_value.remote().result() == "Success!"

    handle = serve.run(
        SimpleDeployment.options(
            user_config={
                "value": "Failure!",
                "another-value": "Failure!",
                "nested": {"value": "Success!"},
            }
        ).bind()
    )

    assert handle.get_value.remote().result() == "Failure!"
    assert handle.get_nested_value.remote().result() == "Success!"


def test_http_proxy_request_cancellation(serve_instance):
    # https://github.com/ray-project/ray/issues/21425
    s = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class A:
        def __init__(self) -> None:
            self.counter = 0

        async def __call__(self):
            self.counter += 1
            ret_val = self.counter
            await s.wait.remote()
            return ret_val

    serve.run(A.bind())

    url = get_application_url("HTTP") + "/A"
    with ThreadPoolExecutor() as pool:
        # Send the first request, it should block for the result
        first_blocking_fut = pool.submit(functools.partial(httpx.get, url, timeout=100))
        time.sleep(1)
        assert not first_blocking_fut.done()

        # Send more requests, these should be queued in handle.
        # But because first request is hanging and these have low timeout.
        # They should all disconnect from http connection.
        # These requests should never reach the replica.
        rest_blocking_futs = [
            pool.submit(functools.partial(httpx.get, url, timeout=0.5))
            for _ in range(3)
        ]
        time.sleep(1)
        assert all(f.done() for f in rest_blocking_futs)

        # Now unblock the first request.
        ray.get(s.send.remote())
        assert first_blocking_fut.result().text == "1"

    # Sending another request to verify that only one request has been
    # processed so far.
    assert httpx.get(url).text == "2"


def test_nonserializable_deployment(serve_instance):
    lock = threading.Lock()

    class D:
        def hello(self, _):
            return lock

    # Check that the `inspect_serializability` trace was printed
    with pytest.raises(
        TypeError,
        match=r"Could not serialize the deployment[\s\S]*was found to be non-serializable.*",  # noqa
    ):
        serve.deployment(D)

    @serve.deployment
    class E:
        def __init__(self, arg):
            self.arg = arg

    with pytest.raises(TypeError, match="pickle"):
        serve.run(E.bind(lock))

    with pytest.raises(TypeError, match="pickle"):
        serve.run(E.bind(arg=lock))


def test_deploy_application_unhealthy(serve_instance):
    """Test deploying an application that becomes unhealthy."""

    @ray.remote
    class Event:
        def __init__(self):
            self.is_set = False

        def set(self):
            self.is_set = True

        def is_set(self):
            return self.is_set

    event = Event.remote()

    @serve.deployment(health_check_period_s=1, health_check_timeout_s=3)
    class Model:
        def __call__(self):
            return "hello world"

        def check_health(self):
            if ray.get(event.is_set.remote()):
                raise RuntimeError("Intentionally failing.")

    handle = serve.run(Model.bind(), name="app")
    assert handle.remote().result() == "hello world"
    assert serve.status().applications["app"].status == ApplicationStatus.RUNNING

    # When a deployment becomes unhealthy, application should transition -> UNHEALTHY
    event.set.remote()
    wait_for_condition(
        lambda: serve.status().applications["app"].status == ApplicationStatus.UNHEALTHY
    )

    # Check that application stays unhealthy
    for _ in range(10):
        assert serve.status().applications["app"].status == ApplicationStatus.UNHEALTHY
        time.sleep(0.1)

    # At least 10 control loop iterations should have passed. Check that
    # the logs from application state manager notifying about unhealthy
    # deployments doesn't spam, they should get printed only once.
    controller_pid = [
        actor["pid"]
        for actor in list_actors()
        if actor["name"] == "SERVE_CONTROLLER_ACTOR"
    ][0]
    controller_log_file_name = get_component_file_name(
        "controller", controller_pid, component_type=None, suffix=".log"
    )
    controller_log_path = os.path.join(get_serve_logs_dir(), controller_log_file_name)
    with open(controller_log_path, "r") as f:
        s = f.read()
        assert s.count("The deployments ['Model'] are UNHEALTHY.") <= 1


@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env support experimental on windows"
)
def test_deploy_bad_pip_package_deployment(serve_instance):
    """Test deploying with a bad runtime env at deployment level."""

    @serve.deployment(ray_actor_options={"runtime_env": {"pip": ["does_not_exist"]}})
    class Model:
        def __call__(self):
            return "hello world"

    serve._run(Model.bind(), _blocking=False)

    def check_fail():
        app_status = serve.status().applications["default"]
        assert app_status.status == ApplicationStatus.DEPLOY_FAILED
        deployment_message = app_status.deployments["Model"].message
        assert "No matching distribution found for does_not_exist" in deployment_message
        return True

    # TODO: Figure out why timeout 30 is needed instead of 15 or lower the timeout to 15.
    wait_for_condition(check_fail, timeout=30)


def test_deploy_same_deployment_name_different_app(serve_instance):
    @serve.deployment
    class Model:
        def __init__(self, name):
            self.name = name

        def __call__(self):
            return f"hello {self.name}"

    serve.run(Model.bind("alice"), name="app1", route_prefix="/app1")
    serve.run(Model.bind("bob"), name="app2", route_prefix="/app2")

    url = get_application_url("HTTP", app_name="app1")
    assert httpx.get(f"{url}").text == "hello alice"
    proxy_url = "http://localhost:8000/-/routes"
    routes = httpx.get(proxy_url).json()
    assert routes["/app1"] == "app1"

    url = get_application_url("HTTP", app_name="app2")
    assert httpx.get(f"{url}").text == "hello bob"
    routes = httpx.get(proxy_url).json()
    assert routes["/app2"] == "app2"

    app1_status = serve.status().applications["app1"]
    app2_status = serve.status().applications["app2"]
    assert app1_status.status == "RUNNING"
    assert app1_status.deployments["Model"].status == "HEALTHY"
    assert app2_status.status == "RUNNING"
    assert app2_status.deployments["Model"].status == "HEALTHY"


@pytest.mark.parametrize("use_options", [True, False])
def test_num_replicas_auto_api(serve_instance, use_options):
    """Test setting only `num_replicas="auto"`."""

    signal = SignalActor.remote()

    class A:
        async def __call__(self):
            await signal.wait.remote()

    if use_options:
        A = serve.deployment(A).options(num_replicas="auto")
    else:
        A = serve.deployment(num_replicas="auto")(A)

    serve.run(A.bind(), name="default")
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    check_num_replicas_eq("A", 1)

    app_details = serve_instance.get_serve_details()["applications"]["default"]
    deployment_config = app_details["deployments"]["A"]["deployment_config"]
    assert "num_replicas" not in deployment_config
    assert deployment_config["max_ongoing_requests"] == 5
    assert deployment_config["autoscaling_config"] == {
        # Set by `num_replicas="auto"`
        "target_ongoing_requests": 2.0,
        "min_replicas": 1,
        "max_replicas": 100,
        # Untouched defaults
        "metrics_interval_s": 10.0,
        "upscale_delay_s": 30.0,
        "look_back_period_s": 30.0,
        "downscale_delay_s": 600.0,
        "upscale_smoothing_factor": None,
        "downscale_smoothing_factor": None,
        "upscaling_factor": None,
        "downscaling_factor": None,
        "smoothing_factor": 1.0,
        "initial_replicas": None,
    }


@pytest.mark.parametrize("use_options", [True, False])
def test_num_replicas_auto_basic(serve_instance, use_options):
    """Test `num_replicas="auto"` and the defaults are used by autoscaling."""

    signal = SignalActor.remote()

    class A:
        async def __call__(self):
            await signal.wait.remote()

    if use_options:
        A = serve.deployment(A).options(
            num_replicas="auto",
            autoscaling_config={"metrics_interval_s": 1, "upscale_delay_s": 1},
            graceful_shutdown_timeout_s=1,
        )
    else:
        A = serve.deployment(
            num_replicas="auto",
            autoscaling_config={"metrics_interval_s": 1, "upscale_delay_s": 1},
            graceful_shutdown_timeout_s=1,
        )(A)

    h = serve.run(A.bind(), name="default")
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    check_num_replicas_eq("A", 1)

    app_details = serve_instance.get_serve_details()["applications"]["default"]
    deployment_config = app_details["deployments"]["A"]["deployment_config"]
    assert "num_replicas" not in deployment_config
    assert deployment_config["max_ongoing_requests"] == 5
    assert deployment_config["autoscaling_config"] == {
        # Set by `num_replicas="auto"`
        "target_ongoing_requests": 2.0,
        "min_replicas": 1,
        "max_replicas": 100,
        # Overrided by `autoscaling_config`
        "metrics_interval_s": 1.0,
        "upscale_delay_s": 1.0,
        # Untouched defaults
        "look_back_period_s": 30.0,
        "downscale_delay_s": 600.0,
        "upscale_smoothing_factor": None,
        "downscale_smoothing_factor": None,
        "upscaling_factor": None,
        "downscaling_factor": None,
        "smoothing_factor": 1.0,
        "initial_replicas": None,
    }

    for i in range(3):
        [h.remote() for _ in range(2)]

        def check_num_waiters(target: int):
            assert ray.get(signal.cur_num_waiters.remote()) == target
            return True

        wait_for_condition(check_num_waiters, target=2 * (i + 1))
        print(time.time(), f"Number of waiters on signal reached {2*(i+1)}.")
        wait_for_condition(check_num_replicas_eq, name="A", target=i + 1)
        print(time.time(), f"Confirmed number of replicas are at {i+1}.")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
