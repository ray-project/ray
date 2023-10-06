import functools
import os
import sys
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict

import pytest
import requests
from pydantic import ValidationError

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import ApplicationStatus
from ray.serve.drivers import DAGDriver


class TestGetDeployment:
    # Test V1 API get_deployment()
    def get_deployment(self, name, use_list_api):
        if use_list_api:
            return serve.list_deployments()[name]
        else:
            return serve.get_deployment(name)

    @pytest.mark.parametrize("use_list_api", [True, False])
    def test_basic_get(self, serve_instance, use_list_api):
        name = "test"

        @serve.deployment(name=name, version="1")
        def d(*args):
            return "1", os.getpid()

        with pytest.raises(KeyError):
            self.get_deployment(name, use_list_api)

        d.deploy()
        val1, pid1 = ray.get(d.get_handle().remote())
        assert val1 == "1"

        del d

        d2 = self.get_deployment(name, use_list_api)
        val2, pid2 = ray.get(d2.get_handle().remote())
        assert val2 == "1"
        assert pid2 == pid1

    @pytest.mark.parametrize("use_list_api", [True, False])
    def test_get_after_delete(self, serve_instance, use_list_api):
        name = "test"

        @serve.deployment(name=name, version="1")
        def d(*args):
            return "1", os.getpid()

        d.deploy()
        del d

        d2 = self.get_deployment(name, use_list_api)
        d2.delete()
        del d2

        with pytest.raises(KeyError):
            self.get_deployment(name, use_list_api)

    @pytest.mark.parametrize("use_list_api", [True, False])
    def test_deploy_new_version(self, serve_instance, use_list_api):
        name = "test"

        @serve.deployment(name=name, version="1")
        def d(*args):
            return "1", os.getpid()

        d.deploy()
        val1, pid1 = ray.get(d.get_handle().remote())
        assert val1 == "1"

        del d

        d2 = self.get_deployment(name, use_list_api)
        d2.options(version="2").deploy()
        val2, pid2 = ray.get(d2.get_handle().remote())
        assert val2 == "1"
        assert pid2 != pid1

    @pytest.mark.parametrize("use_list_api", [True, False])
    def test_deploy_empty_version(self, serve_instance, use_list_api):
        name = "test"

        @serve.deployment(name=name)
        def d(*args):
            return "1", os.getpid()

        d.deploy()
        val1, pid1 = ray.get(d.get_handle().remote())
        assert val1 == "1"

        del d

        d2 = self.get_deployment(name, use_list_api)
        d2.deploy()
        val2, pid2 = ray.get(d2.get_handle().remote())
        assert val2 == "1"
        assert pid2 != pid1

    @pytest.mark.parametrize("use_list_api", [True, False])
    def test_init_args(self, serve_instance, use_list_api):
        name = "test"

        @serve.deployment(name=name)
        class D:
            def __init__(self, val):
                self._val = val

            def __call__(self, *arg):
                return self._val, os.getpid()

        D.deploy("1")
        val1, pid1 = ray.get(D.get_handle().remote())
        assert val1 == "1"

        del D

        D2 = self.get_deployment(name, use_list_api)
        D2.deploy()
        val2, pid2 = ray.get(D2.get_handle().remote())
        assert val2 == "1"
        assert pid2 != pid1

        D2 = self.get_deployment(name, use_list_api)
        D2.deploy("2")
        val3, pid3 = ray.get(D2.get_handle().remote())
        assert val3 == "2"
        assert pid3 != pid2

    @pytest.mark.parametrize("use_list_api", [True, False])
    def test_scale_replicas(self, serve_instance, use_list_api):
        name = "test"

        @serve.deployment(name=name)
        def d(*args):
            return os.getpid()

        def check_num_replicas(num):
            handle = self.get_deployment(name, use_list_api).get_handle()
            assert len(set(ray.get([handle.remote() for _ in range(50)]))) == num

        d.deploy()
        check_num_replicas(1)
        del d

        d2 = self.get_deployment(name, use_list_api)
        d2.options(num_replicas=2).deploy()
        check_num_replicas(2)


def test_list_deployments(serve_instance):
    assert serve.list_deployments() == {}

    @serve.deployment(name="hi", num_replicas=2)
    def d1(*args):
        pass

    d1.deploy()

    assert serve.list_deployments() == {"hi": d1}


def test_deploy_change_route_prefix(serve_instance):
    name = "test"

    @serve.deployment(name=name, version="1", route_prefix="/old")
    def d(*args):
        return f"1|{os.getpid()}"

    def call(route):
        ret = requests.get(f"http://localhost:8000/{route}").text
        return ret.split("|")[0], ret.split("|")[1]

    d.deploy()
    val1, pid1 = call("old")
    assert val1 == "1"

    # Check that the old route is gone and the response from the new route
    # has the same value and PID (replica wasn't restarted).
    def check_switched():
        try:
            print(call("old"))
            return False
        except Exception:
            print("failed")
            pass

        try:
            val2, pid2 = call("new")
        except Exception:
            return False

        assert val2 == "1"
        assert pid2 == pid1
        return True

    d.options(route_prefix="/new").deploy()
    wait_for_condition(check_switched)


@pytest.mark.parametrize("prefixes", [[None, "/f", None], ["/f", None, "/f"]])
def test_deploy_nullify_route_prefix(serve_instance, prefixes):
    # With multi dags support, dag driver will receive all route
    # prefix when route_prefix is "None", since "None" will be converted
    # to "/" internally.
    # Note: the expose http endpoint will still be removed for internal
    # dag node by setting "None" to route_prefix
    @serve.deployment
    def f(*args):
        return "got me"

    for prefix in prefixes:
        dag = DAGDriver.options(route_prefix=prefix).bind(f.bind())
        handle = serve.run(dag)
        assert requests.get("http://localhost:8000/f").status_code == 200
        assert requests.get("http://localhost:8000/f").text == '"got me"'
        assert ray.get(handle.predict.remote()) == "got me"


@pytest.mark.timeout(10, method="thread")
def test_deploy_empty_bundle(serve_instance):
    @serve.deployment(ray_actor_options={"num_cpus": 0})
    class D:
        def hello(self, _):
            return "hello"

    # This should succesfully terminate within the provided time-frame.
    serve.run(D.bind())


def test_deployment_error_handling(serve_instance):
    @serve.deployment
    def f():
        pass

    with pytest.raises(
        ValidationError, match="1 validation error for RayActorOptionsSchema.*"
    ):
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

    SimpleDeployment.options(
        user_config={
            "value": "Success!",
            "nested": {"value": "Success!"},
        }
    ).deploy()

    handle = SimpleDeployment.get_handle()
    assert ray.get(handle.get_value.remote()) == "Success!"
    assert ray.get(handle.get_nested_value.remote()) == "Success!"

    SimpleDeployment.options(
        user_config={
            "value": "Failure!",
            "another-value": "Failure!",
            "nested": {"value": "Success!"},
        }
    ).deploy()

    handle = SimpleDeployment.get_handle()
    assert ray.get(handle.get_value.remote()) == "Failure!"
    assert ray.get(handle.get_nested_value.remote()) == "Success!"


def test_http_proxy_request_cancellation(serve_instance):
    # https://github.com/ray-project/ray/issues/21425
    s = SignalActor.remote()

    @serve.deployment(max_concurrent_queries=1)
    class A:
        def __init__(self) -> None:
            self.counter = 0

        async def __call__(self):
            self.counter += 1
            ret_val = self.counter
            await s.wait.remote()
            return ret_val

    serve.run(A.bind())

    url = "http://127.0.0.1:8000/A"
    with ThreadPoolExecutor() as pool:
        # Send the first request, it should block for the result
        first_blocking_fut = pool.submit(
            functools.partial(requests.get, url, timeout=100)
        )
        time.sleep(1)
        assert not first_blocking_fut.done()

        # Send more requests, these should be queued in handle.
        # But because first request is hanging and these have low timeout.
        # They should all disconnect from http connection.
        # These requests should never reach the replica.
        rest_blocking_futs = [
            pool.submit(functools.partial(requests.get, url, timeout=0.5))
            for _ in range(3)
        ]
        time.sleep(1)
        assert all(f.done() for f in rest_blocking_futs)

        # Now unblock the first request.
        ray.get(s.send.remote())
        assert first_blocking_fut.result().text == "1"

    # Sending another request to verify that only one request has been
    # processed so far.
    assert requests.get(url).text == "2"


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
    assert ray.get(handle.remote()) == "hello world"
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


@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env support experimental on windows"
)
def test_deploy_bad_pip_package_deployment(serve_instance):
    """Test deploying with a bad runtime env at deployment level."""

    @serve.deployment(ray_actor_options={"runtime_env": {"pip": ["does_not_exist"]}})
    class Model:
        def __call__(self):
            return "hello world"

    serve.run(Model.bind(), _blocking=False)

    def check_fail():
        app_status = serve.status().applications["default"]
        assert app_status.status == ApplicationStatus.DEPLOY_FAILED
        deployment_message = app_status.deployments["Model"].message
        assert "No matching distribution found for does_not_exist" in deployment_message
        return True

    wait_for_condition(check_fail, timeout=15)


def test_deploy_same_deployment_name_different_app(serve_instance):
    @serve.deployment
    class Model:
        def __init__(self, name):
            self.name = name

        def __call__(self):
            return f"hello {self.name}"

    serve.run(Model.bind("alice"), name="app1", route_prefix="/app1")
    serve.run(Model.bind("bob"), name="app2", route_prefix="/app2")

    assert requests.get("http://localhost:8000/app1").text == "hello alice"
    assert requests.get("http://localhost:8000/app2").text == "hello bob"
    routes = requests.get("http://localhost:8000/-/routes").json()
    assert routes["/app1"] == "app1"
    assert routes["/app2"] == "app2"

    app1_status = serve.status().applications["app1"]
    app2_status = serve.status().applications["app2"]
    assert app1_status.status == "RUNNING"
    assert app1_status.deployments["Model"].status == "HEALTHY"
    assert app2_status.status == "RUNNING"
    assert app2_status.deployments["Model"].status == "HEALTHY"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
