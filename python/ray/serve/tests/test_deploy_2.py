from concurrent.futures.thread import ThreadPoolExecutor
import functools
import os
import sys
import time

import pytest
import requests

import ray
from ray._private.test_utils import SignalActor, wait_for_condition
from ray import serve

from ray.serve.api import deploy_group


class TestGetDeployment:
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
    @serve.deployment
    def f(*args):
        return "got me"

    for prefix in prefixes:
        f.options(route_prefix=prefix).deploy()
        if prefix is None:
            assert requests.get("http://localhost:8000/f").status_code == 404
        else:
            assert requests.get("http://localhost:8000/f").text == "got me"
        assert ray.get(f.get_handle().remote()) == "got me"


@pytest.mark.timeout(10, method="thread")
def test_deploy_empty_bundle(serve_instance):
    @serve.deployment(ray_actor_options={"num_cpus": 0})
    class D:
        def hello(self, _):
            return "hello"

    # This should succesfully terminate within the provided time-frame.
    D.deploy()


def test_deployment_error_handling(serve_instance):
    @serve.deployment
    def f():
        pass

    with pytest.raises(RuntimeError, match=". is not a valid URI"):
        # This is an invalid configuration since dynamic upload of working
        # directories is not supported. The error this causes in the controller
        # code should be caught and reported back to the `deploy` caller.

        f.options(ray_actor_options={"runtime_env": {"working_dir": "."}}).deploy()


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

    A.deploy()

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


class TestDeployGroup:
    @serve.deployment
    def f():
        return "f reached"

    @serve.deployment
    def g():
        return "g reached"

    @serve.deployment
    class C:
        async def __call__(self):
            return "C reached"

    @serve.deployment
    class D:
        async def __call__(self):
            return "D reached"

    def deploy_and_check_responses(
        self, deployments, responses, blocking=True, client=None
    ):
        """
        Helper function that deploys the list of deployments, calls them with
        their handles, and checks whether they return the objects in responses.
        If blocking is False, this function uses a non-blocking deploy and uses
        the client to wait until the deployments finish deploying.
        """

        deploy_group(deployments, _blocking=blocking)

        def check_all_deployed():
            try:
                for deployment, response in zip(deployments, responses):
                    if ray.get(deployment.get_handle().remote()) != response:
                        return False
            except Exception:
                return False

            return True

        if blocking:
            # If blocking, this should be guaranteed to pass immediately.
            assert check_all_deployed()
        else:
            # If non-blocking, this should pass eventually.
            wait_for_condition(check_all_deployed)

    def test_basic_deploy_group(self, serve_instance):
        """
        Atomically deploys a group of deployments, including both functions and
        classes. Checks whether they deploy correctly.
        """

        deployments = [self.f, self.g, self.C, self.D]
        responses = ["f reached", "g reached", "C reached", "D reached"]

        self.deploy_and_check_responses(deployments, responses)

    def test_non_blocking_deploy_group(self, serve_instance):
        """Checks deploy_group's behavior when _blocking=False."""

        deployments = [self.f, self.g, self.C, self.D]
        responses = ["f reached", "g reached", "C reached", "D reached"]
        self.deploy_and_check_responses(
            deployments, responses, blocking=False, client=serve_instance
        )

    def test_mutual_handles(self, serve_instance):
        """
        Atomically deploys a group of deployments that get handles to other
        deployments in the group inside their __init__ functions. The handle
        references should fail in a non-atomic deployment. Checks whether the
        deployments deploy correctly.
        """

        @serve.deployment
        class MutualHandles:
            async def __init__(self, handle_name):
                self.handle = serve.get_deployment(handle_name).get_handle()

            async def __call__(self, echo: str):
                return await self.handle.request_echo.remote(echo)

            async def request_echo(self, echo: str):
                return echo

        names = []
        for i in range(10):
            names.append("a" * i)

        deployments = []
        for idx in range(len(names)):
            # Each deployment will hold a ServeHandle with the next name in
            # the list
            deployment_name = names[idx]
            handle_name = names[(idx + 1) % len(names)]

            deployments.append(
                MutualHandles.options(name=deployment_name, init_args=(handle_name,))
            )

        deploy_group(deployments)

        for deployment in deployments:
            assert (ray.get(deployment.get_handle().remote("hello"))) == "hello"

    def test_decorated_deployments(self, serve_instance):
        """
        Checks deploy_group's behavior when deployments have options set in
        their @serve.deployment decorator.
        """

        @serve.deployment(num_replicas=2, max_concurrent_queries=5)
        class DecoratedClass1:
            async def __call__(self):
                return "DecoratedClass1 reached"

        @serve.deployment(num_replicas=4, max_concurrent_queries=2)
        class DecoratedClass2:
            async def __call__(self):
                return "DecoratedClass2 reached"

        deployments = [DecoratedClass1, DecoratedClass2]
        responses = ["DecoratedClass1 reached", "DecoratedClass2 reached"]
        self.deploy_and_check_responses(deployments, responses)

    def test_empty_list(self, serve_instance):
        """Checks deploy_group's behavior when deployment group is empty."""

        self.deploy_and_check_responses([], [])

    def test_invalid_input(self, serve_instance):
        """
        Checks deploy_group's behavior when deployment group contains
        non-Deployment objects.
        """

        with pytest.raises(TypeError):
            deploy_group([self.f, self.C, "not a Deployment object"])

    def test_import_path_deployment(self, serve_instance):
        test_env_uri = (
            "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        )
        test_module_uri = (
            "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        )

        ray_actor_options = {
            "runtime_env": {"py_modules": [test_env_uri, test_module_uri]}
        }

        shallow = serve.deployment(
            name="shallow",
            ray_actor_options=ray_actor_options,
        )("test_env.shallow_import.ShallowClass")

        deep = serve.deployment(
            name="deep",
            ray_actor_options=ray_actor_options,
        )("test_env.subdir1.subdir2.deep_import.DeepClass")

        one = serve.deployment(
            name="one",
            ray_actor_options=ray_actor_options,
        )("test_module.test.one")

        deployments = [shallow, deep, one]
        responses = ["Hello shallow world!", "Hello deep world!", 2]

        self.deploy_and_check_responses(deployments, responses)

    def test_different_pymodules(self, serve_instance):
        test_env_uri = (
            "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        )
        test_module_uri = (
            "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        )

        shallow = serve.deployment(
            name="shallow",
            ray_actor_options={"runtime_env": {"py_modules": [test_env_uri]}},
        )("test_env.shallow_import.ShallowClass")

        one = serve.deployment(
            name="one",
            ray_actor_options={"runtime_env": {"py_modules": [test_module_uri]}},
        )("test_module.test.one")

        deployments = [shallow, one]
        responses = ["Hello shallow world!", 2]

        self.deploy_and_check_responses(deployments, responses)

    def test_import_path_deployment_decorated(self, serve_instance):
        func = serve.deployment(
            name="decorated_func",
        )("ray.serve.tests.test_deploy.decorated_func")

        clss = serve.deployment(
            name="decorated_clss",
        )("ray.serve.tests.test_deploy.DecoratedClass")

        deployments = [func, clss]
        responses = ["got decorated func", "got decorated class"]

        self.deploy_and_check_responses(deployments, responses)

        # Check that non-default decorated values were overwritten
        assert serve.get_deployment("decorated_func").max_concurrent_queries != 17
        assert serve.get_deployment("decorated_clss").max_concurrent_queries != 17


# Decorated function with non-default max_concurrent queries
@serve.deployment(max_concurrent_queries=17)
def decorated_func(req=None):
    return "got decorated func"


# Decorated class with non-default max_concurrent queries
@serve.deployment(max_concurrent_queries=17)
class DecoratedClass:
    def __call__(self, req=None):
        return "got decorated class"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
