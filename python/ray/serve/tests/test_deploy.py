import os
import sys
import time
from collections import defaultdict

import pytest
import requests

import ray
from ray import serve
from ray._private.pydantic_compat import ValidationError
from ray._private.test_utils import SignalActor
from ray.serve._private.utils import get_random_letters
from ray.serve.exceptions import RayServeException


@pytest.mark.parametrize("use_handle", [True, False])
def test_deploy_basic(serve_instance, use_handle):
    """Test basic serve.run().

    1. Deploy an application with one deployment `d`.
    2. Redeploy `d` and check that a new actor is started.
    3. Update the code for `d`, redeploy `d`, and check that a new actor
        is started with the new code.
    """

    @serve.deployment
    def d():
        return "code version 1", os.getpid()

    def call():
        if use_handle:
            handle = serve.get_deployment_handle("d", "default")
            return handle.remote().result()
        else:
            return requests.get("http://localhost:8000/d").json()

    serve.run(d.bind())
    resp, pid1 = call()
    assert resp == "code version 1"

    # Redeploying should start a new actor.
    serve.run(d.bind())
    resp, pid2 = call()
    assert resp == "code version 1"
    assert pid2 != pid1

    # Redeploying with new code should start a new actor with new code
    @serve.deployment
    def d():
        return "code version 2", os.getpid()

    serve.run(d.bind())
    resp, pid3 = call()
    assert resp == "code version 2"
    assert pid3 != pid2


def test_empty_decorator(serve_instance):
    @serve.deployment
    def func(*args):
        return "hi"

    @serve.deployment
    class Class:
        def ping(self, *args):
            return "pong"

    assert func.name == "func"
    assert Class.name == "Class"
    func_handle = serve.run(func.bind())
    assert ray.get(func_handle.remote()) == "hi"

    class_handle = serve.run(Class.bind())
    assert ray.get(class_handle.ping.remote()) == "pong"


def test_reconfigure_with_exception(serve_instance):
    @serve.deployment
    class A:
        def __init__(self):
            self.config = "yoo"

        def reconfigure(self, config):
            if config == "hi":
                raise Exception("oops")

            self.config = config

        def __call__(self, *args):
            return self.config

    with pytest.raises(ValidationError):
        serve.run(A.options(user_config="hi").bind())


@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_single_replica(serve_instance, use_handle):
    # Tests that redeploying a deployment with a single replica waits for the
    # replica to completely shut down before starting a new one.
    client = serve_instance

    name = "test"

    @ray.remote
    def call(block=False):
        if use_handle:
            handle = serve.get_deployment_handle(name, "app")
            ret = handle.handler.remote(block).result()
        else:
            ret = requests.get(
                f"http://localhost:8000/{name}", params={"block": block}
            ).text

        return ret.split("|")[0], ret.split("|")[1]

    signal_name = f"signal-{get_random_letters()}"
    signal = SignalActor.options(name=signal_name).remote()

    @serve.deployment(name=name, version="1")
    class V1:
        async def handler(self, block: bool):
            if block:
                signal = ray.get_actor(signal_name)
                await signal.wait.remote()

            return f"1|{os.getpid()}"

        async def __call__(self, request):
            return await self.handler(request.query_params["block"] == "True")

    class V2:
        async def handler(self, *args):
            return f"2|{os.getpid()}"

        async def __call__(self, request):
            return await self.handler()

    serve.run(V1.bind(), name="app")
    ref1 = call.remote(block=False)
    val1, pid1 = ray.get(ref1)
    assert val1 == "1"

    # ref2 will block until the signal is sent.
    ref2 = call.remote(block=True)
    assert len(ray.wait([ref2], timeout=2.1)[0]) == 0

    # Redeploy new version. This should not go through until the old version
    # replica completely stops.
    V2 = V1.options(func_or_class=V2, version="2")
    serve.run(V2.bind(), _blocking=False, name="app")
    with pytest.raises(TimeoutError):
        client._wait_for_application_running("app", timeout_s=0.1)

    # It may take some time for the handle change to propagate and requests
    # to get sent to the new version. Repeatedly send requests until they
    # start blocking
    start = time.time()
    new_version_ref = None
    while time.time() - start < 30:
        ready, not_ready = ray.wait([call.remote(block=False)], timeout=5)
        if len(ready) == 1:
            # If the request doesn't block, it must have been the old version.
            val, pid = ray.get(ready[0])
            assert val == "1"
            assert pid == pid1
        elif len(not_ready) == 1:
            # If the request blocks, it must have been the new version.
            new_version_ref = not_ready[0]
            break
    else:
        assert False, "Timed out waiting for new version to be called."

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val2, pid2 = ray.get(ref2)
    assert val2 == "1"
    assert pid2 == pid1

    # Now the goal and request to the new version should complete.
    client._wait_for_application_running("app")
    new_version_val, new_version_pid = ray.get(new_version_ref)
    assert new_version_val == "2"
    assert new_version_pid != pid2


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_multiple_replicas(serve_instance, use_handle):
    # Tests that redeploying a deployment with multiple replicas performs
    # a rolling update.
    client = serve_instance

    name = "test"

    @ray.remote(num_cpus=0)
    def call(block=False):
        if use_handle:
            handle = serve.get_deployment_handle(name, "app")
            ret = handle.handler.remote(block).result()
        else:
            ret = requests.get(
                f"http://localhost:8000/{name}", params={"block": block}
            ).text

        return ret.split("|")[0], ret.split("|")[1]

    signal_name = f"signal-{get_random_letters()}"
    signal = SignalActor.options(name=signal_name).remote()

    @serve.deployment(name=name, version="1", num_replicas=2)
    class V1:
        async def handler(self, block: bool):
            if block:
                signal = ray.get_actor(signal_name)
                await signal.wait.remote()

            return f"1|{os.getpid()}"

        async def __call__(self, request):
            return await self.handler(request.query_params["block"] == "True")

    class V2:
        async def handler(self, *args):
            return f"2|{os.getpid()}"

        async def __call__(self, request):
            return await self.handler()

    def make_nonblocking_calls(expected, expect_blocking=False):
        # Returns dict[val, set(pid)].
        blocking = []
        responses = defaultdict(set)
        start = time.time()
        while time.time() - start < 30:
            refs = [call.remote(block=False) for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=5)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)
            for ref in not_ready:
                blocking.extend(not_ready)

            if all(len(responses[val]) == num for val, num in expected.items()) and (
                expect_blocking is False or len(blocking) > 0
            ):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses, blocking

    serve.run(V1.bind(), name="app")
    responses1, _ = make_nonblocking_calls({"1": 2})
    pids1 = responses1["1"]

    # ref2 will block a single replica until the signal is sent. Check that
    # some requests are now blocking.
    ref2 = call.remote(block=True)
    responses2, blocking2 = make_nonblocking_calls({"1": 1}, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    # Redeploy new version. Since there is one replica blocking, only one new
    # replica should be started up.
    V2 = V1.options(func_or_class=V2, version="2")
    serve.run(V2.bind(), _blocking=False, name="app")
    with pytest.raises(TimeoutError):
        client._wait_for_application_running("app", timeout_s=0.1)
    responses3, blocking3 = make_nonblocking_calls({"1": 1}, expect_blocking=True)

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val, pid = ray.get(ref2)
    assert val == "1"
    assert pid in responses1["1"]

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    client._wait_for_application_running("app")
    make_nonblocking_calls({"2": 2})


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("use_handle", [True, False])
def test_reconfigure_multiple_replicas(serve_instance, use_handle):
    # Tests that updating the user_config with multiple replicas performs a
    # rolling update.
    client = serve_instance

    name = "test"

    @ray.remote(num_cpus=0)
    def call():
        if use_handle:
            handle = serve.get_deployment_handle(name, "app")
            ret = handle.handler.remote().result()
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    signal_name = f"signal-{get_random_letters()}"
    signal = SignalActor.options(name=signal_name).remote()

    @serve.deployment(name=name, version="1", num_replicas=2)
    class V1:
        def __init__(self):
            self.config = None

        async def reconfigure(self, config):
            # Don't block when the replica is first created.
            if self.config is not None:
                signal = ray.get_actor(signal_name)
                ray.get(signal.wait.remote())
            self.config = config

        async def handler(self):
            val = self.config["test"]
            return f"{val}|{os.getpid()}"

        async def __call__(self, request):
            return await self.handler()

    def make_nonblocking_calls(expected, expect_blocking=False):
        # Returns dict[val, set(pid)].
        blocking = []
        responses = defaultdict(set)
        start = time.time()
        while time.time() - start < 30:
            refs = [call.remote() for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=5)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)
            for ref in not_ready:
                blocking.extend(not_ready)

            if all(len(responses[val]) == num for val, num in expected.items()) and (
                expect_blocking is False or len(blocking) > 0
            ):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses, blocking

    serve.run(V1.options(user_config={"test": "1"}).bind(), name="app")
    responses1, _ = make_nonblocking_calls({"1": 2})
    pids1 = responses1["1"]

    # Reconfigure should block one replica until the signal is sent. Check that
    # some requests are now blocking.
    serve.run(V1.options(user_config={"test": "2"}).bind(), name="app", _blocking=False)
    responses2, blocking2 = make_nonblocking_calls({"1": 1}, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    # Signal reconfigure to finish. Now the goal should complete and both
    # replicas should have the updated config.
    ray.get(signal.send.remote())
    client._wait_for_application_running("app")
    make_nonblocking_calls({"2": 2})


def test_reconfigure_with_queries(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment(max_concurrent_queries=10, num_replicas=3)
    class A:
        def __init__(self):
            self.state = None

        def reconfigure(self, config):
            self.state = config

        async def __call__(self):
            await signal.wait.remote()
            return self.state["a"]

    handle = serve.run(A.options(version="1", user_config={"a": 1}).bind())
    refs = []
    for _ in range(30):
        refs.append(handle.remote())

    @ray.remote(num_cpus=0)
    def reconfigure():
        serve.run(A.options(version="1", user_config={"a": 2}).bind())

    reconfigure_ref = reconfigure.remote()
    signal.send.remote()
    ray.get(reconfigure_ref)
    for ref in refs:
        assert ray.get(ref) == 1
    assert ray.get(handle.remote()) == 2


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_scale_down(serve_instance, use_handle):
    # Tests redeploying with a new version and lower num_replicas.
    name = "test"

    @serve.deployment(name=name, version="1", num_replicas=4)
    def v1(*args):
        return f"1|{os.getpid()}"

    @ray.remote(num_cpus=0)
    def call():
        if use_handle:
            handle = serve.get_app_handle("app")
            ret = handle.remote().result()
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    def make_calls(expected):
        # Returns dict[val, set(pid)].
        responses = defaultdict(set)
        start = time.time()
        while time.time() - start < 30:
            refs = [call.remote() for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=5)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)

            if all(len(responses[val]) == num for val, num in expected.items()):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses

    serve.run(v1.bind(), name="app")
    responses1 = make_calls({"1": 4})
    pids1 = responses1["1"]

    @serve.deployment(name=name, version="2", num_replicas=2)
    def v2(*args):
        return f"2|{os.getpid()}"

    serve.run(v2.bind(), name="app")
    responses2 = make_calls({"2": 2})
    assert all(pid not in pids1 for pid in responses2["2"])


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_scale_up(serve_instance, use_handle):
    # Tests redeploying with a new version and higher num_replicas.
    name = "test"

    @serve.deployment(name=name, version="1", num_replicas=2)
    def v1(*args):
        return f"1|{os.getpid()}"

    @ray.remote(num_cpus=0)
    def call():
        if use_handle:
            handle = serve.get_app_handle("app")
            ret = handle.remote().result()
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    def make_calls(expected):
        # Returns dict[val, set(pid)].
        responses = defaultdict(set)
        start = time.time()
        while time.time() - start < 30:
            refs = [call.remote() for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=5)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)

            if all(len(responses[val]) == num for val, num in expected.items()):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses

    serve.run(v1.bind(), name="app")
    responses1 = make_calls({"1": 2})
    pids1 = responses1["1"]

    @serve.deployment(name=name, version="2", num_replicas=4)
    def v2(*args):
        return f"2|{os.getpid()}"

    serve.run(v2.bind(), name="app")
    responses2 = make_calls({"2": 4})
    assert all(pid not in pids1 for pid in responses2["2"])


def test_deploy_handle_validation(serve_instance):
    @serve.deployment
    class A:
        def b(self, *args):
            return "hello"

    handle = serve.run(A.bind(), name="app")

    # Legacy code path
    assert ray.get(handle.options(method_name="b").remote()) == "hello"
    # New code path
    assert ray.get(handle.b.remote()) == "hello"
    with pytest.raises(RayServeException):
        ray.get(handle.c.remote())


def test_deploy_with_init_args(serve_instance):
    @serve.deployment()
    class D:
        def __init__(self, *args):
            self._args = args

        def get_args(self):
            return self._args

    handle = serve.run(D.bind(1, 2, 3)).options(use_new_handle_api=True)
    assert handle.get_args.remote().result() == (1, 2, 3)


def test_deploy_with_init_kwargs(serve_instance):
    @serve.deployment()
    class D:
        def __init__(self, **kwargs):
            self._kwargs = kwargs

        def get_kwargs(self, *args):
            return self._kwargs

    handle = serve.run(D.bind(a=1, b=2)).options(use_new_handle_api=True)
    assert handle.get_kwargs.remote().result() == {"a": 1, "b": 2}


def test_init_args_with_closure(serve_instance):
    @serve.deployment
    class Evaluator:
        def __init__(self, func):
            self.func = func

        def __call__(self, inp):
            return self.func(inp)

    handle = serve.run(Evaluator.bind(lambda a: a + 1))
    assert ray.get(handle.remote(41)) == 42


def test_input_validation():
    name = "test"

    @serve.deployment(name=name)
    class Base:
        pass

    with pytest.raises(RuntimeError):
        Base()

    with pytest.raises(TypeError):

        @serve.deployment(name=name, version=1)
        class BadVersion:
            pass

    with pytest.raises(TypeError):
        Base.options(version=1)

    with pytest.raises(ValidationError):

        @serve.deployment(num_replicas="hi")
        class BadNumReplicas:
            pass

    with pytest.raises(ValidationError):
        Base.options(num_replicas="hi")

    with pytest.raises(ValueError):

        @serve.deployment(num_replicas=0)
        class ZeroNumReplicas:
            pass

    with pytest.raises(ValueError):
        Base.options(num_replicas=0)

    with pytest.raises(ValidationError):

        @serve.deployment(num_replicas=-1)
        class NegativeNumReplicas:
            pass

    with pytest.raises(ValidationError):
        Base.options(num_replicas=-1)

    with pytest.raises(TypeError):

        @serve.deployment(init_args={1, 2, 3})
        class BadInitArgs:
            pass

    with pytest.raises(TypeError):
        Base.options(init_args="hi")

    with pytest.raises(TypeError):

        @serve.deployment(ray_actor_options=[1, 2, 3])
        class BadActorOpts:
            pass

    with pytest.raises(TypeError):
        Base.options(ray_actor_options="hi")

    with pytest.raises(ValidationError):

        @serve.deployment(max_concurrent_queries="hi")
        class BadMaxQueries:
            pass

    with pytest.raises(ValidationError):
        Base.options(max_concurrent_queries=[1])

    with pytest.raises(ValueError):

        @serve.deployment(max_concurrent_queries=0)
        class ZeroMaxQueries:
            pass

    with pytest.raises(ValueError):
        Base.options(max_concurrent_queries=0)

    with pytest.raises(ValueError):

        @serve.deployment(max_concurrent_queries=-1)
        class NegativeMaxQueries:
            pass

    with pytest.raises(ValueError):
        Base.options(max_concurrent_queries=-1)


def test_deployment_properties():
    class DClass:
        pass

    D = serve.deployment(
        name="name",
        init_args=("hello", 123),
        version="version",
        num_replicas=2,
        user_config="hi",
        max_concurrent_queries=100,
        route_prefix="/hello",
        ray_actor_options={"num_cpus": 2},
    )(DClass)

    assert D.name == "name"
    assert D.init_args == ("hello", 123)
    assert D.version == "version"
    assert D.num_replicas == 2
    assert D.user_config == "hi"
    assert D.max_concurrent_queries == 100
    assert D.route_prefix == "/hello"
    assert D.ray_actor_options == {"num_cpus": 2}

    D = serve.deployment(
        version=None,
        route_prefix=None,
    )(DClass)
    assert D.version is None
    assert D.route_prefix is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
