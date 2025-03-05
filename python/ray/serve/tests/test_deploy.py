import os
import sys
import time
from collections import defaultdict
from typing import Callable

import pytest
import requests

import ray
from ray import serve
from ray._private.pydantic_compat import ValidationError
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS
from ray.serve._private.utils import get_random_string
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
    assert func_handle.remote().result() == "hi"

    class_handle = serve.run(Class.bind())
    assert class_handle.ping.remote().result() == "pong"


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

    with pytest.raises(RuntimeError):
        serve.run(A.options(user_config="hi").bind())


@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_single_replica(serve_instance, use_handle):
    """Tests redeploying a deployment with a single replica.

    The new replica should should start without waiting for the
    old version replica to completely shut down.
    """

    name = "test"

    @ray.remote
    def call():
        if use_handle:
            handle = serve.get_deployment_handle(name, "app")
            return handle.handler.remote().result()
        else:
            return requests.get("http://localhost:8000/").json()

    signal_name = f"signal-{get_random_string()}"
    signal = SignalActor.options(name=signal_name).remote()

    # V1 blocks on signal
    @serve.deployment(name=name)
    class V1:
        async def handler(self):
            await signal.wait.remote()
            return 1, os.getpid()

        async def __call__(self):
            return await self.handler()

    # V2 doesn't block on signal
    @serve.deployment(name=name)
    class V2:
        async def handler(self):
            return 2, os.getpid()

        async def __call__(self):
            return await self.handler()

    serve.run(V1.bind(), name="app")

    # Send unblocked signal first to get pid of running replica
    signal.send.remote()
    val1, pid1 = ray.get(call.remote())
    assert val1 == 1

    # blocked_ref will block until the signal is sent.
    signal.send.remote(clear=True)
    blocked_ref = call.remote()
    assert len(ray.wait([blocked_ref], timeout=2.1)[0]) == 0

    # Redeploy new version.
    serve._run(V2.bind(), _blocking=False, name="app")

    start = time.time()
    while time.time() - start < 30:
        ready, _ = ray.wait([call.remote()], timeout=2)
        if RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS:
            # If the request doesn't block, it must be V2 which doesn't wait
            # for signal. Otherwise, it must have been sent to V1 which
            # waits on signal The request might have been sent to V1 if the
            # long poll broadcast was delayed
            if len(ready) == 1:
                val, pid = ray.get(ready[0])
                assert val == 2
                assert pid != pid1
                break
        else:
            # Any requests that go through during this time should have
            # been sent to replicas of the old version
            if len(ready) == 1:
                val, pid = ray.get(ready[0])
                assert val == 1
                assert pid == pid1
            else:
                break
    else:
        assert False, "Timed out waiting for new version to be called."

    # Unblock blocked_ref
    ray.get(signal.send.remote())
    val2, pid2 = ray.get(blocked_ref)
    assert val2 == 1
    assert pid2 == pid1


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_redeploy_multiple_replicas(serve_instance):
    client = serve_instance
    name = "test"
    signal = SignalActor.remote()

    @serve.deployment(name=name, num_replicas=2)
    class V1:
        async def __call__(self, block: bool):
            if block:
                await signal.wait.remote()

            return "v1", os.getpid()

    @serve.deployment(name=name, num_replicas=2)
    class V2:
        async def __call__(self, **kwargs):
            return "v2", os.getpid()

    h = serve.run(V1.bind(), name="app")
    vals1, pids1 = zip(*[h.remote(block=False).result() for _ in range(10)])
    assert set(vals1) == {"v1"}
    assert len(set(pids1)) == 2

    # ref2 will block a single replica until the signal is sent.
    ref2 = h.remote(block=True)
    with pytest.raises(TimeoutError):
        ref2.result(timeout_s=1)

    # Redeploy new version.
    serve._run(V2.bind(), _blocking=False, name="app")
    with pytest.raises(TimeoutError):
        client._wait_for_application_running("app", timeout_s=2)

    if RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS:
        # Two new replicas should be started.
        vals2, pids2 = zip(*[h.remote(block=False).result() for _ in range(10)])
        assert set(vals2) == {"v2"}
    else:
        vals2, pids2 = zip(*[h.remote(block=False).result() for _ in range(10)])
        # Since there is one replica blocking, only one new
        # replica should be started up.
        assert "v1" in vals2

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val, pid = ref2.result()
    assert val == "v1"
    assert pid in pids1

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    client._wait_for_application_running("app", timeout_s=10)
    vals3, pids3 = zip(*[h.remote(block=False).result() for _ in range(10)])
    assert set(vals3) == {"v2"}
    assert len(set(pids3)) == 2


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

    signal_name = f"signal-{get_random_string()}"
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
    serve._run(
        V1.options(user_config={"test": "2"}).bind(), name="app", _blocking=False
    )
    responses2, blocking2 = make_nonblocking_calls({"1": 1}, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    # Signal reconfigure to finish. Now the goal should complete and both
    # replicas should have the updated config.
    ray.get(signal.send.remote())
    client._wait_for_application_running("app")
    make_nonblocking_calls({"2": 2})


def test_reconfigure_does_not_run_while_there_are_active_queries(serve_instance):
    """
    This tests checks that reconfigure can't trigger while there are active requests,
    so that the actor's state is not mutated mid-request.

    https://github.com/ray-project/ray/pull/20315
    """
    signal = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=10, num_replicas=1)
    class A:
        def __init__(self):
            self.state = None

        def reconfigure(self, config):
            self.state = config

        async def __call__(self):
            await signal.wait.remote()
            return self.state["a"]

    handle = serve.run(A.options(version="1", user_config={"a": 1}).bind())
    responses = [handle.remote() for _ in range(10)]

    # Give the queries time to get to the replicas before the reconfigure.
    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == len(responses)
    )

    @ray.remote(num_cpus=0)
    def reconfigure():
        serve.run(A.options(version="1", user_config={"a": 2}).bind())

    # Start the reconfigure;
    # this will not complete until the signal is released
    # to allow the queries to complete.
    reconfigure_ref = reconfigure.remote()

    # Release the signal to allow the queries to complete.
    signal.send.remote()

    # Wait for the reconfigure to complete.
    ray.get(reconfigure_ref)

    # These should all be 1 because the queries were sent before the reconfigure,
    # the reconfigure blocks until they complete,
    # and we just waited for the reconfigure to finish.
    results = [r.result() for r in responses]
    print(results)
    assert all([r == 1 for r in results])

    # If we query again, it should be 2,
    # because the reconfigure will have gone through after the
    # original queries completed.
    assert handle.remote().result() == 2


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


def test_handle_method_name_validation(serve_instance):
    @serve.deployment
    class A:
        def b(self, *args):
            return "hello"

    handle = serve.run(A.bind(), name="app")

    # Specify method via `.options`.
    assert handle.options(method_name="b").remote().result() == "hello"

    # Specify method via attribute.
    assert handle.b.remote().result() == "hello"

    # Unknown method.
    with pytest.raises(RayServeException):
        handle.options(method_name="c").remote().result()

    with pytest.raises(RayServeException):
        handle.c.remote().result()


def test_deploy_with_init_args(serve_instance):
    @serve.deployment()
    class D:
        def __init__(self, *args):
            self._args = args

        def get_args(self):
            return self._args

    handle = serve.run(D.bind(1, 2, 3))
    assert handle.get_args.remote().result() == (1, 2, 3)


def test_deploy_with_init_kwargs(serve_instance):
    @serve.deployment()
    class D:
        def __init__(self, **kwargs):
            self._kwargs = kwargs

        def get_kwargs(self, *args):
            return self._kwargs

    handle = serve.run(D.bind(a=1, b=2))
    assert handle.get_kwargs.remote().result() == {"a": 1, "b": 2}


def test_init_args_with_closure(serve_instance):
    @serve.deployment
    class Evaluator:
        def __init__(self, func: Callable):
            self._func = func

        def __call__(self, inp: int) -> int:
            return self._func(inp)

    handle = serve.run(Evaluator.bind(lambda a: a + 1))
    assert handle.remote(41).result() == 42


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

        @serve.deployment(ray_actor_options=[1, 2, 3])
        class BadActorOpts:
            pass

    with pytest.raises(TypeError):
        Base.options(ray_actor_options="hi")

    with pytest.raises(ValidationError):

        @serve.deployment(max_ongoing_requests="hi")
        class BadMaxQueries:
            pass

    with pytest.raises(ValidationError):
        Base.options(max_ongoing_requests=[1])

    with pytest.raises(ValueError):

        @serve.deployment(max_ongoing_requests=0)
        class ZeroMaxQueries:
            pass

    with pytest.raises(ValueError):
        Base.options(max_ongoing_requests=0)

    with pytest.raises(ValueError):

        @serve.deployment(max_ongoing_requests=-1)
        class NegativeMaxQueries:
            pass

    with pytest.raises(ValueError):
        Base.options(max_ongoing_requests=-1)


def test_deployment_properties():
    class DClass:
        pass

    D = serve.deployment(
        name="name",
        version="version",
        num_replicas=2,
        user_config="hi",
        max_ongoing_requests=100,
        ray_actor_options={"num_cpus": 2},
    )(DClass)

    assert D.name == "name"
    assert D.version == "version"
    assert D.num_replicas == 2
    assert D.user_config == "hi"
    assert D.max_ongoing_requests == 100
    assert D.ray_actor_options == {"num_cpus": 2}

    D = serve.deployment(
        version=None,
    )(DClass)
    assert D.version is None


def test_deploy_multiple_apps_batched(serve_instance):
    @serve.deployment
    class A:
        def __call__(self):
            return "a"

    @serve.deployment
    class B:
        def __call__(self):
            return "b"

    serve.run_many(
        [
            serve.RunTarget(A.bind(), name="a", route_prefix="/a"),
            serve.RunTarget(B.bind(), name="b", route_prefix="/b"),
        ]
    )

    assert serve.get_app_handle("a").remote().result() == "a"
    assert serve.get_app_handle("b").remote().result() == "b"

    assert requests.get("http://localhost:8000/a").text == "a"
    assert requests.get("http://localhost:8000/b").text == "b"


def test_redeploy_multiple_apps_batched(serve_instance):
    @serve.deployment
    class A:
        def __call__(self):
            return "a", os.getpid()

    @serve.deployment
    class V1:
        def __call__(self):
            return "version 1", os.getpid()

    @serve.deployment
    class V2:
        def __call__(self):
            return "version 2", os.getpid()

    serve.run_many(
        [
            serve.RunTarget(A.bind(), name="a", route_prefix="/a"),
            serve.RunTarget(V1.bind(), name="v", route_prefix="/v"),
        ]
    )

    a1, pida1 = serve.get_app_handle("a").remote().result()

    assert a1 == "a"

    v1, pid1 = serve.get_app_handle("v").remote().result()

    assert v1 == "version 1"

    serve.run_many(
        [
            serve.RunTarget(V2.bind(), name="v", route_prefix="/v"),
        ]
    )

    v2, pid2 = serve.get_app_handle("v").remote().result()

    assert v2 == "version 2"
    assert pid1 != pid2

    # Redeploying "v" should not have affected "a"
    a2, pida2 = serve.get_app_handle("a").remote().result()

    assert a1 == a2
    assert pida1 == pida2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
