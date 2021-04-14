from collections import defaultdict
import os
import sys
import time

from pydantic.error_wrappers import ValidationError
import pytest
import requests

import ray
from ray.test_utils import SignalActor, wait_for_condition
from ray import serve
from ray.serve.utils import get_random_letters


@pytest.mark.parametrize("use_handle", [True, False])
def test_basic_updates(serve_instance, use_handle):
    @serve.deployment(version="1")
    def d(*args):
        return f"1|{os.getpid()}"

    def call():
        if use_handle:
            ret = ray.get(d.get_handle().remote())
        else:
            ret = requests.get("http://localhost:8000/d").text

        return ret.split("|")[0], ret.split("|")[1]

    d.deploy()
    assert len(d.replicas) == 1
    assert d.replicas[0].version == "1"
    tag1 = d.replicas[0].replica_tag
    val1, pid1 = call()
    assert val1 == "1"

    # Redeploying with the same version and code should do nothing.
    d.deploy()
    assert len(d.replicas) == 1
    assert d.replicas[0].version == "1"
    assert d.replicas[0].replica_tag == tag1
    val2, pid2 = call()
    assert val2 == "1"
    assert pid2 == pid1

    # Redeploying with a new version should start a new actor.
    d.options(version="2").deploy()
    assert len(d.replicas) == 1
    assert d.replicas[0].version == "2"
    tag2 = d.replicas[0].replica_tag
    assert tag2 != tag1
    val3, pid3 = call()
    assert val3 == "1"
    assert pid3 != pid2

    @serve.deployment(version="2")
    def d(*args):
        return f"2|{os.getpid()}"

    # Redeploying with the same version and new code should do nothing.
    d.deploy()
    assert len(d.replicas) == 1
    assert d.replicas[0].version == "2"
    assert d.replicas[0].replica_tag == tag2
    val4, pid4 = call()
    assert val4 == "1"
    assert pid4 == pid3

    # Redeploying with new code and a new version should start a new actor
    # running the new code.
    d.options(version="3").deploy()
    assert len(d.replicas) == 1
    assert d.replicas[0].version == "3"
    assert d.replicas[0].replica_tag != tag2
    val5, pid5 = call()
    assert val5 == "2"
    assert pid5 != pid4


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
    func.deploy()
    Class.deploy()

    assert ray.get(func.get_handle().remote()) == "hi"
    assert ray.get(Class.get_handle().ping.remote()) == "pong"


@pytest.mark.parametrize("use_handle", [True, False])
def test_no_version(serve_instance, use_handle):
    name = "test"

    @serve.deployment(name=name)
    def v1(*args):
        return f"1|{os.getpid()}"

    def call():
        if use_handle:
            ret = ray.get(v1.get_handle().remote())
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    v1.deploy()
    assert len(v1.replicas) == 1
    tag1 = v1.replicas[0].replica_tag
    val1, pid1 = call()
    assert val1 == "1"

    @serve.deployment(name=name)
    def v2(*args):
        return f"2|{os.getpid()}"

    # Not specifying a version tag should cause it to always be updated.
    v2.deploy()
    assert len(v2.replicas) == 1
    tag2 = v2.replicas[0].replica_tag
    assert tag2 != tag1
    val2, pid2 = call()
    assert val2 == "2"
    assert pid2 != pid1

    v2.deploy()
    assert len(v2.replicas) == 1
    tag3 = v2.replicas[0].replica_tag
    assert tag3 != tag2
    val3, pid3 = call()
    assert val3 == "2"
    assert pid3 != pid2

    # Specifying the version should stop updates from happening.
    v2.options(version="1").deploy()
    assert len(v2.replicas) == 1
    assert v2.replicas[0].version == "1"
    tag4 = v2.replicas[0].replica_tag
    assert tag4 != tag3
    val4, pid4 = call()
    assert val4 == "2"
    assert pid4 != pid3

    v2.options(version="1").deploy()
    assert len(v2.replicas) == 1
    assert v2.replicas[0].version == "1"
    assert v2.replicas[0].replica_tag == tag4
    val5, pid5 = call()
    assert val5 == "2"
    assert pid5 == pid4


@pytest.mark.parametrize("use_handle", [True, False])
def test_config_change(serve_instance, use_handle):
    @serve.deployment(version="1")
    class D:
        def __init__(self):
            self.ret = "1"

        def reconfigure(self, d):
            self.ret = d["ret"]

        def __call__(self, *args):
            return f"{self.ret}|{os.getpid()}"

    def call():
        if use_handle:
            ret = ray.get(D.get_handle().remote())
        else:
            ret = requests.get("http://localhost:8000/D").text

        return ret.split("|")[0], ret.split("|")[1]

    # First deploy with no user config set.
    D.deploy()
    assert len(D.replicas) == 1
    tag1 = D.replicas[0].replica_tag
    assert D.replicas[0].version == "1"
    val1, pid1 = call()
    assert val1 == "1"

    # Now update the user config without changing versions. Actor should stay
    # alive but return value should change.
    D.options(user_config={"ret": "2"}).deploy()
    assert len(D.replicas) == 1
    assert D.replicas[0].replica_tag == tag1
    assert D.replicas[0].version == "1"
    val2, pid2 = call()
    assert pid2 == pid1
    assert val2 == "2"

    # Update the user config without changing the version again.
    D.options(user_config={"ret": "3"}).deploy()
    assert len(D.replicas) == 1
    assert D.replicas[0].replica_tag == tag1
    assert D.replicas[0].version == "1"
    val3, pid3 = call()
    assert pid3 == pid2
    assert val3 == "3"

    # Update the version without changing the user config.
    D.options(version="2", user_config={"ret": "3"}).deploy()
    assert len(D.replicas) == 1
    tag2 = D.replicas[0].replica_tag
    assert tag2 != tag1
    assert D.replicas[0].version == "2"
    val4, pid4 = call()
    assert pid4 != pid3
    assert val4 == "3"

    # Update the version and the user config.
    D.options(version="3", user_config={"ret": "4"}).deploy()
    assert len(D.replicas) == 1
    tag3 = D.replicas[0].replica_tag
    assert tag3 != tag2
    assert D.replicas[0].version == "3"
    val5, pid5 = call()
    assert pid5 != pid4
    assert val5 == "4"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_single_replica(serve_instance, use_handle):
    # Tests that redeploying a deployment with a single replica waits for the
    # replica to completely shut down before starting a new one.
    client = serve_instance

    name = "test"

    @ray.remote
    def call(block=False):
        if use_handle:
            handle = serve.get_deployment(name).get_handle()
            ret = ray.get(handle.handler.remote(block))
        else:
            ret = requests.get(
                f"http://localhost:8000/{name}", params={
                    "block": block
                }).text

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

    V1.deploy()
    assert len(V1.replicas) == 1
    tag1 = V1.replicas[0].replica_tag
    assert V1.replicas[0].version == "1"
    ref1 = call.remote(block=False)
    val1, pid1 = ray.get(ref1)
    assert val1 == "1"

    # ref2 will block until the signal is sent.
    ref2 = call.remote(block=True)
    assert len(ray.wait([ref2], timeout=0.1)[0]) == 0

    # Redeploy new version. This should not go through until the old version
    # replica completely stops.
    V2 = V1.options(backend_def=V2, version="2")
    goal_ref = V2.deploy(_blocking=False)
    assert not client._wait_for_goal(goal_ref, timeout=0.1)
    assert len(V2.replicas) == 0

    # It may take some time for the handle change to propagate and requests
    # to get sent to the new version. Repeatedly send requests until they
    # start blocking
    start = time.time()
    new_version_ref = None
    while time.time() - start < 30:
        ready, not_ready = ray.wait([call.remote(block=False)], timeout=0.5)
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
    assert client._wait_for_goal(goal_ref)
    assert len(V2.replicas) == 1
    assert V2.replicas[0].replica_tag != tag1
    assert V2.replicas[0].version == "2"
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
            handle = serve.get_deployment(name).get_handle()
            ret = ray.get(handle.handler.remote(block))
        else:
            ret = requests.get(
                f"http://localhost:8000/{name}", params={
                    "block": block
                }).text

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
            ready, not_ready = ray.wait(refs, timeout=0.5)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)
            for ref in not_ready:
                blocking.extend(not_ready)

            if (all(
                    len(responses[val]) == num
                    for val, num in expected.items())
                    and (expect_blocking is False or len(blocking) > 0)):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses, blocking

    V1.deploy()
    assert len(V1.replicas) == 2
    assert V1.replicas[0].version == "1"
    assert V1.replicas[1].version == "1"
    responses1, _ = make_nonblocking_calls({"1": 2})
    pids1 = responses1["1"]

    # ref2 will block a single replica until the signal is sent. Check that
    # some requests are now blocking.
    ref2 = call.remote(block=True)
    responses2, blocking2 = make_nonblocking_calls(
        {
            "1": 1
        }, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    # Redeploy new version. Since there is one replica blocking, only one new
    # replica should be started up.
    V2 = V1.options(backend_def=V2, version="2")
    goal_ref = V2.deploy(_blocking=False)
    assert not client._wait_for_goal(goal_ref, timeout=0.1)
    responses3, blocking3 = make_nonblocking_calls(
        {
            "1": 1
        }, expect_blocking=True)

    assert len(V2.replicas) == 1
    assert V2.replicas[0].version == "1"

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val, pid = ray.get(ref2)
    assert val == "1"
    assert pid in responses1["1"]

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    assert client._wait_for_goal(goal_ref)
    assert len(V2.replicas) == 2
    assert V2.replicas[0].version == "2"
    assert V2.replicas[1].version == "2"
    make_nonblocking_calls({"2": 2})


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
            handle = v1.get_handle()
            ret = ray.get(handle.remote())
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    def make_calls(expected):
        # Returns dict[val, set(pid)].
        responses = defaultdict(set)
        start = time.time()
        while time.time() - start < 30:
            refs = [call.remote() for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=0.5)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)

            if all(
                    len(responses[val]) == num
                    for val, num in expected.items()):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses

    v1.deploy()
    assert len(v1.replicas) == 4
    assert all(r.version == "1" for r in v1.replicas)
    responses1 = make_calls({"1": 4})
    pids1 = responses1["1"]

    @serve.deployment(name=name, version="2", num_replicas=2)
    def v2(*args):
        return f"2|{os.getpid()}"

    v2.deploy()
    assert len(v2.replicas) == 2
    assert all(r.version == "2" for r in v2.replicas)
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
            handle = v1.get_handle()
            ret = ray.get(handle.remote())
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    def make_calls(expected):
        # Returns dict[val, set(pid)].
        responses = defaultdict(set)
        start = time.time()
        while time.time() - start < 30:
            refs = [call.remote() for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=0.5)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)

            if all(
                    len(responses[val]) == num
                    for val, num in expected.items()):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses

    v1.deploy()
    assert len(v1.replicas) == 2
    assert all(r.version == "1" for r in v1.replicas)
    responses1 = make_calls({"1": 2})
    pids1 = responses1["1"]

    @serve.deployment(name=name, version="2", num_replicas=4)
    def v2(*args):
        return f"2|{os.getpid()}"

    v2.deploy()
    assert len(v2.replicas) == 4
    assert all(r.version == "2" for r in v2.replicas)
    responses2 = make_calls({"2": 4})
    assert all(pid not in pids1 for pid in responses2["2"])


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_deploy_handle_validation(serve_instance):
    class A:
        def b(self, *args):
            return "hello"

    serve_instance.deploy("f", A)
    handle = serve.get_handle("f")

    # Legacy code path
    assert ray.get(handle.options(method_name="b").remote()) == "hello"
    # New code path
    assert ray.get(handle.b.remote()) == "hello"
    with pytest.raises(AttributeError):
        handle.c.remote()

    # Test missing_ok case
    missing_handle = serve.get_handle("g", missing_ok=True)
    with pytest.raises(AttributeError):
        missing_handle.b.remote()
    serve_instance.deploy("g", A)
    # Old code path still work
    assert ray.get(missing_handle.options(method_name="b").remote()) == "hello"
    # Because the missing_ok flag, handle.b.remote won't work.
    with pytest.raises(AttributeError):
        missing_handle.b.remote()


def test_init_args(serve_instance):
    with pytest.raises(TypeError):

        @serve.deployment(init_args=[1, 2, 3])
        class BadInitArgs:
            pass

    @serve.deployment(init_args=(1, 2, 3))
    class D:
        def __init__(self, *args):
            self._args = args

        def get_args(self, *args):
            return self._args

    D.deploy()
    handle = D.get_handle()

    def check(*args):
        assert ray.get(handle.get_args.remote()) == args

    # Basic sanity check.
    assert ray.get(handle.get_args.remote()) == (1, 2, 3)
    check(1, 2, 3)

    # Check passing args to `.deploy()`.
    D.deploy(4, 5, 6)
    check(4, 5, 6)

    # Passing args to `.deploy()` shouldn't override those passed in decorator.
    D.deploy()
    check(1, 2, 3)

    # Check setting with `.options()`.
    new_D = D.options(init_args=(7, 8, 9))
    new_D.deploy()
    check(7, 8, 9)

    # Should not have changed old deployment object.
    D.deploy()
    check(1, 2, 3)

    # Check that args are only updated on version change.
    D.options(version="1").deploy()
    check(1, 2, 3)

    D.options(version="1").deploy(10, 11, 12)
    check(1, 2, 3)

    D.options(version="2").deploy(10, 11, 12)
    check(10, 11, 12)


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

    with pytest.raises(ValidationError):

        @serve.deployment(num_replicas=0)
        class ZeroNumReplicas:
            pass

    with pytest.raises(ValidationError):
        Base.options(num_replicas=0)

    with pytest.raises(ValidationError):

        @serve.deployment(num_replicas=-1)
        class NegativeNumReplicas:
            pass

    with pytest.raises(ValidationError):
        Base.options(num_replicas=-1)

    with pytest.raises(TypeError):

        @serve.deployment(init_args=[1, 2, 3])
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
            assert len(set(ray.get(
                [handle.remote() for _ in range(50)]))) == num

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


class TestRuntimeMetadata:
    def test_no_metadata(self, serve_instance):
        @serve.deployment
        def d(*args):
            return "hi"

        d.deploy()
        assert len(d.replicas) == 1
        assert d.replicas[0].runtime_metadata is None

    def test_basic(self, serve_instance):
        @serve.deployment
        class D:
            def ready_check(self):
                return "meta"

        D.deploy()
        assert len(D.replicas) == 1
        assert D.replicas[0].runtime_metadata == "meta"

    def test_multiple_replicas_same_meta(self, serve_instance):
        @serve.deployment(num_replicas=2)
        class D:
            def ready_check(self):
                return "meta"

        D.deploy()
        assert len(D.replicas) == 2
        assert all(r.runtime_metadata == "meta" for r in D.replicas)

    def test_multiple_replicas_different_meta(self, serve_instance):
        @serve.deployment(num_replicas=2)
        class D:
            def ready_check(self):
                return os.getpid()

        D.deploy()
        assert len(D.replicas) == 2
        assert len(set(r.runtime_metadata for r in D.replicas)) == 2

    def test_redeploy_different_meta(self, serve_instance):
        @serve.deployment(name="test", num_replicas=2)
        class D:
            def ready_check(self):
                return "meta1"

        D.deploy()
        assert len(D.replicas) == 2
        assert all(r.runtime_metadata == "meta1" for r in D.replicas)
        tags1 = set(r.replica_tag for r in D.replicas)
        assert len(tags1) == 2

        @serve.deployment(name="test", num_replicas=2)
        class D:
            def ready_check(self):
                return "meta2"

        D.deploy()
        assert len(D.replicas) == 2
        assert all(r.runtime_metadata == "meta2" for r in D.replicas)
        tags2 = set(r.replica_tag for r in D.replicas)
        assert len(tags2) == 2
        assert tags2.isdisjoint(tags1)

    def test_kill_replicas_new_meta(self, serve_instance):
        @serve.deployment(name="test", num_replicas=2)
        class D:
            def ready_check(self):
                return os.getpid()

        D.deploy()
        assert len(D.replicas) == 2
        metas1 = set(r.runtime_metadata for r in D.replicas)
        assert len(metas1) == 2
        tags1 = set(r.replica_tag for r in D.replicas)
        assert len(tags1) == 2

        [ray.kill(r.actor_handle) for r in D.replicas]

        def restarted():
            new_tags = set(r.replica_tag for r in D.replicas)
            return len(D.replicas) == 2 and new_tags.isdisjoint(tags1)

        wait_for_condition(restarted)

        assert len(D.replicas) == 2
        metas2 = set(r.runtime_metadata for r in D.replicas)
        assert metas2.isdisjoint(metas1)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
