import os
import sys
import time
import pytest
import requests
import asyncio
import ray
from ray import serve
from collections import defaultdict
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve.utils import get_random_letters


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
    val1, pid1 = call()
    assert val1 == "1"

    # Now update the user config without changing versions. Actor should stay
    # alive but return value should change.
    D.options(user_config={"ret": "2"}).deploy()
    val2, pid2 = call()
    assert pid2 == pid1
    assert val2 == "2"

    # Update the user config without changing the version again.
    D.options(user_config={"ret": "3"}).deploy()
    val3, pid3 = call()
    assert pid3 == pid2
    assert val3 == "3"

    # Update the version without changing the user config.
    D.options(version="2", user_config={"ret": "3"}).deploy()
    val4, pid4 = call()
    assert pid4 != pid3
    assert val4 == "3"

    # Update the version and the user config.
    D.options(version="3", user_config={"ret": "4"}).deploy()
    val5, pid5 = call()
    assert pid5 != pid4
    assert val5 == "4"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
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

    A.options(user_config="not_hi").deploy()
    config = ray.get(A.get_handle().remote())
    assert config == "not_hi"

    with pytest.raises(RuntimeError):
        A.options(user_config="hi").deploy()

    def rolled_back():
        try:
            config = ray.get(A.get_handle().remote())
            return config == "not_hi"
        except Exception:
            return False

    # Ensure we should be able to rollback to "hi" config
    wait_for_condition(rolled_back)


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
            ready, not_ready = ray.wait(refs, timeout=5)
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
    V2 = V1.options(func_or_class=V2, version="2")
    goal_ref = V2.deploy(_blocking=False)
    assert not client._wait_for_goal(goal_ref, timeout=0.1)
    responses3, blocking3 = make_nonblocking_calls(
        {
            "1": 1
        }, expect_blocking=True)

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val, pid = ray.get(ref2)
    assert val == "1"
    assert pid in responses1["1"]

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    assert client._wait_for_goal(goal_ref)
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
            handle = serve.get_deployment(name).get_handle()
            ret = ray.get(handle.handler.remote())
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
            return f"{self.config}|{os.getpid()}"

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

            if (all(
                    len(responses[val]) == num
                    for val, num in expected.items())
                    and (expect_blocking is False or len(blocking) > 0)):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses, blocking

    V1.options(user_config="1").deploy()
    responses1, _ = make_nonblocking_calls({"1": 2})
    pids1 = responses1["1"]

    # Reconfigure should block one replica until the signal is sent. Check that
    # some requests are now blocking.
    goal_ref = V1.options(user_config="2").deploy(_blocking=False)
    responses2, blocking2 = make_nonblocking_calls(
        {
            "1": 1
        }, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    # Signal reconfigure to finish. Now the goal should complete and both
    # replicas should have the updated config.
    ray.get(signal.send.remote())
    assert client._wait_for_goal(goal_ref)
    make_nonblocking_calls({"2": 2})


def test_reconfigure_with_queries(serve_instance):
    @serve.deployment(max_concurrent_queries=10, num_replicas=3)
    class A:
        def __init__(self):
            self.state = None

        def reconfigure(self, config):
            self.state = config

        async def __call__(self):
            await asyncio.sleep(10)
            return self.state["a"]

    A.options(version="1", user_config={"a": 1}).deploy()
    handle = A.get_handle()
    refs = []
    for _ in range(30):
        refs.append(handle.remote())

    A.options(version="1", user_config={"a": 2}).deploy()

    for ref in refs:
        assert ray.get(ref) == 1
    assert ray.get(handle.remote()) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
