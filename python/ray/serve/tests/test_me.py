import asyncio
import concurrent.futures
import threading

import pytest
import requests

import ray
from ray._private.test_utils import SignalActor

from ray import serve
from ray.serve.exceptions import RayServeException
from ray.serve.handle import _HandleOptions, RayServeHandle, RayServeSyncHandle
from ray.serve._private.router import PowerOfTwoChoicesReplicaScheduler
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.common import RequestProtocol
import subprocess


@pytest.mark.asyncio
async def test_async_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    f.deploy()

    @ray.remote
    class TaskActor:
        async def task(self, handle):
            ref = await handle.remote()
            output = await ref
            return output

    # Test pickling via ray.remote()
    handle = f.get_handle(sync=False)

    task_actor = TaskActor.remote()
    result = await task_actor.task.remote(handle)
    assert result == "hello"


def test_sync_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    handle = serve.run(f.bind())

    @ray.remote
    def task(handle):
        return ray.get(handle.remote())

    # Test pickling via ray.remote()
    result_ref = task.remote(handle)
    assert ray.get(result_ref) == "hello"


def test_handle_prefers_replicas_on_same_node():
    """Verify that handle calls prefer replicas on the same node when possible.

    If all replicas on the same node are occupied (at `max_concurrent_queries` limit),
    requests should spill to other nodes.
    """

    subprocess.check_output(["ray", "stop", "--force"])


def test_repeated_get_handle_cached(serve_instance):
    @serve.deployment
    def f(_):
        return ""

    f.deploy()

    handle_sets = {f.get_handle() for _ in range(100)}
    assert len(handle_sets) == 1

    handle_sets = {serve.get_deployment("f").get_handle() for _ in range(100)}
    assert len(handle_sets) == 1


def test_handle_typing(serve_instance):
    @serve.deployment
    class DeploymentClass:
        pass

    @serve.deployment
    def deployment_func():
        pass

    @serve.deployment
    class Ingress:
        def __init__(
            self, class_downstream: RayServeHandle, func_downstream: RayServeHandle
        ):
            # serve.run()'ing this deployment fails if these assertions fail.
            assert isinstance(class_downstream, RayServeHandle)
            assert isinstance(func_downstream, RayServeHandle)

    h = serve.run(Ingress.bind(DeploymentClass.bind(), deployment_func.bind()))
    assert isinstance(h, RayServeSyncHandle)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
