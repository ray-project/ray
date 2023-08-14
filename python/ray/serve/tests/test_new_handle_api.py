import sys

import pytest

import ray

from ray import serve
from ray.serve.exceptions import RayServeException
from ray.serve.handle import (
    DeploymentHandle,
    DeploymentHandleRef,
    DeploymentHandleGenerator,
    RayServeHandle,
    RayServeSyncHandle,
)

"""Test cases:
- Test setting the flag via `.options`.
- Test setting the flag via environment variable.
- Test handle with an async deployment.
- Test handle returned from `serve.run`.
- Test handle returned from `serve.get_app_handle`.
- Test handle returned from `serve.get_deployment_handle`.
- Test composition.
"""


def test_basic(serve_instance):
    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: RayServeHandle):
            self._handle = handle
            assert isinstance(self._handle, RayServeHandle)
            self._new_handle = handle.options(use_new_handle_api=True)
            assert isinstance(self._new_handle, DeploymentHandle)

        async def __call__(self):
            ref = self._new_handle.remote()
            assert isinstance(ref, DeploymentHandleRef)
            return await ref

    handle: RayServeSyncHandle = serve.run(Deployment.bind(downstream.bind()))
    assert isinstance(handle, RayServeSyncHandle)
    new_handle = handle.options(use_new_handle_api=True)
    assert isinstance(new_handle, DeploymentHandle)
    assert new_handle.remote().result() == "hello"


def test_get_app_and_deployment_handle(serve_instance):
    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: RayServeHandle):
            pass

        async def check_get_deployment_handle(self):
            handle = serve.get_deployment_handle(deployment_name="downstream")
            assert isinstance(handle, DeploymentHandle)

            ref = handle.remote()
            assert isinstance(ref, DeploymentHandleRef)
            return await ref

    serve.run(Deployment.bind(downstream.bind()))
    handle = serve.get_app_handle("default")
    assert isinstance(handle, DeploymentHandle)
    assert handle.check_get_deployment_handle.remote().result() == "hello"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
