import sys

import pytest

from ray import serve
from ray.serve._private.constants import RAY_SERVE_ENABLE_NEW_HANDLE_API
from ray.serve.handle import DeploymentHandle, DeploymentResponse


def test_basic(serve_instance):
    assert (
        RAY_SERVE_ENABLE_NEW_HANDLE_API
    ), "This test needs to be run with RAY_SERVE_ENABLE_NEW_HANDLE_API=1 set."

    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle
            assert isinstance(self._handle, DeploymentHandle)

        async def __call__(self):
            ref = self._handle.remote()
            assert isinstance(ref, DeploymentResponse)
            return await ref

    handle: DeploymentHandle = serve.run(Deployment.bind(downstream.bind()))
    assert isinstance(handle, DeploymentHandle)
    assert handle.remote().result() == "hello"


def test_get_app_and_deployment_handle(serve_instance):
    assert (
        RAY_SERVE_ENABLE_NEW_HANDLE_API
    ), "This test needs to be run with RAY_SERVE_ENABLE_NEW_HANDLE_API=1 set."

    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            pass

        async def check_get_deployment_handle(self):
            handle = serve.get_deployment_handle(deployment_name="downstream")
            assert isinstance(handle, DeploymentHandle)

            ref = handle.remote()
            assert isinstance(ref, DeploymentResponse)
            return await ref

    serve.run(Deployment.bind(downstream.bind()))
    handle = serve.get_app_handle("default")
    assert isinstance(handle, DeploymentHandle)
    assert handle.check_get_deployment_handle.remote().result() == "hello"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
