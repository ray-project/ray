import sys

import pytest

from ray import serve
from ray.serve.handle import (
    DeploymentHandle,
    DeploymentHandleRef,
)
from ray.serve._private.constants import RAY_SERVE_ENABLE_NEW_HANDLE_API

def test_set_flag_via_env_var(serve_instance):
    assert RAY_SERVE_ENABLE_NEW_HANDLE_API, (
        "This test needs to be run with RAY_SERVE_ENABLE_NEW_HANDLE_API=1 set."
    )

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
            assert isinstance(ref, DeploymentHandleRef)
            return await ref

    handle: DeploymentHandle = serve.run(Deployment.bind(downstream.bind()))
    assert isinstance(handle, DeploymentHandle)

    assert handle.remote().result() == "hello"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
