from typing import Callable, Optional

import pytest

from ray.serve._private.common import DeploymentID

# from ray.serve._private.http_util import MessageQueue
from ray.serve._private.replica import UserCallableWrapper
from ray.serve._private.router import RequestMetadata


class BasicClass:
    def __call__(self):
        return "hi"


"""
Tests to write:
    - async __init__
    - destructor is idempotent
    - health check doesn't call user one if not defined
    - sync method calls
    - async method calls
    - sync generator calls
    - async generator calls
    - stuff related to grpc
    - stuff related to http
"""


def _make_user_callable_wrapper(
    callable: Optional[Callable] = None, *init_args, **init_kwargs
):
    return UserCallableWrapper(
        callable if callable is not None else BasicClass,
        init_args,
        init_kwargs,
        deployment_id=DeploymentID(app="test_app", name="test_name"),
    )


@pytest.mark.asyncio
async def test_calling_initialize_twice():
    user_callable_wrapper = _make_user_callable_wrapper()

    await user_callable_wrapper.initialize_callable()
    assert isinstance(user_callable_wrapper.user_callable, BasicClass)
    with pytest.raises(RuntimeError):
        await user_callable_wrapper.initialize_callable()


@pytest.mark.asyncio
async def test_calling_methods_before_initialize():
    user_callable_wrapper = _make_user_callable_wrapper()

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_user_method(None, tuple(), dict())

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_user_health_check()

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_reconfigure(None)

    await user_callable_wrapper.call_destructor()


@pytest.mark.asyncio
async def test_basic_class_callable():
    user_callable_wrapper = _make_user_callable_wrapper()

    request_metadata = RequestMetadata(
        request_id="test_request", endpoint="test_endpoint"
    )
    await user_callable_wrapper.initialize_callable()
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
    ) == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
