import asyncio
import concurrent.futures
import sys
from unittest.mock import Mock

import pytest

from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.handle_options import DynamicHandleOptions, InitHandleOptions
from ray.serve._private.utils import DEFAULT
from ray.serve.handle import DeploymentHandle


def test_dynamic_handle_options():
    default_options = DynamicHandleOptions()
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting method name.
    only_set_method = default_options.copy_and_update(method_name="hi")
    assert only_set_method.method_name == "hi"
    assert only_set_method.multiplexed_model_id == ""
    assert only_set_method.session_id == ""
    assert only_set_method.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting model ID.
    only_set_model_id = default_options.copy_and_update(multiplexed_model_id="hi")
    assert only_set_model_id.method_name == "__call__"
    assert only_set_model_id.multiplexed_model_id == "hi"
    assert only_set_model_id.session_id == ""
    assert only_set_model_id.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting stream.
    only_set_stream = default_options.copy_and_update(stream=True)
    assert only_set_stream.method_name == "__call__"
    assert only_set_stream.multiplexed_model_id == ""
    assert only_set_stream.session_id == ""
    assert only_set_stream.stream is True

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting session ID.
    only_set_session_id = default_options.copy_and_update(session_id="sess_abc")
    assert only_set_session_id.method_name == "__call__"
    assert only_set_session_id.multiplexed_model_id == ""
    assert only_set_session_id.session_id == "sess_abc"
    assert only_set_session_id.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting multiple.
    set_multiple = default_options.copy_and_update(method_name="hi", stream=True)
    assert set_multiple.method_name == "hi"
    assert set_multiple.multiplexed_model_id == ""
    assert set_multiple.session_id == ""
    assert set_multiple.stream is True


def test_init_handle_options():
    default_options = InitHandleOptions.create()
    assert default_options._prefer_local_routing is False
    assert default_options._source == DeploymentHandleSource.UNKNOWN

    default1 = InitHandleOptions.create(_prefer_local_routing=DEFAULT.VALUE)
    assert default1._prefer_local_routing is False
    assert default1._source == DeploymentHandleSource.UNKNOWN

    default2 = InitHandleOptions.create(_source=DEFAULT.VALUE)
    assert default2._prefer_local_routing is False
    assert default2._source == DeploymentHandleSource.UNKNOWN

    prefer_local = InitHandleOptions.create(
        _prefer_local_routing=True, _source=DEFAULT.VALUE
    )
    assert prefer_local._prefer_local_routing is True
    assert prefer_local._source == DeploymentHandleSource.UNKNOWN

    proxy_options = InitHandleOptions.create(_source=DeploymentHandleSource.PROXY)
    assert proxy_options._prefer_local_routing is False
    assert proxy_options._source == DeploymentHandleSource.PROXY


def _make_handle_for_shutdown(*, run_in_separate_loop, shutdown_future):
    # Bypass __init__ (which spins up a real router) and set only the two
    # attributes that shutdown_async touches.
    handle = DeploymentHandle.__new__(DeploymentHandle)
    handle._router = Mock()
    handle._router.shutdown = Mock(return_value=shutdown_future)
    handle.init_options = Mock(_run_router_in_separate_loop=run_in_separate_loop)
    return handle


@pytest.mark.asyncio
async def test_shutdown_async_same_loop_awaits_future_directly():
    """`_is_router_running_in_separate_loop` is a method, so calling it without
    `()` was always truthy and `shutdown_async` always tried to
    `asyncio.wrap_future` the router's future. For a router in the same event
    loop that future is an ``asyncio.Future``, and ``wrap_future`` rejects it.
    """
    loop = asyncio.get_running_loop()
    shutdown_future = loop.create_future()
    shutdown_future.set_result(None)

    handle = _make_handle_for_shutdown(
        run_in_separate_loop=False, shutdown_future=shutdown_future
    )

    # Must not raise: the else branch awaits the asyncio.Future directly.
    await handle.shutdown_async()
    assert shutdown_future.done()


@pytest.mark.asyncio
async def test_shutdown_async_separate_loop_wraps_concurrent_future():
    """A router in a separate loop returns a ``concurrent.futures.Future``,
    which must be wrapped before it can be awaited on this loop."""
    shutdown_future = concurrent.futures.Future()
    shutdown_future.set_result(None)

    handle = _make_handle_for_shutdown(
        run_in_separate_loop=True, shutdown_future=shutdown_future
    )

    await handle.shutdown_async()
    assert shutdown_future.done()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
