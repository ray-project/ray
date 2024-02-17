import asyncio
import concurrent.futures
from unittest.mock import Mock

import pytest

from ray.exceptions import RayTaskError
from ray.serve._private.proxy_state import ActorProxyWrapper, wrap_as_future
from ray.serve.schema import LoggingConfig


def _create_object_ref_mock():
    fut = concurrent.futures.Future()

    ray_object_ref_mock = Mock(future=Mock(return_value=fut))

    return ray_object_ref_mock, fut


def _create_mocked_actor_proxy_wrapper(actor_handle_mock):
    return ActorProxyWrapper(
        logging_config=LoggingConfig(),
        actor_handle=actor_handle_mock,
        node_id="some_node_id",
    )


@pytest.mark.asyncio
async def test_wrap_as_future_timeout():
    object_ref_mock, fut = _create_object_ref_mock()

    aio_fut = wrap_as_future(ref=object_ref_mock, timeout_s=0)

    assert not aio_fut.done()
    # Yield the event-loop
    await asyncio.sleep(0.001)

    assert aio_fut.done()
    with pytest.raises(TimeoutError) as exc_info:
        aio_fut.result()

    assert "Future cancelled after timeout 0s" in str(exc_info.value)


@pytest.mark.asyncio
async def test_wrap_as_future_success():
    # Test #1: Validate wrapped asyncio future completes, upon completion of the
    #          ObjectRef's one

    object_ref_mock, fut = _create_object_ref_mock()

    aio_fut = wrap_as_future(ref=object_ref_mock, timeout_s=3600)

    assert not aio_fut.done()
    # Complete source future (ObjectRef one)
    fut.set_result("test")
    # Yield the event-loop
    await asyncio.sleep(0.001)

    assert aio_fut.done()
    assert aio_fut.result() == "test"

    # Test #2: Wrapped asyncio future completed before time-out expiration,
    #          should not be affected by the cancellation callback

    object_ref_mock, fut = _create_object_ref_mock()
    # Purposefully set timeout to 0, ie future has to be cancelled upon next
    # event-loop iteration
    aio_fut = wrap_as_future(ref=object_ref_mock, timeout_s=0)

    assert not aio_fut.done()
    # Complete source future (ObjectRef one)
    fut.set_result("test")
    # Yield the event-loop
    await asyncio.sleep(0.001)

    assert aio_fut.done()
    assert aio_fut.result() == "test"


@pytest.mark.parametrize(
    ("response", "is_ready"),
    [
        # ProxyActor.ready responds with an tuple/array of 2 strings
        ('["foo", "bar"]', True),
        ("malformed_json", False),
        (Exception(), False),
    ],
)
@pytest.mark.asyncio
async def test_is_ready_check_success(response, is_ready):
    """Tests calling is_ready method on ProxyActorWrapper, mocking out underlying
    ActorHandle response"""
    object_ref_mock, fut = _create_object_ref_mock()
    actor_handle_mock = Mock(ready=Mock(remote=Mock(return_value=object_ref_mock)))

    proxy_wrapper = _create_mocked_actor_proxy_wrapper(actor_handle_mock)

    for _ in range(10):
        assert proxy_wrapper.is_ready(timeout_s=1) is None
        # Yield loop!
        await asyncio.sleep(0.01)

    # Complete source future
    if isinstance(response, Exception):
        object_ref_mock.future().set_exception(response)
    else:
        object_ref_mock.future().set_result(response)

    # Yield loop!
    await asyncio.sleep(0)
    # NOTE: Timeout setting is only relevant, in case there's no pending request
    #       and one will be issued
    assert proxy_wrapper.is_ready(timeout_s=1) is is_ready


@pytest.mark.asyncio
async def test_is_ready_check_timeout():
    object_ref_mock, fut = _create_object_ref_mock()
    actor_handle_mock = Mock(ready=Mock(remote=Mock(return_value=object_ref_mock)))

    proxy_wrapper = _create_mocked_actor_proxy_wrapper(actor_handle_mock)

    # First call, invokes ProxyActor.ready call
    assert proxy_wrapper.is_ready(timeout_s=0) is None
    # Yield loop!
    await asyncio.sleep(0.001)

    assert proxy_wrapper.is_ready(timeout_s=0) is False


@pytest.mark.parametrize(
    ("response", "is_healthy"),
    [
        (None, True),
        (RayTaskError("check_health", "<traceback>", "cuz"), False),
    ],
)
@pytest.mark.asyncio
async def test_is_healthy_check_success(response, is_healthy):
    """Tests calling is_healthy method on ProxyActorWrapper, mocking out underlying
    ActorHandle response"""
    object_ref_mock, fut = _create_object_ref_mock()
    actor_handle_mock = Mock(
        check_health=Mock(remote=Mock(return_value=object_ref_mock))
    )

    proxy_wrapper = _create_mocked_actor_proxy_wrapper(actor_handle_mock)

    for _ in range(10):
        assert proxy_wrapper.is_healthy(timeout_s=1) is None
        # Yield loop!
        await asyncio.sleep(0.01)

    object_ref_mock.future.assert_called_once()

    # Complete source future
    if isinstance(response, Exception):
        object_ref_mock.future.return_value.set_exception(response)
    else:
        object_ref_mock.future.return_value.set_result(response)

    # Yield loop!
    await asyncio.sleep(0)
    # NOTE: Timeout setting is only relevant, in case there's no pending request
    #       and one will be issued
    assert proxy_wrapper.is_healthy(timeout_s=1) is is_healthy


@pytest.mark.asyncio
async def test_is_healthy_check_timeout():
    object_ref_mock, fut = _create_object_ref_mock()
    actor_handle_mock = Mock(
        check_health=Mock(remote=Mock(return_value=object_ref_mock))
    )

    proxy_wrapper = _create_mocked_actor_proxy_wrapper(actor_handle_mock)

    # First call, invokes ProxyActor.ready call
    assert proxy_wrapper.is_healthy(timeout_s=0) is None
    # Yield loop!
    await asyncio.sleep(0.001)

    assert proxy_wrapper.is_healthy(timeout_s=0) is False


@pytest.mark.parametrize(
    ("response", "is_drained"),
    [
        (True, True),
        (False, False),
        (RayTaskError("is_drained", "<traceback>", "cuz"), False),
    ],
)
@pytest.mark.asyncio
async def test_is_drained_check_success(response, is_drained):
    """Tests calling is_drained method on ProxyActorWrapper, mocking out underlying
    ActorHandle response"""
    object_ref_mock, fut = _create_object_ref_mock()
    actor_handle_mock = Mock(is_drained=Mock(remote=Mock(return_value=object_ref_mock)))

    proxy_wrapper = _create_mocked_actor_proxy_wrapper(actor_handle_mock)

    for _ in range(10):
        assert proxy_wrapper.is_drained(timeout_s=1) is None
        # Yield loop!
        await asyncio.sleep(0.01)

    object_ref_mock.future.assert_called_once()

    # Complete source future
    if isinstance(response, Exception):
        object_ref_mock.future.return_value.set_exception(response)
    else:
        object_ref_mock.future.return_value.set_result(response)

    # Yield loop!
    await asyncio.sleep(0)
    # NOTE: Timeout setting is only relevant, in case there's no pending request
    #       and one will be issued
    assert proxy_wrapper.is_drained(timeout_s=1) is is_drained


@pytest.mark.asyncio
async def test_is_drained_check_timeout():
    object_ref_mock, fut = _create_object_ref_mock()
    actor_handle_mock = Mock(is_drained=Mock(remote=Mock(return_value=object_ref_mock)))

    proxy_wrapper = _create_mocked_actor_proxy_wrapper(actor_handle_mock)

    # First call, invokes ProxyActor.ready call
    assert proxy_wrapper.is_drained(timeout_s=0) is None
    # Yield loop!
    await asyncio.sleep(0.001)

    assert proxy_wrapper.is_drained(timeout_s=0) is False


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
