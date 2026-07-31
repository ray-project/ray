import asyncio
import concurrent.futures
import logging
import time
from typing import Optional
from unittest.mock import Mock

import pytest

import ray
import ray.serve._private.proxy_state as proxy_state
from ray._common.test_utils import wait_for_condition
from ray.exceptions import ActorUnschedulableError, RayTaskError
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.proxy_state import ActorProxyWrapper, wrap_as_future
from ray.serve.config import HTTPOptions, gRPCOptions
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


@pytest.mark.asyncio
async def test_wrap_as_future_timeout_does_not_assert_on_late_source_completion(
    monkeypatch,
):
    object_ref_mock, fut = _create_object_ref_mock()
    loop = asyncio.get_running_loop()
    queued_callbacks = []

    def queue_callback(callback, *args, context=None):
        queued_callbacks.append((callback, args, context))

    monkeypatch.setattr(loop, "call_soon_threadsafe", queue_callback)

    aio_fut = wrap_as_future(ref=object_ref_mock, timeout_s=0)

    fut.set_result("test")
    assert len(queued_callbacks) == 1

    await asyncio.sleep(0.001)

    assert aio_fut.done()
    with pytest.raises(TimeoutError):
        aio_fut.result()

    callback, args, _ = queued_callbacks.pop()
    callback(*args)


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
        (True, True),
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


def _make_unschedulable_wrapper(name: str) -> ActorProxyWrapper:
    """Create an ActorProxyWrapper around a real proxy pinned to a removed node.

    This goes through ActorProxyWrapper's own production creation path
    (`_get_or_create_proxy_actor`), which starts a real
    `ray.serve._private.proxy.ProxyActor` hard-pinned to its node via
    `label_selector` -- exactly how the controller pins proxies. Passing a node
    id that does not exist reproduces the production condition (a proxy pinned
    to a removed node), so the `ActorUnschedulableError` path is exercised
    end-to-end against real Ray/Serve rather than mocked. The proxy never
    schedules, so `ProxyActor.__init__` never runs; `check_health`/`shutdown`
    are the real methods `is_shutdown`/`kill` invoke.
    """
    # A syntactically valid but nonexistent node id (28-byte hex).
    nonexistent_node_id = "f" * 56
    return ActorProxyWrapper(
        logging_config=LoggingConfig(),
        http_options=HTTPOptions(),
        grpc_options=gRPCOptions(),
        name=name,
        node_id=nonexistent_node_id,
        node_ip_address="127.0.0.1",
    )


@ray.remote
class FakeShutdownProxy:
    """Helper actor that can sleep or raise a configured exception on shutdown."""

    def __init__(self, exc: Optional[Exception] = None, sleep_s: float = 0.0):
        self._exc = exc
        self._sleep_s = sleep_s

    def check_health(self):
        return True

    def shutdown(self):
        if self._sleep_s > 0:
            time.sleep(self._sleep_s)
        if self._exc is not None:
            raise self._exc
        return True

    def update_draining(self, draining):
        pass


def test_is_shutdown_true_when_actor_unschedulable(ray_shutdown):
    """End-to-end regression test for the proxy update loop hang.

    A proxy actor hard-pinned to a removed node becomes permanently
    unschedulable, and real Ray surfaces this from ``check_health`` as
    ``ActorUnschedulableError`` (a ``RayError`` that is *not* a
    ``RayActorError``). Before the fix, that exception propagated out of
    ``is_shutdown`` -> ``kill`` -> ``ProxyState.shutdown`` and wedged the
    controller's proxy update loop, leaving ``proxies: {}`` and all ingress
    returning 503.

    Ray first reports the health check as pending (``GetTimeoutError`` ->
    ``is_shutdown() is False``), then transitions to ``ActorUnschedulableError``
    once the scheduler gives up. ``is_shutdown()`` must converge to ``True``
    without ever raising.
    """
    ray.init(num_cpus=2)
    wrapper = _make_unschedulable_wrapper(name="test_is_shutdown_unschedulable")
    wait_for_condition(wrapper.is_shutdown, timeout=30)


def test_kill_does_not_raise_when_actor_unschedulable(ray_shutdown):
    """``kill`` calls ``is_shutdown`` first; on an unschedulable proxy it must
    short-circuit and return instead of propagating the exception.

    This is the exact call chain (``kill`` -> ``is_shutdown``) that hung the
    controller loop, so ``kill()`` must complete without raising.
    """
    ray.init(num_cpus=2)
    wrapper = _make_unschedulable_wrapper(name="test_kill_unschedulable")
    # Wait until the actor is actually unschedulable, then kill() is a no-op.
    wait_for_condition(wrapper.is_shutdown, timeout=30)
    wrapper.kill()  # must not raise


@pytest.mark.parametrize(
    "actor_kwargs,timeout_s,expected_level,expected_substring",
    [
        (
            {"exc": ActorUnschedulableError("simulated unschedulable error")},
            5.0,
            logging.INFO,
            "skipping graceful shutdown",
        ),
        (
            {"exc": RuntimeError("simulated runtime error")},
            5.0,
            logging.ERROR,
            "failed.",
        ),
        (
            {"sleep_s": 5.0},
            0.1,
            logging.WARNING,
            "timed out after",
        ),
    ],
)
def test_kill_error_logging_and_force_kill(
    ray_shutdown,
    caplog,
    monkeypatch,
    actor_kwargs,
    timeout_s,
    expected_level,
    expected_substring,
):
    """Verify kill() logs appropriate messages and force-kills the actor across
    unschedulable errors, unexpected RayErrors, and graceful shutdown timeouts.

    Note: In tests, remote exceptions arrive wrapped in RayTaskError, which matches
    except ActorUnschedulableError via as_instanceof_cause().
    """
    monkeypatch.setattr(proxy_state, "PROXY_GRACEFUL_SHUTDOWN_TIMEOUT_S", timeout_s)
    ray.init(num_cpus=2)
    actor_handle = FakeShutdownProxy.remote(**actor_kwargs)
    wrapper = ActorProxyWrapper(
        logging_config=LoggingConfig(),
        actor_handle=actor_handle,
        node_id="test_node_id",
    )

    caplog.set_level(logging.INFO, logger=SERVE_LOGGER_NAME)
    serve_logger = logging.getLogger(SERVE_LOGGER_NAME)
    serve_logger.addHandler(caplog.handler)
    try:
        wrapper.kill()
        assert any(
            record.levelno == expected_level
            and "test_node_id" in record.getMessage()
            and expected_substring in record.getMessage()
            for record in caplog.records
        )
    finally:
        serve_logger.removeHandler(caplog.handler)
    wait_for_condition(wrapper.is_shutdown, timeout=15)


def test_kill_success_path(ray_shutdown):
    """Verify ray.kill force-terminates the actor on the success path when
    graceful shutdown succeeds cleanly without raising."""
    ray.init(num_cpus=2)
    actor_handle = FakeShutdownProxy.remote()
    wrapper = ActorProxyWrapper(
        logging_config=LoggingConfig(),
        actor_handle=actor_handle,
        node_id="test_node_id",
    )
    wrapper.kill()
    wait_for_condition(wrapper.is_shutdown, timeout=15)


def test_kill_older_session_actor_handle(ray_shutdown, caplog):
    """Verify that calling kill() on an actor handle from a previous Ray session
    succeeds without raising an exception or logging ERROR-level stack traces."""
    ray.init(num_cpus=2)
    actor_handle = FakeShutdownProxy.remote()
    wrapper = ActorProxyWrapper(
        logging_config=LoggingConfig(),
        actor_handle=actor_handle,
        node_id="test_node_id",
    )

    ray.shutdown()
    ray.init(num_cpus=2)

    caplog.set_level(logging.INFO, logger=SERVE_LOGGER_NAME)
    serve_logger = logging.getLogger(SERVE_LOGGER_NAME)
    serve_logger.addHandler(caplog.handler)
    try:
        wrapper.kill()
        error_records = [
            r
            for r in caplog.records
            if r.levelno >= logging.ERROR and "test_node_id" in r.getMessage()
        ]
        assert len(error_records) == 0
    finally:
        serve_logger.removeHandler(caplog.handler)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
