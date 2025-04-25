import asyncio
import concurrent.futures
import time
from unittest.mock import Mock, patch

import pytest

from ray._private.test_utils import wait_for_condition
from ray.serve._private.router import SingletonThreadRouter
from ray.serve._private.common import DeploymentID, RequestMetadata
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.router import AsyncioRouter


def test_request_assignment():
    mock_replica_result = Mock(spec=ReplicaResult)

    mock_router = Mock(spec=AsyncioRouter)

    async def mock_assign_request(*args, **kwargs):
        return mock_replica_result

    mock_router.assign_request.side_effect = mock_assign_request

    with patch("ray.serve._private.router.AsyncioRouter", return_value=mock_router):
        router = SingletonThreadRouter(
            controller_handle=Mock(),
            deployment_id=DeploymentID(name="test", app_name="test"),
            handle_id="test",
            self_actor_id="test",
            handle_source="test",
            replica_scheduler=Mock(),
            enable_strict_max_ongoing_requests=False,
            resolve_request_arg_func=Mock(),
        )

        request_meta = RequestMetadata(
            request_id="test",
            internal_request_id="test",
        )

        future = router.assign_request(request_meta)
        assert isinstance(future, concurrent.futures.Future)
        assert future.result() == mock_replica_result


@pytest.mark.asyncio
async def test_cancellation_propagation():
    mock_replica_result = Mock(spec=ReplicaResult)
    mock_replica_result.cancel = Mock()

    loop = SingletonThreadRouter._get_singleton_asyncio_loop()

    async def init_events():
        return asyncio.Event(), asyncio.Event()

    f = asyncio.run_coroutine_threadsafe(init_events(), loop)
    lock_acquire_event, cancel_block_event = f.result()

    mock_router = Mock(spec=AsyncioRouter)

    async def mock_assign_request(*args, **kwargs):
        try:
            await lock_acquire_event.wait()
            return mock_replica_result
        except asyncio.CancelledError:
            cancel_block_event.set()
            raise

    mock_router.assign_request.side_effect = mock_assign_request

    with patch("ray.serve._private.router.AsyncioRouter", return_value=mock_router):
        router = SingletonThreadRouter(
            controller_handle=Mock(),
            deployment_id=DeploymentID(name="test", app_name="test"),
            handle_id="test",
            self_actor_id="test",
            handle_source="test",
            replica_scheduler=Mock(),
            enable_strict_max_ongoing_requests=False,
        )

        request_meta = RequestMetadata(
            request_id="test",
            internal_request_id="test",
        )

        assign_request_future = router.assign_request(request_meta)
        assign_request_future.cancel()

        async def coro():
            await cancel_block_event.wait()

        asyncio.run_coroutine_threadsafe(coro(), loop).result()

        def release_lock():
            lock_acquire_event.set()

        loop.call_soon_threadsafe(release_lock)

        with pytest.raises(concurrent.futures.CancelledError):
            assign_request_future.result()

        # ensure call count remains 0 after 1 second
        await asyncio.sleep(1)
        assert mock_replica_result.cancel.call_count == 0


@pytest.mark.asyncio
async def test_replica_result_cancellation():
    mock_replica_result = Mock(spec=ReplicaResult)
    mock_replica_result.cancel = Mock()

    mock_router = Mock(spec=AsyncioRouter)

    async def mock_assign_request(*args, **kwargs):
        time.sleep(1)
        return mock_replica_result

    mock_router.assign_request.side_effect = mock_assign_request

    with patch("ray.serve._private.router.AsyncioRouter", return_value=mock_router):
        router = SingletonThreadRouter(
            controller_handle=Mock(),
            deployment_id=DeploymentID(name="test", app_name="test"),
            handle_id="test",
            self_actor_id="test",
            handle_source="test",
            replica_scheduler=Mock(),
            enable_strict_max_ongoing_requests=False,
        )

        request_meta = RequestMetadata(
            request_id="test",
            internal_request_id="test",
        )

        assign_request_future = router.assign_request(request_meta)
        assign_request_future.cancel()

        with pytest.raises(concurrent.futures.CancelledError):
            assign_request_future.result()

        wait_for_condition(lambda: mock_replica_result.cancel.call_count == 1)


def test_assign_request_with_exception():
    mock_replica_result = Mock(spec=ReplicaResult)
    mock_replica_result.cancel = Mock()

    mock_router = Mock(spec=AsyncioRouter)

    async def mock_assign_request(*args, **kwargs):
        raise Exception("test exception")

    mock_router.assign_request.side_effect = mock_assign_request

    with patch("ray.serve._private.router.AsyncioRouter", return_value=mock_router):
        router = SingletonThreadRouter(
            controller_handle=Mock(),
            deployment_id=DeploymentID(name="test", app_name="test"),
            handle_id="test",
            self_actor_id="test",
            handle_source="test",
            replica_scheduler=Mock(),
            enable_strict_max_ongoing_requests=False,
        )

        request_meta = RequestMetadata(
            request_id="test",
            internal_request_id="test",
        )

        assign_request_future = router.assign_request(request_meta)

        assert assign_request_future.exception() is not None
        assert str(assign_request_future.exception()) == "test exception"


def test_assign_request_with_exception_during_cancellation():
    mock_replica_result = Mock(spec=ReplicaResult)
    mock_replica_result.cancel = Mock()

    loop = SingletonThreadRouter._get_singleton_asyncio_loop()

    async def init_events():
        return asyncio.Event(), asyncio.Event()

    f = asyncio.run_coroutine_threadsafe(init_events(), loop)
    lock_acquire_event, cancel_block_event = f.result()

    mock_router = Mock(spec=AsyncioRouter)

    async def mock_assign_request(*args, **kwargs):
        try:
            await lock_acquire_event.wait()
            return mock_replica_result
        except asyncio.CancelledError:
            cancel_block_event.set()
            raise Exception("test exception")

    mock_router.assign_request.side_effect = mock_assign_request

    with patch("ray.serve._private.router.AsyncioRouter", return_value=mock_router):
        router = SingletonThreadRouter(
            controller_handle=Mock(),
            deployment_id=DeploymentID(name="test", app_name="test"),
            handle_id="test",
            self_actor_id="test",
            handle_source="test",
            replica_scheduler=Mock(),
            enable_strict_max_ongoing_requests=False,
        )

        request_meta = RequestMetadata(
            request_id="test",
            internal_request_id="test",
        )

        assign_request_future = router.assign_request(request_meta)
        assign_request_future.cancel()

        async def coro():
            await cancel_block_event.wait()

        asyncio.run_coroutine_threadsafe(coro(), loop).result()

        def release_lock():
            lock_acquire_event.set()

        loop.call_soon_threadsafe(release_lock)

        assert assign_request_future.cancelled() is True
        with pytest.raises(concurrent.futures.CancelledError):
            assign_request_future.exception()


def test_shutdown():
    mock_router = Mock(spec=AsyncioRouter)

    async def mock_shutdown(*args, **kwargs):
        return

    mock_router.shutdown.side_effect = mock_shutdown

    with patch("ray.serve._private.router.AsyncioRouter", return_value=mock_router):
        router = SingletonThreadRouter(
            controller_handle=Mock(),
            deployment_id=DeploymentID(name="test", app_name="test"),
            handle_id="test",
            self_actor_id="test",
            handle_source="test",
            replica_scheduler=Mock(),
            enable_strict_max_ongoing_requests=False,
        )

        shutdown_future = router.shutdown()
        assert isinstance(shutdown_future, concurrent.futures.Future)
        assert shutdown_future.result() is None

        assert mock_router.shutdown.call_count == 1
