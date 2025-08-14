import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import (
    SignalActor,
    async_wait_for_condition,
    wait_for_condition,
)
from ray.serve._private.constants import RAY_SERVE_FORCE_LOCAL_TESTING_MODE
from ray.serve._private.test_utils import send_signal_on_cancellation, tlog
from ray.serve.exceptions import RequestCancelledError


def test_cancel_sync_handle_call_during_execution(serve_instance):
    """Test cancelling handle request during execution (sync context)."""
    running_signal_actor = SignalActor.remote()
    cancelled_signal_actor = SignalActor.remote()

    @serve.deployment
    class Ingress:
        async def __call__(self, *args):
            async with send_signal_on_cancellation(cancelled_signal_actor):
                await running_signal_actor.send.remote()

    h = serve.run(Ingress.bind())

    # Send a request and wait for it to start executing.
    r = h.remote()
    ray.get(running_signal_actor.wait.remote(), timeout=10)

    # Cancel it and verify that it is cancelled via signal.
    r.cancel()
    ray.get(cancelled_signal_actor.wait.remote(), timeout=10)

    with pytest.raises(RequestCancelledError):
        r.result()


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't have assignment/execution split",
)
def test_cancel_sync_handle_call_during_assignment(serve_instance):
    """Test cancelling handle request during assignment (sync context)."""
    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Ingress:
        def __init__(self):
            self._num_requests = 0

        async def __call__(self, *args):
            self._num_requests += 1
            await signal_actor.wait.remote()

            return self._num_requests

    h = serve.run(Ingress.bind())

    # Send a request and wait for it to be ongoing so we know that further requests
    # will block trying to assign a replica.
    initial_response = h.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    # Make a second request, cancel it, and verify that it is cancelled.
    second_response = h.remote()
    second_response.cancel()
    with pytest.raises(RequestCancelledError):
        second_response.result()

    # Now signal the initial request to finish and check that the second request
    # never reached the replica.
    ray.get(signal_actor.send.remote())
    assert initial_response.result() == 1
    for i in range(2, 12):
        assert h.remote().result() == i


def test_cancel_async_handle_call_during_execution(serve_instance):
    """Test cancelling handle request during execution (async context)."""
    running_signal_actor = SignalActor.remote()
    cancelled_signal_actor = SignalActor.remote()

    @serve.deployment
    class Downstream:
        async def __call__(self, *args):
            async with send_signal_on_cancellation(cancelled_signal_actor):
                await running_signal_actor.send.remote()

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle

        async def __call__(self, *args):
            # Send a request and wait for it to start executing.
            r = self._h.remote()
            await running_signal_actor.wait.remote()

            # Cancel it and verify that it is cancelled via signal.
            r.cancel()
            await cancelled_signal_actor.wait.remote()

            with pytest.raises(RequestCancelledError):
                await r

    h = serve.run(Ingress.bind(Downstream.bind()))
    h.remote().result()  # Would raise if test failed.


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't have assignment/execution split",
)
def test_cancel_async_handle_call_during_assignment(serve_instance):
    """Test cancelling handle request during assignment (async context)."""
    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Downstream:
        def __init__(self):
            self._num_requests = 0

        async def __call__(self, *args):
            self._num_requests += 1
            await signal_actor.wait.remote()

            return self._num_requests

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle

        async def __call__(self, *args):
            # Send a request and wait for it to be ongoing so we know that further
            # requests will block trying to assign a replica.
            initial_response = self._h.remote()

            async def one_waiter():
                return await signal_actor.cur_num_waiters.remote() == 1

            await async_wait_for_condition(one_waiter)

            # Make a second request, cancel it, and verify that it is cancelled.
            second_response = self._h.remote()
            second_response.cancel()
            with pytest.raises(RequestCancelledError):
                await second_response

            # Now signal the initial request to finish and check that the second request
            # never reached the replica.
            await signal_actor.send.remote()
            assert await initial_response == 1
            for i in range(2, 12):
                assert await self._h.remote() == i

    h = serve.run(Ingress.bind(Downstream.bind()))
    h.remote().result()  # Would raise if test failed.


def test_cancel_generator_sync(serve_instance):
    """Test cancelling streaming handle request during execution."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Ingress:
        async def __call__(self, *args):
            yield "hi"
            async with send_signal_on_cancellation(signal_actor):
                pass

    h = serve.run(Ingress.bind()).options(stream=True)

    # Send a request and wait for it to start executing.
    g = h.remote()

    assert next(g) == "hi"

    # Cancel it and verify that it is cancelled via signal.
    g.cancel()

    with pytest.raises(RequestCancelledError):
        next(g)

    ray.get(signal_actor.wait.remote(), timeout=10)


def test_cancel_generator_async(serve_instance):
    """Test cancelling streaming handle request during execution."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Downstream:
        async def __call__(self, *args):
            yield "hi"
            async with send_signal_on_cancellation(signal_actor):
                pass

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle.options(stream=True)

        async def __call__(self, *args):
            # Send a request and wait for it to start executing.
            g = self._h.remote()
            assert await g.__anext__() == "hi"

            # Cancel it and verify that it is cancelled via signal.
            g.cancel()

            with pytest.raises(RequestCancelledError):
                await g.__anext__()

            await signal_actor.wait.remote()

    h = serve.run(Ingress.bind(Downstream.bind()))
    h.remote().result()  # Would raise if test failed.


def test_only_relevant_task_is_cancelled(serve_instance):
    """Test cancelling one request doesn't affect others."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Ingress:
        async def __call__(self, *args):
            await signal_actor.wait.remote()
            return "ok"

    h = serve.run(Ingress.bind())

    r1 = h.remote()
    r2 = h.remote()

    # Wait for both requests to be executing.
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 2)

    r1.cancel()
    with pytest.raises(RequestCancelledError):
        r1.result()

    # Now signal r2 to run to completion and check that it wasn't cancelled.
    ray.get(signal_actor.send.remote())
    assert r2.result() == "ok"


def test_out_of_band_task_is_not_cancelled(serve_instance):
    """
    Test cancelling a request doesn't cancel tasks submitted
    outside the request's context.
    """
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Downstream:
        async def hi(self):
            await signal_actor.wait.remote()
            return "ok"

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._h = handle
            self._out_of_band_req = self._h.hi.remote()

        async def __call__(self, *args):
            await self._h.hi.remote()

        async def get_out_of_band_response(self):
            return await self._out_of_band_req

    h = serve.run(Ingress.bind(Downstream.bind()))

    # Send a request, wait for downstream request to start, and cancel it.
    r1 = h.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 2)

    r1.cancel()
    with pytest.raises(RequestCancelledError):
        r1.result()

    # Now signal out of band request to run to completion and check that it wasn't
    # cancelled.
    ray.get(signal_actor.send.remote())
    assert h.get_out_of_band_response.remote().result() == "ok"


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't implement recursive cancellation",
)
def test_recursive_cancellation_during_execution(serve_instance):
    inner_signal_actor = SignalActor.remote()
    outer_signal_actor = SignalActor.remote()

    @serve.deployment
    async def inner():
        async with send_signal_on_cancellation(inner_signal_actor):
            pass

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._handle = handle

        async def __call__(self):
            _ = self._handle.remote()
            async with send_signal_on_cancellation(outer_signal_actor):
                pass

    h = serve.run(Ingress.bind(inner.bind()))

    resp = h.remote()
    with pytest.raises(TimeoutError):
        resp.result(timeout_s=0.5)

    resp.cancel()
    ray.get(inner_signal_actor.wait.remote(), timeout=10)
    ray.get(outer_signal_actor.wait.remote(), timeout=10)


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't implement recursive cancellation",
)
def test_recursive_cancellation_during_assignment(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Counter:
        def __init__(self):
            self._count = 0

        async def __call__(self):
            self._count += 1
            await signal.wait.remote()

        def get_count(self):
            return self._count

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._handle = handle

        async def __call__(self):
            self._handle.remote()
            await signal.wait.remote()
            return "hi"

        async def get_count(self):
            return await self._handle.get_count.remote()

        async def check_requests_pending_assignment_cache(self):
            requests_pending_assignment = ray.serve.context._requests_pending_assignment
            return {k: list(v.keys()) for k, v in requests_pending_assignment.items()}

    h = serve.run(Ingress.bind(Counter.bind()))

    # Send two requests to Ingress. The second should be queued and
    # pending assignment at Ingress because max ongoing requests for
    # Counter is only 1.
    tlog("Sending two requests to Ingress.")
    resp1 = h.remote()
    with pytest.raises(TimeoutError):
        resp1.result(timeout_s=0.5)
    resp2 = h.remote()
    with pytest.raises(TimeoutError):
        resp2.result(timeout_s=0.5)

    # Cancel second request, which should be pending assignment.
    tlog("Canceling second request.")
    resp2.cancel()

    # Release signal so that the first request can complete, and any new
    # requests to Counter can be let through
    tlog("Releasing signal.")
    ray.get(signal.send.remote())
    assert resp1.result() == "hi"

    # The second request, even though it was pending assignment to a
    # Counter replica, should have been properly canceled. Confirm this
    # by making sure that no more calls to __call__ were made
    for _ in range(10):
        assert h.get_count.remote().result() == 1

    tlog("Confirmed second request was properly canceled.")

    # Check that cache was cleared so there are no memory leaks
    requests_pending_assignment = (
        h.check_requests_pending_assignment_cache.remote().result()
    )
    for k, v in requests_pending_assignment.items():
        assert len(v) == 0, f"Request {k} has in flight requests in cache: {v}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
