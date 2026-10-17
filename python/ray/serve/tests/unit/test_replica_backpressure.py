import asyncio
import sys
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ray.exceptions import ActorDiedError, ActorUnavailableError
from ray.serve._private.common import RequestMetadata
from ray.serve._private.constants import HEALTHY_MESSAGE
from ray.serve._private.http_util import is_builtin_app_health_path
from ray.serve._private.replica import Replica
from ray.serve._private.router import AsyncioRouter
from ray.serve._private.utils import Semaphore
from ray.serve.exceptions import DeploymentUnavailableError


def _make_metadata() -> RequestMetadata:
    return RequestMetadata(
        request_id="test-request",
        internal_request_id="test-internal-request",
        is_direct_ingress=True,
    )


def _make_fake_replica(max_ongoing_requests: int):
    """Minimal stand-in exposing the real slot/queue accounting methods."""
    fake = MagicMock()
    fake._num_queued_requests = 0
    fake._semaphore = Semaphore(lambda: max_ongoing_requests)
    fake._start_request = Replica._start_request.__get__(fake, Replica)
    fake._track_queued_request = Replica._track_queued_request.__get__(fake, Replica)
    return fake


async def _wait_for_semaphore_waiter(semaphore: Semaphore):
    for _ in range(1000):
        await asyncio.sleep(0)
        if semaphore._waiters:
            return
    raise AssertionError("no coroutine blocked on the semaphore")


class TestTrackQueuedRequest:
    def test_release_decrements_exactly_once(self):
        fake = _make_fake_replica(max_ongoing_requests=1)

        with fake._track_queued_request() as release:
            assert fake._num_queued_requests == 1
            release()
            assert fake._num_queued_requests == 0
            # Extra calls are a no-op.
            release()
            assert fake._num_queued_requests == 0

        # Exiting the block must not decrement below zero.
        assert fake._num_queued_requests == 0

    @pytest.mark.asyncio
    async def test_count_released_when_cancelled_while_waiting(self):
        """A request cancelled while blocked on the slot is released on block
        exit, as the direct-ingress handlers wire it up."""
        fake = _make_fake_replica(max_ongoing_requests=1)

        # Hold the only slot so the request blocks while queued.
        await fake._semaphore.acquire()

        async def handler():
            with fake._track_queued_request() as release:
                async with fake._start_request(_make_metadata()):
                    release()

        task = asyncio.ensure_future(handler())
        await _wait_for_semaphore_waiter(fake._semaphore)
        assert fake._num_queued_requests == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert fake._num_queued_requests == 0


def test_builtin_app_health_path_does_not_match_system_route():
    assert is_builtin_app_health_path("/app", "/app/-/healthz")
    assert not is_builtin_app_health_path("/app", "/app/healthz")
    assert not is_builtin_app_health_path("/app", "/app/-/healthz/extra")
    assert not is_builtin_app_health_path("/app", "/-/healthz")
    assert not is_builtin_app_health_path("/", "/-/healthz")
    assert not is_builtin_app_health_path("/", "/healthz")
    assert not is_builtin_app_health_path("/application", "/app/-/healthz")
    assert not is_builtin_app_health_path("/app", "/apple/-/healthz")


class TestBuiltinAppHealth:
    @pytest.mark.asyncio
    async def test_skips_semaphore_when_saturated(self):
        fake = _make_fake_replica(max_ongoing_requests=1)
        await fake._semaphore.acquire()
        fake._shutting_down = False
        fake._deployment_id = SimpleNamespace(app_name="app", name="ingress")
        fake._deployment_config = SimpleNamespace(health_check_timeout_s=30)
        fake._metrics_manager = MagicMock()
        called = {"n": 0}

        async def check_health():
            called["n"] += 1

        fake.check_health = check_health
        sent = []

        async def send(msg):
            sent.append(msg)

        serve = Replica._serve_builtin_app_health.__get__(fake, Replica)
        await serve("/app/-/healthz", "GET", send, time.time())

        assert called["n"] == 1
        assert fake._semaphore.locked()
        assert not fake._semaphore._waiters
        assert sent[0]["status"] == 200
        assert sent[1]["body"] == HEALTHY_MESSAGE.encode()

    @pytest.mark.asyncio
    async def test_failure_does_not_take_a_slot(self):
        fake = _make_fake_replica(max_ongoing_requests=1)
        await fake._semaphore.acquire()
        fake._shutting_down = False
        fake._deployment_id = SimpleNamespace(app_name="app", name="ingress")
        fake._deployment_config = SimpleNamespace(health_check_timeout_s=30)
        fake._metrics_manager = MagicMock()

        async def check_health():
            raise RuntimeError("down")

        fake.check_health = check_health
        sent = []

        async def send(msg):
            sent.append(msg)

        serve = Replica._serve_builtin_app_health.__get__(fake, Replica)
        await serve("/app/-/healthz", "GET", send, time.time())

        assert fake._semaphore.locked()
        assert not fake._semaphore._waiters
        assert sent[0]["status"] == 503
        assert sent[1]["body"] == b"UNHEALTHY"

    @pytest.mark.asyncio
    async def test_hanging_check_health_returns_unhealthy(self):
        """A hung user check_health is unhealthy within health_check_timeout_s."""
        fake = _make_fake_replica(max_ongoing_requests=1)
        await fake._semaphore.acquire()
        fake._shutting_down = False
        fake._deployment_id = SimpleNamespace(app_name="app", name="ingress")
        fake._deployment_config = SimpleNamespace(health_check_timeout_s=0.05)
        fake._metrics_manager = MagicMock()

        async def check_health():
            await asyncio.sleep(30)

        fake.check_health = check_health
        sent = []

        async def send(msg):
            sent.append(msg)

        serve = Replica._serve_builtin_app_health.__get__(fake, Replica)
        started = time.monotonic()
        await serve("/app/-/healthz", "GET", send, time.time())
        elapsed = time.monotonic() - started

        assert elapsed < 2
        assert fake._semaphore.locked()
        assert not fake._semaphore._waiters
        assert sent[0]["status"] == 503
        assert sent[1]["body"] == b"UNHEALTHY"


class _HealthReplica:
    def __init__(self, replica_id, error=None):
        self.replica_id = replica_id
        self._error = error
        self.health_check_calls = 0

    async def check_health(self):
        self.health_check_calls += 1
        if self._error is not None:
            raise self._error


def _make_ingress_health_router(replicas):
    """Bind AsyncioRouter.check_ingress_health without starting long poll."""
    dropped = []
    unavailable = []
    fake = SimpleNamespace(
        _deployment_available=True,
        _deployment_config=None,
        deployment_id=SimpleNamespace(name="ingress", app_name="app"),
        _request_router_initialized=asyncio.Event(),
        _active_request_router=SimpleNamespace(
            curr_replicas={r.replica_id: r for r in replicas},
            on_replica_actor_died=dropped.append,
            on_replica_actor_unavailable=unavailable.append,
        ),
    )
    fake._request_router_initialized.set()
    fake.check_ingress_health = AsyncioRouter.check_ingress_health.__get__(
        fake, AsyncioRouter
    )
    return fake, dropped, unavailable


class TestCheckIngressHealthFallback:
    @pytest.mark.asyncio
    async def test_uses_first_running_replica(self):
        r1 = _HealthReplica("r1")
        r2 = _HealthReplica("r2")
        router, dropped, _ = _make_ingress_health_router([r1, r2])

        await router.check_ingress_health()

        assert r1.health_check_calls == 1
        assert r2.health_check_calls == 0
        assert dropped == []

    @pytest.mark.asyncio
    async def test_dead_first_replica_falls_through(self):
        r1 = _HealthReplica("r1", error=ActorDiedError())
        r2 = _HealthReplica("r2")
        router, dropped, _ = _make_ingress_health_router([r1, r2])

        await router.check_ingress_health()

        assert r1.health_check_calls == 1
        assert r2.health_check_calls == 1
        assert dropped == ["r1"]

    @pytest.mark.asyncio
    async def test_unavailable_first_replica_falls_through(self):
        r1 = _HealthReplica(
            "r1",
            error=ActorUnavailableError(error_message="unavailable", actor_id=None),
        )
        r2 = _HealthReplica("r2")
        router, _, unavailable = _make_ingress_health_router([r1, r2])

        await router.check_ingress_health()

        assert r1.health_check_calls == 1
        assert r2.health_check_calls == 1
        assert unavailable == ["r1"]

    @pytest.mark.asyncio
    async def test_all_replicas_stale_raises_unavailable(self):
        r1 = _HealthReplica("r1", error=ActorDiedError())
        router, dropped, _ = _make_ingress_health_router([r1])

        with pytest.raises(DeploymentUnavailableError):
            await router.check_ingress_health()

        assert dropped == ["r1"]

    @pytest.mark.asyncio
    async def test_ordinary_health_failure_does_not_fall_through(self):
        r1 = _HealthReplica("r1", error=RuntimeError("unhealthy"))
        r2 = _HealthReplica("r2")
        router, dropped, _ = _make_ingress_health_router([r1, r2])

        with pytest.raises(RuntimeError, match="unhealthy"):
            await router.check_ingress_health()

        assert r1.health_check_calls == 1
        assert r2.health_check_calls == 0
        assert dropped == []

    @pytest.mark.asyncio
    async def test_hanging_health_check_times_out_without_fallthrough(self):
        """Timeout uses health_check_timeout_s and does not try the next replica."""

        class _Hang:
            def __init__(self, replica_id):
                self.replica_id = replica_id
                self.health_check_calls = 0

            async def check_health(self):
                self.health_check_calls += 1
                await asyncio.sleep(30)

        r1 = _Hang("r1")
        r2 = _HealthReplica("r2")
        router, dropped, _ = _make_ingress_health_router([r1, r2])
        router._deployment_config = SimpleNamespace(health_check_timeout_s=0.05)

        started = time.monotonic()
        with pytest.raises(TimeoutError, match="timed out"):
            await router.check_ingress_health()
        elapsed = time.monotonic() - started

        assert elapsed < 2
        assert r1.health_check_calls == 1
        assert r2.health_check_calls == 0
        assert dropped == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
