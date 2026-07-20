import asyncio
import pickle
import sys
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from ray.serve._private.common import ReplicaHealthFrame
from ray.serve._private.replica import Replica
from ray.serve._private.replica_result import ActorReplicaResult
from ray.serve._private.request_router.common import PendingRequest


def _metadata(**kwargs):
    from ray.serve._private.common import RequestMetadata

    return RequestMetadata(request_id="r", internal_request_id="i", **kwargs)


class _FakeManager:
    """Just enough of ReplicaMetricsManager for the frame generator."""

    def __init__(self, period_s=0.02, carries=False):
        self._self_health_period_s = period_s
        self.snapshot = (True, None, 0)
        self.carries = carries

    def reports_carry_health(self):
        return self.carries

    @property
    def self_health_period_s(self):
        return self._self_health_period_s

    def self_health_frame(self):
        healthy, checked_at, failures = self.snapshot
        if checked_at is None:
            return None
        return ReplicaHealthFrame(
            healthy=healthy,
            health_checked_at=checked_at,
            health_consecutive_failures=failures,
        )


def _fake_replica(manager, user_coro_fn):
    return SimpleNamespace(
        _metrics_manager=manager,
        _user_callable_wrapper=SimpleNamespace(
            call_user_method=lambda md, args, kwargs: user_coro_fn()
        ),
    )


async def _drain(gen):
    items = []
    async for item in gen:
        items.append(item)
    return items


class TestUnaryWithHealthFrames:
    """Replica-side generator: frames interleave while the user call is pending."""

    @pytest.mark.asyncio
    async def test_frames_then_result(self):
        manager = _FakeManager()
        release = asyncio.Event()

        async def user():
            await release.wait()
            return "result"

        replica = _fake_replica(manager, user)
        manager.snapshot = (True, 100.0, 0)

        async def release_later():
            await asyncio.sleep(0.1)
            manager.snapshot = (True, 101.0, 0)
            await asyncio.sleep(0.1)
            release.set()

        task = asyncio.ensure_future(release_later())
        items = await _drain(
            Replica._call_unary_with_health_frames(replica, _metadata(), (), {})
        )
        await task

        assert items[-1] == "result"
        frames = [i for i in items[:-1] if isinstance(i, ReplicaHealthFrame)]
        assert len(items[:-1]) == len(frames) >= 2
        # Throttled: one frame per distinct checked_at, in order.
        assert [f.health_checked_at for f in frames] == [100.0, 101.0]

    @pytest.mark.asyncio
    async def test_fast_request_yields_only_result(self):
        manager = _FakeManager(period_s=10.0)
        manager.snapshot = (True, 100.0, 0)

        async def user():
            return "fast"

        items = await _drain(
            Replica._call_unary_with_health_frames(
                _fake_replica(manager, user), _metadata(), (), {}
            )
        )
        assert items == ["fast"]

    @pytest.mark.asyncio
    async def test_no_frames_before_first_self_check(self):
        manager = _FakeManager()  # snapshot checked_at is None
        release = asyncio.Event()

        async def user():
            await release.wait()
            return "r"

        asyncio.get_running_loop().call_later(0.1, release.set)
        items = await _drain(
            Replica._call_unary_with_health_frames(
                _fake_replica(manager, user), _metadata(), (), {}
            )
        )
        assert items == ["r"]

    @pytest.mark.asyncio
    async def test_no_frames_while_reports_carry_health(self):
        manager = _FakeManager(carries=True)
        manager.snapshot = (True, 100.0, 0)
        release = asyncio.Event()

        async def user():
            await release.wait()
            return "r"

        asyncio.get_running_loop().call_later(0.15, release.set)
        items = await _drain(
            Replica._call_unary_with_health_frames(
                _fake_replica(manager, user), _metadata(), (), {}
            )
        )
        assert items == ["r"]

    @pytest.mark.asyncio
    async def test_user_exception_propagates(self):
        manager = _FakeManager()

        async def user():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await _drain(
                Replica._call_unary_with_health_frames(
                    _fake_replica(manager, user), _metadata(), (), {}
                )
            )


class _FakeGen:
    """Stands in for a ray ObjectRefGenerator: __anext__ pops awaitable 'refs'."""

    def __init__(self, futures):
        self._futures = list(futures)

    async def __anext__(self):
        if not self._futures:
            raise StopAsyncIteration
        fut = self._futures.pop(0)
        # Yield control like a real gen so pending futures can be filled.
        while not fut.done():
            await asyncio.sleep(0.01)
        return fut


def _done_future(value=None, exception=None):
    fut = asyncio.get_running_loop().create_future()
    if exception is not None:
        fut.set_exception(exception)
    else:
        fut.set_result(value)
    return fut


def _result_wrapper(gen, frames_possible=True):
    r = ActorReplicaResult.__new__(ActorReplicaResult)
    r._obj_ref = None
    r._obj_ref_gen = gen
    r._is_streaming = False
    r._request_id = "r"
    r._object_ref_or_gen_sync_lock = threading.Lock()
    r._with_rejection = True
    r._rejection_response = None
    r._health_frames_possible = frames_possible
    r._on_health_frame = None
    r._frame_pump_task = None
    r._pump_outcome = None
    r._consumption_started = False
    return r


class TestFramePump:
    """Router-side pump: drains frames in the background, then holds the result."""

    @pytest.mark.asyncio
    async def test_pump_records_frames_and_get_async_returns_result(self):
        frame1 = ReplicaHealthFrame(healthy=True, health_checked_at=1.0)
        frame2 = ReplicaHealthFrame(healthy=False, health_checked_at=2.0)
        gen = _FakeGen(
            [_done_future(frame1), _done_future(frame2), _done_future("result")]
        )
        r = _result_wrapper(gen)
        seen = []
        r._on_health_frame = seen.append

        r._maybe_start_frame_pump(asyncio.get_running_loop())
        assert r._pump_outcome is not None
        assert (await r.get_async()) == "result"
        assert seen == [frame1, frame2]

    @pytest.mark.asyncio
    async def test_pump_settles_error_ref_and_get_async_raises(self):
        gen = _FakeGen(
            [
                _done_future(ReplicaHealthFrame(healthy=True, health_checked_at=1.0)),
                _done_future(exception=ValueError("boom")),
            ]
        )
        r = _result_wrapper(gen)
        r._maybe_start_frame_pump(asyncio.get_running_loop())
        with pytest.raises(ValueError, match="boom"):
            await r.get_async()

    @pytest.mark.asyncio
    async def test_to_object_ref_async_waits_on_pump(self):
        result_fut = asyncio.get_running_loop().create_future()
        gen = _FakeGen([result_fut])
        r = _result_wrapper(gen)
        r._maybe_start_frame_pump(asyncio.get_running_loop())
        asyncio.get_running_loop().call_later(0.05, result_fut.set_result, "late")
        ref = await r.to_object_ref_async()
        assert (await ref) == "late"

    @pytest.mark.asyncio
    async def test_direct_consumption_filters_frames_without_pump(self):
        frame = ReplicaHealthFrame(healthy=True, health_checked_at=1.0)
        gen = _FakeGen([_done_future(frame), _done_future("direct")])
        r = _result_wrapper(gen)
        seen = []
        r._on_health_frame = seen.append
        assert (await r.get_async()) == "direct"
        assert seen == [frame]
        # Consumption marked; a late pump start must be a no-op.
        r._maybe_start_frame_pump(asyncio.get_running_loop())
        assert r._pump_outcome is None

    @pytest.mark.asyncio
    async def test_listener_noop_when_frames_not_possible(self):
        gen = _FakeGen([_done_future("x")])
        r = _result_wrapper(gen, frames_possible=False)
        r.set_health_frame_listener(lambda f: None)
        assert r._on_health_frame is None
        assert (await r.get_async()) == "x"


class TestSupportsHealthFramesGate:
    """The actor transport advertises frame support for unary rejection calls only."""

    def _send(self, metadata, with_rejection=True):
        # Resolve the module fresh: other unit suites reload serve modules,
        # so a collection-time import can go stale.
        import importlib

        replica_wrapper_mod = importlib.import_module(
            "ray.serve._private.request_router.replica_wrapper"
        )
        captured = {}

        def remote(pickled_md, *args, **kwargs):
            captured["metadata"] = pickle.loads(pickled_md)
            return "obj_ref_gen"

        endpoint = SimpleNamespace(remote=remote)
        handle = SimpleNamespace(
            handle_request_with_rejection=SimpleNamespace(
                options=lambda **kw: endpoint
            ),
            handle_request_streaming=SimpleNamespace(options=lambda **kw: endpoint),
            handle_request=endpoint,
        )
        wrapper = replica_wrapper_mod.ActorReplicaWrapper(handle)
        rr = mock.MagicMock()
        # Patch the name where the method actually resolves it (its own
        # __globals__) -- other unit suites shuffle module identities, so
        # patching the sys.modules entry can miss.
        with mock.patch.dict(
            wrapper.send_request_python.__func__.__globals__,
            {"ActorReplicaResult": rr},
        ):
            wrapper.send_request_python(
                PendingRequest(args=[], kwargs={}, metadata=metadata),
                with_rejection=with_rejection,
            )
            constructed_md = rr.call_args.args[1]
        return captured["metadata"], constructed_md

    # async: PendingRequest's future field needs a running event loop.
    @pytest.mark.asyncio
    async def test_unary_rejection_sets_flag(self):
        sent, constructed = self._send(_metadata())
        assert sent.supports_health_frames
        assert constructed.supports_health_frames

    @pytest.mark.asyncio
    async def test_streaming_rejection_does_not(self):
        sent, _ = self._send(_metadata(is_streaming=True))
        assert not sent.supports_health_frames

    @pytest.mark.asyncio
    async def test_no_rejection_does_not(self):
        sent, _ = self._send(_metadata(), with_rejection=False)
        assert not sent.supports_health_frames


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
