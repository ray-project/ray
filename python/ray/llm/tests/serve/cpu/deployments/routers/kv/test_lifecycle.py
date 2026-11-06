"""Lifecycle-event delivery from engine replicas to LLMRouter replicas."""

import asyncio
import sys

import pytest
import zmq
import zmq.asyncio as zmq_asyncio

from ray._common.network_utils import find_free_port
from ray.llm._internal.serve.routing_policies.kv_aware import lifecycle
from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
    LifecycleCodec,
    LifecycleEventForwarder,
    LifecycleReceiver,
)


@pytest.fixture
def outcomes(monkeypatch):
    """The ``outcome`` tag of every event the forwarder counts."""
    recorded = []

    class _Counter:
        def inc(self, tags):
            recorded.append(tags["outcome"])

    monkeypatch.setattr(lifecycle, "_lifecycle_events_counter", _Counter())
    return recorded


async def _wait_for(predicate, timeout_s=5.0):
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "timed out"
        await asyncio.sleep(0.01)


def test_codec_round_trip():
    events = [("on_prefill_complete", ("r1",)), ("on_decode_progress", ("r1", 32))]
    assert LifecycleCodec.decode(LifecycleCodec.encode(events)) == events


# Invalid msgpack, and valid msgpack that isn't a list of [hook, args].
@pytest.mark.parametrize("payload", [b"\xc1", b"\x05"])
def test_codec_rejects_malformed_messages(payload):
    with pytest.raises(ValueError):
        LifecycleCodec.decode(payload)


async def _start_receiver():
    received = []

    async def handler(events):
        received.extend(events)

    receiver = LifecycleReceiver(handler=handler)
    port = await receiver.start()
    return receiver, f"tcp://127.0.0.1:{port}", received


_EVENTS = [
    ("on_prefill_complete", ("r1",)),
    ("on_decode_progress", ("r1", 32)),
    ("on_request_completed", ("r1",)),
]


@pytest.mark.asyncio
async def test_report_delivers_to_owner_in_order(outcomes):
    """Events for one owner arrive at its receiver in report order, including
    those reported before the PUSH socket finished connecting."""
    receiver, endpoint, received = await _start_receiver()
    forwarder = LifecycleEventForwarder()
    try:
        for name, args in _EVENTS:
            forwarder.report(endpoint, name, *args)
        await _wait_for(lambda: len(received) == len(_EVENTS))
        assert received == _EVENTS
        assert outcomes == ["pushed"] * len(_EVENTS)
    finally:
        forwarder.close()
        await receiver.close()


@pytest.mark.asyncio
async def test_report_queues_until_owner_listens():
    """Events reported before the owner binds are delivered once it does,
    rather than dropped."""
    endpoint = f"tcp://127.0.0.1:{find_free_port()}"
    forwarder = LifecycleEventForwarder()
    pull = zmq_asyncio.Context.instance().socket(zmq.PULL)
    try:
        for name, args in _EVENTS:
            forwarder.report(endpoint, name, *args)
        pull.bind(endpoint)
        received = []
        for _ in _EVENTS:
            payload = await asyncio.wait_for(pull.recv(), timeout=5)
            received.extend(LifecycleCodec.decode(payload))
        assert received == _EVENTS
    finally:
        forwarder.close()
        pull.close(linger=0)


def test_failed_push_is_dropped(outcomes):
    """An event whose push fails is dropped and counted, without raising."""
    forwarder = LifecycleEventForwarder()
    try:
        forwarder.report("not-an-endpoint", "on_request_completed", "r1")
        assert outcomes == ["dropped"]
    finally:
        forwarder.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
