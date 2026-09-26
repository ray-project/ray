"""Real-actor regression tests for optional LongPollClient host replacement.

These tests need no Serve deployment, HAProxy process, or external service. The
host fixture embeds the real LongPollHost; only its published versions and data
are controlled so that cross-incarnation version collisions are deterministic.
"""

import asyncio
import threading
import time
import uuid

import pytest
import pytest_asyncio

import ray
import ray.serve._private.long_poll as long_poll_module
from ray.serve._private.long_poll import LongPollClient, LongPollHost

WAIT_TIMEOUT_S = 30


async def wait_until(predicate, timeout=WAIT_TIMEOUT_S):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Condition did not become true before the deadline")
        await asyncio.sleep(0.01)


async def actor_result(ref):
    return await asyncio.wait_for(ref, timeout=WAIT_TIMEOUT_S)


@ray.remote(num_cpus=0)
class ReconnectHost:
    def __init__(self, values, snapshot_id=None):
        self.incarnation = uuid.uuid4().hex
        self.host = LongPollHost(listen_for_change_request_timeout_s=(0.05, 0.1))
        self.requests = []
        self.active = 0
        self.max_active = 0
        self.publish(values, snapshot_id)

    def publish(self, values, snapshot_id=None):
        self.host.notify_changed(values)
        if snapshot_id is not None:
            for key in values:
                self.host.snapshot_ids[key] = snapshot_id

    async def listen_for_change(self, snapshot_ids):
        self.requests.append(dict(snapshot_ids))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            return await self.host.listen_for_change(snapshot_ids)
        finally:
            self.active -= 1

    def stats(self):
        return {
            "incarnation": self.incarnation,
            "requests": self.requests,
            "active": self.active,
            "max_active": self.max_active,
            "snapshot_ids": self.host.snapshot_ids,
        }

    def notify_long_poll_client_disabled(self, client_id, reason):
        self.host.notify_client_disabled(client_id, reason)


@pytest.fixture(scope="module")
def reconnect_ray():
    # When run alone, own a small local runtime. Never shut down a runtime owned
    # by another fixture when this module is included in a larger test command.
    owns_runtime = not ray.is_initialized()
    if owns_runtime:
        ray.init(address="local", num_cpus=2, include_dashboard=False)
    yield
    if owns_runtime:
        ray.shutdown()


class Hosts:
    def __init__(self):
        self.namespace = f"long-poll-reconnect-{uuid.uuid4().hex}"
        self.actors = []
        self.clients = []
        self.releases = []

    async def create(self, name, values, snapshot_id=7, max_restarts=0):
        actor = ReconnectHost.options(
            name=name,
            namespace=self.namespace,
            max_restarts=max_restarts,
            max_task_retries=-1 if max_restarts else 0,
        ).remote(values, snapshot_id)
        self.actors.append(actor)
        await actor_result(actor.stats.remote())
        return actor

    def resolver(self, name):
        return lambda: ray.get_actor(name, namespace=self.namespace)

    def client(self, actor, listeners, resolver=None):
        kwargs = {} if resolver is None else {"host_actor_resolver": resolver}
        client = LongPollClient(
            actor,
            listeners,
            call_in_event_loop=asyncio.get_running_loop(),
            client_id=f"test-reconnect-{uuid.uuid4().hex}",
            **kwargs,
        )
        self.clients.append(client)
        return client

    async def retire(self, actor, name):
        await asyncio.to_thread(ray.kill, actor, no_restart=True)

        def absent():
            try:
                ray.get_actor(name, namespace=self.namespace)
            except ValueError:
                return True
            return False

        deadline = asyncio.get_running_loop().time() + WAIT_TIMEOUT_S
        while not await asyncio.to_thread(absent):
            assert asyncio.get_running_loop().time() < deadline, "Name still occupied"
            await asyncio.sleep(0.01)

    def blocking_resolver(self, name):
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        calls = []
        self.releases.append(release)

        def resolve():
            calls.append(time.monotonic())
            entered.set()
            try:
                if not release.wait(WAIT_TIMEOUT_S):
                    raise RuntimeError("Test did not release the blocked resolver")
                return ray.get_actor(name, namespace=self.namespace)
            finally:
                finished.set()

        return resolve, entered, release, finished, calls

    async def close(self):
        for client in self.clients:
            client.stop()
        for release in self.releases:
            release.set()
        # Let event-loop cancellation and late Ray completions see stopped state
        # before the fixture's loop is closed.
        await asyncio.sleep(0.05)
        for actor in self.actors:
            try:
                await asyncio.to_thread(ray.kill, actor, no_restart=True)
            except ray.exceptions.RayError:
                pass
        await asyncio.sleep(0.05)


@pytest_asyncio.fixture
async def hosts(reconnect_ray, monkeypatch):
    monkeypatch.setattr(long_poll_module, "LONG_POLL_HOST_RETRY_DELAY_S", 0.05)
    hosts = Hosts()
    try:
        yield hosts
    finally:
        await hosts.close()


@pytest.mark.asyncio
async def test_named_replacement_with_equal_snapshot_id(hosts):
    old = await hosts.create("controller", {"state": "old"})
    received = []
    client = hosts.client(old, {"state": received.append}, hosts.resolver("controller"))
    await wait_until(lambda: received == ["old"])

    await hosts.retire(old, "controller")
    new = await hosts.create("controller", {"state": "new"})
    assert old._actor_id != new._actor_id
    await wait_until(lambda: received == ["old", "new"])

    stats = await actor_result(new.stats.remote())
    assert stats["requests"][0] == {"state": -1}
    assert stats["max_active"] == 1
    assert client.host_actor._actor_id == new._actor_id
    assert client.is_running


@pytest.mark.asyncio
async def test_named_host_becomes_available_after_several_lookups(hosts):
    old = await hosts.create("controller", {"state": "old"})
    received = []
    calls = []

    def resolve():
        calls.append(time.monotonic())
        return ray.get_actor("controller", namespace=hosts.namespace)

    client = hosts.client(old, {"state": received.append}, resolve)
    await wait_until(lambda: received == ["old"])
    await hosts.retire(old, "controller")
    await wait_until(lambda: len(calls) >= 3)
    assert received == ["old"]
    assert client.is_running
    # A zero-delay retry loop would violate this very loose lower bound. The
    # configured minimum is 25ms; no upper latency/SLA is asserted.
    assert all(b - a >= 0.01 for a, b in zip(calls, calls[1:]))

    await hosts.create("controller", {"state": "new"})
    await wait_until(lambda: received == ["old", "new"])


@pytest.mark.asyncio
async def test_same_actor_id_restart_preserves_existing_ray_retry_behavior(hosts):
    actor = await hosts.create(
        "controller", {"state": "old"}, snapshot_id=None, max_restarts=1
    )
    before = await actor_result(actor.stats.remote())
    received = []
    resolver_calls = []

    def unexpected_resolver():
        resolver_calls.append(True)
        return actor

    client = hosts.client(actor, {"state": received.append}, unexpected_resolver)
    await wait_until(lambda: received == ["old"])
    original_id = actor._actor_id
    await asyncio.to_thread(ray.kill, actor, no_restart=False)
    deadline = asyncio.get_running_loop().time() + WAIT_TIMEOUT_S
    while True:
        after = await actor_result(actor.stats.remote())
        if after["incarnation"] != before["incarnation"]:
            break
        assert asyncio.get_running_loop().time() < deadline, "Actor did not restart"
        await asyncio.sleep(0.01)
    assert after["incarnation"] != before["incarnation"]
    assert actor._actor_id == original_id
    await actor_result(
        actor.publish.remote(
            {"state": "restarted"}, before["snapshot_ids"]["state"] + 1
        )
    )
    await wait_until(lambda: received[-1] == "restarted")
    assert resolver_calls == []
    assert client.is_running


@pytest.mark.asyncio
async def test_default_client_stops_without_resolving_replacement(hosts):
    old = await hosts.create("controller", {"state": "old"})
    received = []
    client = hosts.client(old, {"state": received.append})
    await wait_until(lambda: received == ["old"])
    await hosts.retire(old, "controller")
    await wait_until(lambda: not client.is_running)
    new = await hosts.create("controller", {"state": "new"})
    await asyncio.sleep(0.2)
    assert received == ["old"]
    assert (await actor_result(new.stats.remote()))["requests"] == []


@pytest.mark.asyncio
async def test_one_client_survives_repeated_named_replacements(hosts):
    actor = await hosts.create("controller", {"state": 0})
    received = []
    client = hosts.client(
        actor, {"state": received.append}, hosts.resolver("controller")
    )
    await wait_until(lambda: received == [0])
    actor_ids = [actor._actor_id]
    for value in (1, 2, 3):
        await hosts.retire(actor, "controller")
        actor = await hosts.create("controller", {"state": value})
        actor_ids.append(actor._actor_id)
        await wait_until(lambda: received == list(range(value + 1)))
        stats = await actor_result(actor.stats.remote())
        assert stats["requests"][0] == {"state": -1}
        assert stats["max_active"] == 1
    assert len(set(actor_ids)) == 4
    assert client.is_running


@pytest.mark.asyncio
async def test_listener_added_while_lookup_is_in_flight(hosts):
    old = await hosts.create("controller", {"first": "old"})
    first, second = [], []
    resolve, entered, release, finished, calls = hosts.blocking_resolver("controller")
    client = hosts.client(old, {"first": first.append}, resolve)
    await wait_until(lambda: first == ["old"])
    await hosts.retire(old, "controller")
    await wait_until(entered.is_set)
    new = await hosts.create("controller", {"first": "new", "second": "added"})
    client.add_key_listeners({"second": second.append})
    await wait_until(lambda: "second" in client.key_listeners)
    release.set()
    await wait_until(lambda: first == ["old", "new"] and second == ["added"])
    assert finished.is_set()
    assert len(calls) == 1
    assert (await actor_result(new.stats.remote()))["requests"][0] == {
        "first": -1,
        "second": -1,
    }


@pytest.mark.asyncio
async def test_explicit_empty_snapshot_differs_from_unknown_key(hosts):
    old = await hosts.create("controller", {"routes": ["old"], "later": ["cached"]})
    routes, later = [], []
    hosts.client(
        old,
        {"routes": routes.append, "later": later.append},
        hosts.resolver("controller"),
    )
    await wait_until(lambda: routes == [["old"]] and later == [["cached"]])
    await hosts.retire(old, "controller")
    new = await hosts.create("controller", {"routes": []})
    await wait_until(lambda: routes == [["old"], []])
    # Several real host timeouts must not invent a deletion for the unknown key.
    await asyncio.sleep(0.3)
    assert later == [["cached"]]
    assert len((await actor_result(new.stats.remote()))["requests"]) >= 2
    await actor_result(new.publish.remote({"later": []}, 7))
    await wait_until(lambda: later == [["cached"], []])


@pytest.mark.asyncio
async def test_stop_cancels_pending_retry_before_lookup(hosts, monkeypatch):
    # A long delay makes this a cancellation test, not a race against actor
    # startup latency. Prompt task termination is the decisive oracle.
    monkeypatch.setattr(long_poll_module, "LONG_POLL_HOST_RETRY_DELAY_S", 60.0)
    old = await hosts.create("controller", {"state": "old"})
    received, calls = [], []

    def resolve():
        calls.append(True)
        return ray.get_actor("controller", namespace=hosts.namespace)

    client = hosts.client(old, {"state": received.append}, resolve)
    await wait_until(lambda: received == ["old"])
    await hosts.retire(old, "controller")
    await wait_until(lambda: client._reconnect_task is not None)
    retry_task = client._reconnect_task
    await asyncio.sleep(0)
    client.stop()
    await wait_until(retry_task.done)
    assert retry_task.cancelled() or retry_task.exception() is None
    new = await hosts.create("controller", {"state": "new"})
    await asyncio.sleep(0.1)
    assert calls == []
    assert not client.is_running
    assert received == ["old"]
    assert (await actor_result(new.stats.remote()))["requests"] == []


@pytest.mark.asyncio
async def test_stop_discards_blocked_executor_result_and_loop_keeps_running(hosts):
    old = await hosts.create("controller", {"state": "old"})
    received = []
    resolve, entered, release, finished, calls = hosts.blocking_resolver("controller")
    client = hosts.client(old, {"state": received.append}, resolve)
    await wait_until(lambda: received == ["old"])
    await hosts.retire(old, "controller")
    await wait_until(entered.is_set)

    heartbeat = []

    async def tick():
        for _ in range(5):
            await asyncio.sleep(0.01)
            heartbeat.append(True)

    await asyncio.wait_for(tick(), timeout=2)
    assert len(heartbeat) == 5
    assert not finished.is_set()
    assert len(calls) == 1
    client.stop()
    new = await hosts.create("controller", {"state": "new"})
    release.set()
    await wait_until(finished.is_set)
    await asyncio.sleep(0.2)
    assert not client.is_running
    assert client.host_actor._actor_id == old._actor_id
    assert received == ["old"]
    assert len(calls) == 1
    assert (await actor_result(new.stats.remote()))["requests"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [RuntimeError, ray.exceptions.RaySystemError])
async def test_unexpected_resolver_error_is_terminal(hosts, error_type):
    old = await hosts.create("controller", {"state": "old"})
    received, calls = [], []

    def resolve():
        calls.append(True)
        raise error_type("Resolver configuration is invalid")

    client = hosts.client(old, {"state": received.append}, resolve)
    await wait_until(lambda: received == ["old"])
    await hosts.retire(old, "controller")
    await wait_until(lambda: not client.is_running)
    await asyncio.sleep(0.2)
    assert calls == [True]
    assert received == ["old"]


@pytest.mark.asyncio
async def test_resolver_may_briefly_return_the_dead_handle(hosts):
    old = await hosts.create("controller", {"state": "old"})
    received, calls = [], []

    def resolve():
        calls.append(time.monotonic())
        if len(calls) == 1:
            return old
        return ray.get_actor("controller", namespace=hosts.namespace)

    hosts.client(old, {"state": received.append}, resolve)
    await wait_until(lambda: received == ["old"])
    await hosts.retire(old, "controller")
    await hosts.create("controller", {"state": "new"})
    await wait_until(lambda: received == ["old", "new"])
    assert len(calls) >= 2
    assert calls[1] - calls[0] >= 0.01


@pytest.mark.asyncio
async def test_stop_during_real_poll_discards_later_update(hosts):
    actor = await hosts.create("controller", {"state": "old"})
    received = []
    client = hosts.client(
        actor, {"state": received.append}, hosts.resolver("controller")
    )
    await wait_until(lambda: received == ["old"])
    client.stop()
    await actor_result(actor.publish.remote({"state": "after-stop"}))
    await asyncio.sleep(0.2)
    assert received == ["old"]
    assert not client.is_running


@pytest.mark.asyncio
async def test_listener_stop_discards_already_queued_callbacks(hosts, monkeypatch):
    actor = await hosts.create("controller", {"first": 1, "second": 2})
    received = []

    def first(value):
        received.append(value)
        client.stop()

    client = hosts.client(actor, {"first": first, "second": received.append})
    schedule_callback = client._schedule_to_event_loop
    queued_callbacks = []
    # Let the actual host RPC finish, but hold its callback deliveries until
    # both have arrived. This removes the race between the completion thread
    # scheduling the second callback and the event loop executing the first.
    monkeypatch.setattr(client, "_schedule_to_event_loop", queued_callbacks.append)
    await wait_until(lambda: len(queued_callbacks) == 2)
    assert received == []
    monkeypatch.setattr(client, "_schedule_to_event_loop", schedule_callback)
    for callback in queued_callbacks:
        schedule_callback(callback)
    # No yield occurred while enqueuing: both callbacks precede the first stop.
    assert len(queued_callbacks) == 2
    assert received == []
    await wait_until(lambda: not client.is_running)
    await asyncio.sleep(0.1)
    assert received == [1]


@pytest.mark.asyncio
async def test_named_lookup_timeouts_are_retried(hosts):
    old = await hosts.create("controller", {"state": "old"})
    received, calls = [], []
    timed_out = threading.Event()

    def resolve():
        calls.append(True)
        if len(calls) <= 2:
            # ActorManager maps a timed-out GCS lookup to GetTimeoutError.
            # Inject that known result; this is not a live GCS outage test.
            timed_out.set()
            raise ray.exceptions.GetTimeoutError("Named actor lookup timed out")
        return ray.get_actor("controller", namespace=hosts.namespace)

    client = hosts.client(old, {"state": received.append}, resolve)
    await wait_until(lambda: received == ["old"])
    await hosts.retire(old, "controller")
    await wait_until(timed_out.is_set)
    new = await hosts.create("controller", {"state": "new"})
    await wait_until(lambda: received == ["old", "new"] or not client.is_running)

    assert client.is_running
    assert received == ["old", "new"]
    assert len(calls) >= 3
    assert client.host_actor._actor_id == new._actor_id
    assert (await actor_result(new.stats.remote()))["requests"][0] == {"state": -1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
