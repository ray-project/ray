import asyncio
import io
import logging
import os
import sys
import time
from typing import Dict
from unittest.mock import MagicMock

import pytest

import ray
import ray.serve._private.long_poll as long_poll_module
from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.common import (
    DeploymentID,
    DeploymentTargetInfo,
    EndpointInfo,
    ReplicaID,
    RunningReplicaInfo,
)
from ray.serve._private.long_poll import (
    LongPollClient,
    LongPollHost,
    LongPollNamespace,
    LongPollState,
    UpdatedObject,
    _get_metric_namespace_tag,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentTargetInfo as DeploymentTargetInfoProto,
    EndpointSet,
    LongPollRequest,
    LongPollResult,
)


def test_notifier_events_cleared_without_update(serve_instance):
    """Verify that notifier events are not leaked.

    Previously, events were leaked if there were timeouts and no updates on the key.
    """
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.1, 0.1)
    )
    ray.get(host.notify_changed.remote({"key_1": 999}))

    # Get an initial object snapshot for the key.
    object_ref = host.listen_for_change.remote({"key_1": -1})
    result: Dict[str, UpdatedObject] = ray.get(object_ref)
    assert set(result.keys()) == {"key_1"}
    assert {v.object_snapshot for v in result.values()} == {999}
    new_snapshot_ids = {k: v.snapshot_id for k, v in result.items()}

    # Listen for changes -- this should time out without an update.
    object_ref = host.listen_for_change.remote(new_snapshot_ids)
    assert ray.get(object_ref) == LongPollState.TIME_OUT

    # Verify that the `asyncio.Event` used for the `listen_for_change` task
    # is removed.
    assert ray.get(host._get_num_notifier_events.remote()) == 0


def test_host_standalone(serve_instance):
    host = ray.remote(LongPollHost).remote()

    # Write two values
    ray.get(host.notify_changed.remote({"key_1": 999}))
    ray.get(host.notify_changed.remote({"key_2": 999}))
    object_ref = host.listen_for_change.remote({"key_1": -1, "key_2": -1})

    # We should be able to get the result immediately
    result: Dict[str, UpdatedObject] = ray.get(object_ref)
    assert set(result.keys()) == {"key_1", "key_2"}
    assert {v.object_snapshot for v in result.values()} == {999}

    # Now try to pull it again, nothing should happen
    # because we have the updated snapshot_id
    new_snapshot_ids = {k: v.snapshot_id for k, v in result.items()}
    object_ref = host.listen_for_change.remote(new_snapshot_ids)
    _, not_done = ray.wait([object_ref], timeout=0.2)
    assert len(not_done) == 1

    # Now update the value, we should immediately get updated value
    ray.get(host.notify_changed.remote({"key_2": 999}))
    result = ray.get(object_ref)
    assert len(result) == 1
    assert "key_2" in result


def test_long_poll_wait_for_keys(serve_instance):
    # Variation of the basic case, but the keys are requests before any values
    # are set.
    host = ray.remote(LongPollHost).remote()
    object_ref = host.listen_for_change.remote({"key_1": -1, "key_2": -1})

    ray.get(host.notify_changed.remote({"key_1": 123, "key_2": 456}))

    # We should be able to get the both results immediately
    result: Dict[str, UpdatedObject] = ray.get(object_ref)
    assert result.keys() == {"key_1", "key_2"}
    assert {v.object_snapshot for v in result.values()} == {123, 456}


def test_long_poll_restarts(serve_instance):
    @ray.remote(
        max_restarts=-1,
        max_task_retries=-1,
    )
    class RestartableLongPollHost:
        def __init__(self) -> None:
            print("actor started")
            self.host = LongPollHost()
            self.host.notify_changed({"timer": time.time()})
            self.should_exit = False

        async def listen_for_change(self, key_to_ids):
            print("listening for change ", key_to_ids)
            return await self.host.listen_for_change(key_to_ids)

        async def set_exit(self):
            self.should_exit = True

        async def exit_if_set(self):
            if self.should_exit:
                print("actor exit")
                os._exit(1)

    host = RestartableLongPollHost.remote()
    updated_values = ray.get(host.listen_for_change.remote({"timer": -1}))
    timer: UpdatedObject = updated_values["timer"]

    on_going_ref = host.listen_for_change.remote({"timer": timer.snapshot_id})
    ray.get(host.set_exit.remote())
    # This task should trigger the actor to exit.
    # But the retried task will not because self.should_exit is false.
    host.exit_if_set.remote()

    # on_going_ref should return successfully with a differnt value.
    new_timer: UpdatedObject = ray.get(on_going_ref)["timer"]
    assert new_timer.snapshot_id != timer.snapshot_id + 1
    assert new_timer.object_snapshot != timer.object_snapshot


@pytest.mark.asyncio
async def test_client_callbacks(serve_instance):
    host = ray.remote(LongPollHost).remote()

    # Write two values
    ray.get(host.notify_changed.remote({"key_1": 100}))
    ray.get(host.notify_changed.remote({"key_2": 999}))

    callback_results = dict()

    def key_1_callback(result):
        callback_results["key_1"] = result

    def key_2_callback(result):
        callback_results["key_2"] = result

    _ = LongPollClient(
        host,
        {
            "key_1": key_1_callback,
            "key_2": key_2_callback,
        },
        call_in_event_loop=get_or_create_event_loop(),
    )

    await async_wait_for_condition(
        lambda: callback_results == {"key_1": 100, "key_2": 999},
        timeout=1,
    )

    ray.get(host.notify_changed.remote({"key_2": 1999}))

    await async_wait_for_condition(
        lambda: callback_results == {"key_1": 100, "key_2": 999},
        timeout=1,
    )


@pytest.mark.asyncio
async def test_client_threadsafe(serve_instance):
    host = ray.remote(LongPollHost).remote()
    ray.get(host.notify_changed.remote({"key_1": 100}))

    e = asyncio.Event()

    def key_1_callback(_):
        e.set()

    _ = LongPollClient(
        host,
        {
            "key_1": key_1_callback,
        },
        call_in_event_loop=get_or_create_event_loop(),
    )

    await e.wait()


def test_listen_for_change_java(serve_instance):
    host = ray.remote(LongPollHost).remote()
    ray.get(host.notify_changed.remote({"key_1": 999}))
    request_1 = {"keys_to_snapshot_ids": {"key_1": -1}}
    object_ref = host.listen_for_change_java.remote(
        LongPollRequest(**request_1).SerializeToString()
    )
    result_1: bytes = ray.get(object_ref)
    poll_result_1 = LongPollResult.FromString(result_1)
    assert set(poll_result_1.updated_objects.keys()) == {"key_1"}
    assert poll_result_1.updated_objects["key_1"].object_snapshot.decode() == "999"
    request_2 = {"keys_to_snapshot_ids": {"ROUTE_TABLE": -1}}
    endpoints: Dict[DeploymentID, EndpointInfo] = dict()
    endpoints["deployment_name"] = EndpointInfo(route="/test/xlang/poll")
    endpoints["deployment_name1"] = EndpointInfo(route="/test/xlang/poll1")
    ray.get(host.notify_changed.remote({LongPollNamespace.ROUTE_TABLE: endpoints}))
    object_ref_2 = host.listen_for_change_java.remote(
        LongPollRequest(**request_2).SerializeToString()
    )
    result_2: bytes = ray.get(object_ref_2)
    poll_result_2 = LongPollResult.FromString(result_2)
    assert set(poll_result_2.updated_objects.keys()) == {"ROUTE_TABLE"}
    endpoint_set = EndpointSet.FromString(
        poll_result_2.updated_objects["ROUTE_TABLE"].object_snapshot
    )
    assert set(endpoint_set.endpoints.keys()) == {"deployment_name", "deployment_name1"}
    assert endpoint_set.endpoints["deployment_name"].route == "/test/xlang/poll"

    request_3 = {"keys_to_snapshot_ids": {"(DEPLOYMENT_TARGETS,deployment_name)": -1}}
    replicas = [
        RunningReplicaInfo(
            replica_id=ReplicaID(
                str(i), deployment_id=DeploymentID(name="deployment_name")
            ),
            node_id="node_id",
            node_ip="node_ip",
            availability_zone="some-az",
            actor_name=f"SERVE_REPLICA::default#deployment_name#{i}",
            max_ongoing_requests=1,
        )
        for i in range(2)
    ]
    ray.get(
        host.notify_changed.remote(
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    "deployment_name",
                ): DeploymentTargetInfo(is_available=True, running_replicas=replicas)
            }
        )
    )
    object_ref_3 = host.listen_for_change_java.remote(
        LongPollRequest(**request_3).SerializeToString()
    )
    result_3: bytes = ray.get(object_ref_3)
    poll_result_3 = LongPollResult.FromString(result_3)
    da = DeploymentTargetInfoProto.FromString(
        poll_result_3.updated_objects[
            "(DEPLOYMENT_TARGETS,deployment_name)"
        ].object_snapshot
    )
    assert da.replica_names == [
        "SERVE_REPLICA::default#deployment_name#0",
        "SERVE_REPLICA::default#deployment_name#1",
    ]


def test_listen_for_change_java_timeout_returns_empty_result(serve_instance):
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.5, 0.5)
    )
    ray.get(host.notify_changed.remote({"key_1": 999}))

    initial_result = LongPollResult.FromString(
        ray.get(
            host.listen_for_change_java.remote(
                LongPollRequest(keys_to_snapshot_ids={"key_1": -1}).SerializeToString()
            )
        )
    )
    snapshot_id = initial_result.updated_objects["key_1"].snapshot_id

    timeout_result = LongPollResult.FromString(
        ray.get(
            host.listen_for_change_java.remote(
                LongPollRequest(
                    keys_to_snapshot_ids={"key_1": snapshot_id}
                ).SerializeToString()
            )
        )
    )

    assert len(timeout_result.updated_objects) == 0


def test_get_metric_namespace_tag(monkeypatch):
    """Test _get_metric_namespace_tag in both default and compact modes.

    When RAY_SERVE_COMPACT_LONG_POLL_METRIC_TAGS is disabled (default), metric
    tags use str(key) which preserves deployment-level granularity.
    When enabled, tags use only the LongPollNamespace enum name to bound
    cardinality for workloads with many deployments.
    See https://github.com/ray-project/ray/issues/62299.
    """
    # --- Default mode (compact=False): str(key) is used ---
    monkeypatch.setattr(
        long_poll_module, "RAY_SERVE_COMPACT_LONG_POLL_METRIC_TAGS", False
    )

    # String keys pass through unchanged
    assert _get_metric_namespace_tag("test_key") == "test_key"

    # LongPollNamespace enum keys use str() representation
    assert _get_metric_namespace_tag(LongPollNamespace.DEPLOYMENT_CONFIG) == str(
        LongPollNamespace.DEPLOYMENT_CONFIG
    )

    # Tuple keys include the full string (deployment name visible)
    tuple_key = (LongPollNamespace.DEPLOYMENT_CONFIG, "app1:deployment1")
    assert _get_metric_namespace_tag(tuple_key) == str(tuple_key)

    # --- Compact mode (compact=True): low-cardinality tags ---
    monkeypatch.setattr(
        long_poll_module, "RAY_SERVE_COMPACT_LONG_POLL_METRIC_TAGS", True
    )

    # String keys still pass through unchanged
    assert _get_metric_namespace_tag("test_key") == "test_key"
    assert _get_metric_namespace_tag("key_1") == "key_1"

    # LongPollNamespace enum keys return just the name
    assert (
        _get_metric_namespace_tag(LongPollNamespace.DEPLOYMENT_CONFIG)
        == "DEPLOYMENT_CONFIG"
    )
    assert (
        _get_metric_namespace_tag(LongPollNamespace.DEPLOYMENT_TARGETS)
        == "DEPLOYMENT_TARGETS"
    )
    assert _get_metric_namespace_tag(LongPollNamespace.ROUTE_TABLE) == "ROUTE_TABLE"

    # Tuple keys extract only the namespace, discarding per-deployment identifier
    assert (
        _get_metric_namespace_tag(
            (LongPollNamespace.DEPLOYMENT_CONFIG, "app1:deployment1")
        )
        == "DEPLOYMENT_CONFIG"
    )
    assert (
        _get_metric_namespace_tag(
            (LongPollNamespace.DEPLOYMENT_TARGETS, "app2:deployment2")
        )
        == "DEPLOYMENT_TARGETS"
    )

    # Tuple keys with long deployment identifiers (the real-world case)
    long_key = (
        LongPollNamespace.DEPLOYMENT_CONFIG,
        "Deployment(name='model', app='workday:perf-test-model-00005:"
        "key-value-v1:baked-in:global:global:1')",
    )
    assert _get_metric_namespace_tag(long_key) == "DEPLOYMENT_CONFIG"

    # All LongPollNamespace enum values should work, both bare and in tuples
    for ns in LongPollNamespace:
        assert _get_metric_namespace_tag(ns) == ns.name
        assert _get_metric_namespace_tag((ns, "any_deployment")) == ns.name

    # Defensive: tuple with non-enum first element falls back to str()
    assert _get_metric_namespace_tag(("custom_ns", "value")) == "custom_ns"


@pytest.mark.asyncio
async def test_pending_clients_gauge_aggregates_across_keys(serve_instance):
    """Test that pending_clients_by_namespace correctly aggregates across keys.

    When compact metric tags are enabled and multiple keys share a namespace
    tag (e.g., two DEPLOYMENT_CONFIG keys for different deployments), the gauge
    must reflect the total pending clients for the namespace, not just the last
    key's count.
    See https://github.com/ray-project/ray/issues/62299.
    """
    host = (
        ray.remote(LongPollHost)
        .options(
            runtime_env={"env_vars": {"RAY_SERVE_COMPACT_LONG_POLL_METRIC_TAGS": "1"}}
        )
        .remote(listen_for_change_request_timeout_s=(0.5, 0.5))
    )

    key_a = (LongPollNamespace.DEPLOYMENT_CONFIG, "app1:deploy1")
    key_b = (LongPollNamespace.DEPLOYMENT_CONFIG, "app2:deploy2")

    # Notify initial values so the keys exist
    ray.get(host.notify_changed.remote({key_a: "v1", key_b: "v2"}))

    # Subscribe to both keys — this should register 2 pending clients
    # for the DEPLOYMENT_CONFIG namespace
    ref = host.listen_for_change.remote({key_a: -1, key_b: -1})
    result = ray.get(ref)
    assert key_a in result and key_b in result

    # Now subscribe with up-to-date snapshot IDs so we actually block
    snapshot_ids = {k: v.snapshot_id for k, v in result.items()}
    ref = host.listen_for_change.remote(snapshot_ids)

    # The pending clients count for DEPLOYMENT_CONFIG should be 2
    # (one event per key). We can't read the gauge directly, but we
    # verify the internal tracking counter.
    counter = ray.get(
        host._get_pending_clients_by_namespace.remote("DEPLOYMENT_CONFIG")
    )
    assert counter == 2, f"Expected 2 pending clients, got {counter}"

    # Notify one key — should decrement by 1, not set to 0
    ray.get(host.notify_changed.remote({key_a: "v1_updated"}))

    # Wait for the listen_for_change to return
    result2 = ray.get(ref)
    assert key_a in result2

    # After notification, the counter should have been decremented
    counter_after = ray.get(
        host._get_pending_clients_by_namespace.remote("DEPLOYMENT_CONFIG")
    )
    # Counter should be 0 now (both events cleaned up: one by notify_changed,
    # one by the not_done cleanup in listen_for_change)
    assert counter_after == 0, f"Expected 0 pending clients, got {counter_after}"


@pytest.mark.asyncio
async def test_pending_clients_gauge_non_compact_mode(serve_instance):
    """Test pending_clients bookkeeping when RAY_SERVE_COMPACT_LONG_POLL_METRIC_TAGS=0.

    In non-compact mode, each key maps to its own namespace tag via str(key).
    Verify that pending client counts are correctly incremented, decremented,
    and cleaned up for each per-key tag, with no regressions in the new
    bookkeeping logic.
    See https://github.com/ray-project/ray/issues/62299.
    """
    host = (
        ray.remote(LongPollHost)
        .options(
            runtime_env={"env_vars": {"RAY_SERVE_COMPACT_LONG_POLL_METRIC_TAGS": "0"}}
        )
        .remote(listen_for_change_request_timeout_s=(0.5, 0.5))
    )

    key_a = (LongPollNamespace.DEPLOYMENT_CONFIG, "app1:deploy1")
    key_b = (LongPollNamespace.DEPLOYMENT_CONFIG, "app2:deploy2")

    # Notify initial values so the keys exist
    ray.get(host.notify_changed.remote({key_a: "v1", key_b: "v2"}))

    # Get initial snapshots
    ref = host.listen_for_change.remote({key_a: -1, key_b: -1})
    result = ray.get(ref)
    assert key_a in result and key_b in result

    # Subscribe with up-to-date snapshot IDs so we block (pending)
    snapshot_ids = {k: v.snapshot_id for k, v in result.items()}
    ref = host.listen_for_change.remote(snapshot_ids)

    # In non-compact mode, each key has its own namespace tag (str(key)),
    # so each should have exactly 1 pending client.
    tag_a = str(key_a)
    tag_b = str(key_b)
    counter_a = ray.get(host._get_pending_clients_by_namespace.remote(tag_a))
    counter_b = ray.get(host._get_pending_clients_by_namespace.remote(tag_b))
    assert counter_a == 1, f"Expected 1 pending client for {tag_a}, got {counter_a}"
    assert counter_b == 1, f"Expected 1 pending client for {tag_b}, got {counter_b}"

    # Notify key_a — its pending count should drop, key_b unaffected
    ray.get(host.notify_changed.remote({key_a: "v1_updated"}))
    result2 = ray.get(ref)
    assert key_a in result2

    # After notification + cleanup, both counters should be 0
    counter_a_after = ray.get(host._get_pending_clients_by_namespace.remote(tag_a))
    counter_b_after = ray.get(host._get_pending_clients_by_namespace.remote(tag_b))
    assert (
        counter_a_after == 0
    ), f"Expected 0 pending clients for {tag_a}, got {counter_a_after}"
    assert (
        counter_b_after == 0
    ), f"Expected 0 pending clients for {tag_b}, got {counter_b_after}"

    # Verify cleanup with timeout: subscribe and let it time out
    snapshot_ids2 = {k: v.snapshot_id for k, v in result2.items()}
    # Also include key_b's latest snapshot
    snapshot_ids2[key_b] = result[key_b].snapshot_id
    ref2 = host.listen_for_change.remote(snapshot_ids2)
    timeout_result = ray.get(ref2)
    assert timeout_result == LongPollState.TIME_OUT

    # After timeout, all pending counters should be cleaned up to 0
    counter_a_timeout = ray.get(host._get_pending_clients_by_namespace.remote(tag_a))
    counter_b_timeout = ray.get(host._get_pending_clients_by_namespace.remote(tag_b))
    assert (
        counter_a_timeout == 0
    ), f"Expected 0 after timeout for {tag_a}, got {counter_a_timeout}"
    assert (
        counter_b_timeout == 0
    ), f"Expected 0 after timeout for {tag_b}, got {counter_b_timeout}"


def test_remove_keys_evicts_per_key_state(serve_instance):
    """After remove_keys, a poll with a stale snapshot id must time out
    instead of returning the stale snapshot."""
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.2, 0.2)
    )
    key = (LongPollNamespace.DEPLOYMENT_CONFIG, "app1:deploy1")

    ray.get(host.notify_changed.remote({key: "config-v1"}))

    initial = ray.get(host.listen_for_change.remote({key: -1}))
    assert key in initial
    stale_snapshot_ids = {key: initial[key].snapshot_id}

    ray.get(host.remove_keys.remote([key]))

    poll_result = ray.get(host.listen_for_change.remote(stale_snapshot_ids))
    assert poll_result == LongPollState.TIME_OUT


def test_remove_keys_wakes_parked_waiters(serve_instance):
    """Parked RPCs return promptly after remove_keys (not at the 30 s
    timeout); the evicted key is filtered out of the returned dict."""
    host = ray.remote(LongPollHost).remote(listen_for_change_request_timeout_s=(30, 30))
    key = (LongPollNamespace.DEPLOYMENT_CONFIG, "app:deploy")

    ray.get(host.notify_changed.remote({key: "v1"}))
    initial = ray.get(host.listen_for_change.remote({key: -1}))
    snapshot_id = initial[key].snapshot_id

    parked_ref = host.listen_for_change.remote({key: snapshot_id})
    wait_for_condition(
        lambda: ray.get(host._get_num_notifier_events.remote()) == 1,
        timeout=5,
    )

    ray.get(host.remove_keys.remote([key]))

    start = time.time()
    result = ray.get(parked_ref, timeout=5)
    elapsed = time.time() - start
    assert elapsed < 5, f"parked RPC took {elapsed:.2f}s; should have woken"
    assert result == {}, f"expected empty dict, got {result}"
    assert ray.get(host._get_num_notifier_events.remote()) == 0


def test_remove_keys_decrements_pending_clients_gauge(serve_instance):
    """Gauge must return to 0 after remove_keys wakes parked waiters.
    Otherwise the listen_for_change timeout cleanup skips the decrement
    on the now-missing notifier_events entry and inflates the gauge."""
    host = (
        ray.remote(LongPollHost)
        .options(
            runtime_env={"env_vars": {"RAY_SERVE_COMPACT_LONG_POLL_METRIC_TAGS": "1"}}
        )
        .remote(listen_for_change_request_timeout_s=(30, 30))
    )
    key = (LongPollNamespace.DEPLOYMENT_CONFIG, "app:deploy")

    ray.get(host.notify_changed.remote({key: "v1"}))
    initial = ray.get(host.listen_for_change.remote({key: -1}))
    snapshot_id = initial[key].snapshot_id

    parked_ref = host.listen_for_change.remote({key: snapshot_id})

    wait_for_condition(
        lambda: ray.get(
            host._get_pending_clients_by_namespace.remote("DEPLOYMENT_CONFIG")
        )
        == 1,
        timeout=5,
    )

    ray.get(host.remove_keys.remote([key]))
    ray.get(parked_ref, timeout=5)

    assert (
        ray.get(host._get_pending_clients_by_namespace.remote("DEPLOYMENT_CONFIG")) == 0
    )


def test_delete_tombstone_reaches_parked_subscriber(serve_instance):
    """Parked subscriber must receive the DEPLOYMENT_TARGETS tombstone.

    Evicting the tombstoned key in the same sync tick would race the
    waiter: it wakes after update() returns, sees the key gone, and
    drops the tombstone.
    """
    host = ray.remote(LongPollHost).remote(listen_for_change_request_timeout_s=(30, 30))

    targets_key = (LongPollNamespace.DEPLOYMENT_TARGETS, "d1")
    config_key = (LongPollNamespace.DEPLOYMENT_CONFIG, "d1")

    target_info_initial = DeploymentTargetInfo(is_available=True, running_replicas=[])
    ray.get(
        host.notify_changed.remote(
            {targets_key: target_info_initial, config_key: "config-v1"}
        )
    )

    initial = ray.get(host.listen_for_change.remote({targets_key: -1, config_key: -1}))
    targets_snapshot_id = initial[targets_key].snapshot_id

    parked_ref = host.listen_for_change.remote({targets_key: targets_snapshot_id})
    wait_for_condition(
        lambda: ray.get(host._get_num_notifier_events.remote()) == 1,
        timeout=5,
    )

    # Simulate the delete path: tombstone TARGETS, evict only CONFIG.
    tombstone = DeploymentTargetInfo(is_available=False, running_replicas=[])
    ray.get(host.notify_changed.remote({targets_key: tombstone}))
    ray.get(host.remove_keys.remote([config_key]))

    result = ray.get(parked_ref, timeout=5)
    assert targets_key in result, f"Subscriber missed the tombstone: {result}"
    delivered = result[targets_key].object_snapshot
    assert delivered.is_available is False
    assert delivered.running_replicas == []


def test_long_poll_client_disable_propagates_to_host_log():
    host = LongPollHost()
    host_actor = MagicMock()
    host_actor.notify_long_poll_client_disabled.remote.side_effect = (
        host.notify_client_disabled
    )

    serve_logger = logging.getLogger("ray.serve")
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    serve_logger.addHandler(handler)
    loop = asyncio.new_event_loop()  # never run; is_running() == False
    try:
        client = LongPollClient(
            host_actor, {}, call_in_event_loop=loop, client_id="TestClient"
        )
        client._schedule_to_event_loop(lambda: None)
    finally:
        loop.close()
        serve_logger.removeHandler(handler)

    assert client.is_running is False
    output = buf.getvalue()
    assert "TestClient" in output, output
    assert "not running" in output.lower(), output


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
