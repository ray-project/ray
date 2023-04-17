import sys
import asyncio
import time
import os
from typing import Dict

import pytest

import ray
from ray._private.utils import get_or_create_event_loop
from ray.serve._private.common import EndpointTag, EndpointInfo, RunningReplicaInfo
from ray.serve._private.long_poll import (
    LongPollClient,
    LongPollHost,
    UpdatedObject,
    LongPollNamespace,
)
from ray.serve.generated.serve_pb2 import (
    LongPollRequest,
    LongPollResult,
    EndpointSet,
    ActorNameList,
)


def test_host_standalone(serve_instance):
    host = ray.remote(LongPollHost).remote()

    # Write two values
    ray.get(host.notify_changed.remote("key_1", 999))
    ray.get(host.notify_changed.remote("key_2", 999))
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
    ray.get(host.notify_changed.remote("key_2", 999))
    result = ray.get(object_ref)
    assert len(result) == 1
    assert "key_2" in result


def test_long_poll_wait_for_keys(serve_instance):
    # Variation of the basic case, but the keys are requests before any values
    # are set.
    host = ray.remote(LongPollHost).remote()
    object_ref = host.listen_for_change.remote({"key_1": -1, "key_2": -1})
    ray.get(host.notify_changed.remote("key_1", 999))
    ray.get(host.notify_changed.remote("key_2", 999))

    # We should be able to get the one of the result immediately
    result: Dict[str, UpdatedObject] = ray.get(object_ref)
    assert set(result.keys()).issubset({"key_1", "key_2"})
    assert {v.object_snapshot for v in result.values()} == {999}


def test_long_poll_restarts(serve_instance):
    @ray.remote(
        max_restarts=-1,
        max_task_retries=-1,
    )
    class RestartableLongPollHost:
        def __init__(self) -> None:
            print("actor started")
            self.host = LongPollHost()
            self.host.notify_changed("timer", time.time())
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

    # on_going_ref should return succesfully with a differnt value.
    new_timer: UpdatedObject = ray.get(on_going_ref)["timer"]
    assert new_timer.snapshot_id != timer.snapshot_id + 1
    assert new_timer.object_snapshot != timer.object_snapshot


@pytest.mark.asyncio
async def test_client(serve_instance):
    host = ray.remote(LongPollHost).remote()

    # Write two values
    ray.get(host.notify_changed.remote("key_1", 100))
    ray.get(host.notify_changed.remote("key_2", 999))

    callback_results = dict()

    def key_1_callback(result):
        callback_results["key_1"] = result

    def key_2_callback(result):
        callback_results["key_2"] = result

    client = LongPollClient(
        host,
        {
            "key_1": key_1_callback,
            "key_2": key_2_callback,
        },
        call_in_event_loop=get_or_create_event_loop(),
    )

    while len(client.object_snapshots) == 0:
        time.sleep(0.1)

    assert client.object_snapshots["key_1"] == 100
    assert client.object_snapshots["key_2"] == 999

    ray.get(host.notify_changed.remote("key_2", 1999))

    values = set()
    for _ in range(3):
        values.add(client.object_snapshots["key_2"])
        if 1999 in values:
            break
        await asyncio.sleep(1)
    assert 1999 in values

    assert callback_results == {"key_1": 100, "key_2": 1999}


@pytest.mark.asyncio
async def test_client_threadsafe(serve_instance):
    host = ray.remote(LongPollHost).remote()
    ray.get(host.notify_changed.remote("key_1", 100))

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
    ray.get(host.notify_changed.remote("key_1", 999))
    request_1 = {"keys_to_snapshot_ids": {"key_1": -1}}
    object_ref = host.listen_for_change_java.remote(
        LongPollRequest(**request_1).SerializeToString()
    )
    result_1: bytes = ray.get(object_ref)
    poll_result_1 = LongPollResult.FromString(result_1)
    assert set(poll_result_1.updated_objects.keys()) == {"key_1"}
    assert poll_result_1.updated_objects["key_1"].object_snapshot.decode() == "999"
    request_2 = {"keys_to_snapshot_ids": {"ROUTE_TABLE": -1}}
    endpoints: Dict[EndpointTag, EndpointInfo] = dict()
    endpoints["deployment_name"] = EndpointInfo(route="/test/xlang/poll")
    endpoints["deployment_name1"] = EndpointInfo(route="/test/xlang/poll1")
    ray.get(host.notify_changed.remote(LongPollNamespace.ROUTE_TABLE, endpoints))
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

    request_3 = {"keys_to_snapshot_ids": {"(RUNNING_REPLICAS,deployment_name)": -1}}
    replicas = [
        RunningReplicaInfo(
            deployment_name="deployment_name",
            replica_tag=str(i),
            actor_handle=host,
            max_concurrent_queries=1,
        )
        for i in range(2)
    ]
    ray.get(
        host.notify_changed.remote(
            (LongPollNamespace.RUNNING_REPLICAS, "deployment_name"), replicas
        )
    )
    object_ref_3 = host.listen_for_change_java.remote(
        LongPollRequest(**request_3).SerializeToString()
    )
    result_3: bytes = ray.get(object_ref_3)
    poll_result_3 = LongPollResult.FromString(result_3)
    replica_name_list = ActorNameList.FromString(
        poll_result_3.updated_objects[
            "(RUNNING_REPLICAS,deployment_name)"
        ].object_snapshot
    )
    assert replica_name_list.names == ["SERVE_REPLICA::0", "SERVE_REPLICA::1"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
