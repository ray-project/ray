import sys
import functools
import time
import asyncio
from typing import Dict

import pytest

import ray
from ray.serve.long_poll import (LongPollerAsyncClient, LongPollerHost,
                                 UpdatedObject)


def test_host_standalone(serve_instance):
    host = ray.remote(LongPollerHost).remote()

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


@pytest.mark.skip(
    "Skip until https://github.com/ray-project/ray/issues/11683 fixed "
    "since async actor retries is broken.")
def test_long_pull_restarts(serve_instance):
    @ray.remote(
        max_restarts=-1,
        # max_task_retries=-1,
    )
    class RestartableLongPollerHost:
        def __init__(self) -> None:
            print("actor started")
            self.host = LongPollerHost()
            self.host.notify_changed("timer", time.time())

        async def listen_for_change(self, key_to_ids):
            await asyncio.sleep(0.5)
            return await self.host.listen_for_change(key_to_ids)

        async def exit(self):
            sys.exit(1)

    host = RestartableLongPollerHost.remote()
    updated_values = ray.get(host.listen_for_change.remote({"timer": -1}))
    timer: UpdatedObject = updated_values["timer"]

    on_going_ref = host.listen_for_change.remote({"timer": timer.snapshot_id})
    host.exit.remote()
    on_going_ref = host.listen_for_change.remote({"timer": timer.snapshot_id})
    new_timer: UpdatedObject = ray.get(on_going_ref)["timer"]
    assert new_timer.snapshot_id != timer.snapshot_id + 1
    assert new_timer.object_snapshot != timer.object_snapshot


@pytest.mark.asyncio
async def test_async_client(serve_instance):
    host = ray.remote(LongPollerHost).remote()

    # Write two values
    ray.get(host.notify_changed.remote("key_1", 100))
    ray.get(host.notify_changed.remote("key_2", 999))

    callback_results = dict()

    async def callback(result, key):
        callback_results[key] = result

    client = LongPollerAsyncClient(
        host, {
            "key_1": functools.partial(callback, key="key_1"),
            "key_2": functools.partial(callback, key="key_2")
        })

    while len(client.object_snapshots) == 0:
        # Yield the loop for client to get the result
        await asyncio.sleep(0.2)

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
