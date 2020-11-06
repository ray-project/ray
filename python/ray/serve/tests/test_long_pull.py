import sys
import time
import asyncio
from typing import Dict

import pytest

import ray
from ray.serve.long_pull import (LongPullerAsyncClient, LongPullerHost,
                                 UpdatedObject, LongPullerSyncClient)
from ray.tests.conftest import ray_start_regular_shared


def test_host_standalone(ray_start_regular_shared):
    host = ray.remote(LongPullerHost).remote()

    # Write two values
    ray.get(host.notify_on_changed.remote("key_1", 999))
    ray.get(host.notify_on_changed.remote("key_2", 999))
    object_ref = host.listen_on_changed.remote({"key_1": -1, "key_2": -1})

    # We should be able to get the result immediately
    result: Dict[str, UpdatedObject] = ray.get(object_ref)
    assert set(result.keys()) == {"key_1", "key_2"}
    assert set(v.object_snapshot for v in result.values()) == {999}

    # Now try to pull it again, nothing should happen
    # because we have the updated snapshot_id
    new_snapshot_ids = {k: v.snapshot_id for k, v in result.items()}
    object_ref = host.listen_on_changed.remote(new_snapshot_ids)
    _, not_done = ray.wait([object_ref], timeout=0.2)
    assert len(not_done) == 1

    # Now update the value, we should immediately get updated value
    ray.get(host.notify_on_changed.remote("key_2", 999))
    result = ray.get(object_ref)
    assert len(result) == 1
    assert "key_2" in result


def test_sync_client(ray_start_regular_shared):
    host = ray.remote(LongPullerHost).remote()

    # Write two values
    ray.get(host.notify_on_changed.remote("key_1", 100))
    ray.get(host.notify_on_changed.remote("key_2", 999))

    client = LongPullerSyncClient(host, ["key_1", "key_2"])
    assert client.get_object_snapshot("key_1") == 100
    assert client.get_object_snapshot("key_2") == 999

    ray.get(host.notify_on_changed.remote("key_2", 1999))

    values = set()
    for _ in range(3):
        values.add(client.get_object_snapshot("key_2"))
        if 1999 in values:
            break
        time.sleep(1)
    assert 1999 in values


@pytest.mark.asyncio
async def test_async_client(ray_start_regular_shared):
    host = ray.remote(LongPullerHost).remote()

    # Write two values
    ray.get(host.notify_on_changed.remote("key_1", 100))
    ray.get(host.notify_on_changed.remote("key_2", 999))

    client = LongPullerAsyncClient(host, ["key_1", "key_2"])
    assert client.get_object_snapshot("key_1") == 100
    assert client.get_object_snapshot("key_2") == 999

    ray.get(host.notify_on_changed.remote("key_2", 1999))

    values = set()
    for _ in range(3):
        values.add(client.get_object_snapshot("key_2"))
        if 1999 in values:
            break
        await asyncio.sleep(1)
    assert 1999 in values


def test_watched_object(ray_start_regular_shared):
    host = ray.remote(LongPullerHost).remote()

    # Write two values
    ray.get(host.notify_on_changed.remote("key_1", 100))
    ray.get(host.notify_on_changed.remote("key_2", 999))

    client = LongPullerSyncClient(host, ["key_1", "key_2"])
    key_1 = client.watch_object("key_1")
    key_2 = client.watch_object("key_2")
    assert key_1.get() == 100
    assert key_2.get() == 999

    ray.get(host.notify_on_changed.remote("key_2", 1999))

    values = set()
    for _ in range(3):
        values.add(key_2.get())
        if 1999 in values:
            break
        time.sleep(1)
    assert 1999 in values


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
