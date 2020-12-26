import copy
import json
import os
import random
import platform
import sys

import numpy as np
import pytest
import ray
from ray.external_storage import (create_url_with_offset,
                                  parse_url_with_offset)
from ray.test_utils import wait_for_condition

bucket_name = "object-spilling-test"
spill_local_path = "/tmp/spill"
file_system_object_spilling_config = {
    "type": "filesystem",
    "params": {
        "directory_path": spill_local_path
    }
}
smart_open_object_spilling_config = {
    "type": "smart_open",
    "params": {
        "uri": f"s3://{bucket_name}/"
    }
}


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        # TODO(sang): Add a mock dependency to test S3.
        # smart_open_object_spilling_config,
    ])
def object_spilling_config(request, tmpdir):
    if request.param["type"] == "filesystem":
        request.param["params"]["directory_path"] = str(tmpdir)
    yield json.dumps(request.param)


def test_invalid_config_raises_exception(shutdown_only):
    # Make sure ray.init raises an exception before
    # it starts processes when invalid object spilling
    # config is given.
    with pytest.raises(ValueError):
        ray.init(_system_config={
            "object_spilling_config": json.dumps({
                "type": "abc"
            }),
        })

    with pytest.raises(Exception):
        copied_config = copy.deepcopy(file_system_object_spilling_config)
        # Add invalid params to the config.
        copied_config["params"].update({"random_arg": "abc"})
        ray.init(_system_config={
            "object_spilling_config": json.dumps(copied_config),
        })


def test_url_generation_and_parse():
    url = "s3://abc/def/ray_good"
    offset = 10
    size = 30
    url_with_offset = create_url_with_offset(url=url, offset=offset, size=size)
    parsed_result = parse_url_with_offset(url_with_offset)
    assert parsed_result.base_url == url
    assert parsed_result.offset == offset
    assert parsed_result.size == size


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spilling_not_done_for_pinned_object(tmp_path, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": str(temp_folder)
                }
            }),
            "min_spilling_size": 0,
        })
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    ref = ray.get(ray.put(arr))  # noqa
    # Since the ref exists, it should raise OOM.
    with pytest.raises(ray.exceptions.ObjectStoreFullError):
        ref2 = ray.put(arr)  # noqa

    def is_dir_empty():
        num_files = 0
        for path in temp_folder.iterdir():
            num_files += 1
        return num_files == 0

    wait_for_condition(is_dir_empty)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 0,
        "object_store_memory": 75 * 1024 * 1024,
        "_system_config": {
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "max_io_workers": 4,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": "/tmp"
                }
            }),
            "min_spilling_size": 0,
        },
    }],
    indirect=True)
def test_spill_remote_object(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(object_store_memory=75 * 1024 * 1024)

    @ray.remote
    def put():
        return np.random.rand(5 * 1024 * 1024)  # 40 MB data

    @ray.remote
    def depends(arg):
        return

    ref = put.remote()
    copy = np.copy(ray.get(ref))
    # Evict local copy.
    ray.put(np.random.rand(5 * 1024 * 1024))  # 40 MB data
    # Remote copy should cause first remote object to get spilled.
    ray.get(put.remote())

    sample = ray.get(ref)
    assert np.array_equal(sample, copy)
    # Evict the spilled object.
    del sample
    ray.get(put.remote())
    ray.put(np.random.rand(5 * 1024 * 1024))  # 40 MB data

    # Test passing the spilled object as an arg to another task.
    ray.get(depends.remote(ref))


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_objects_automatically(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0
        })
    replay_buffer = []
    solution_buffer = []
    buffer_length = 100

    # Create objects of more than 800 MiB.
    for _ in range(buffer_length):
        ref = None
        while ref is None:
            multiplier = random.choice([1, 2, 3])
            arr = np.random.rand(multiplier * 1024 * 1024)
            ref = ray.put(arr)
            replay_buffer.append(ref)
            solution_buffer.append(arr)

    print("-----------------------------------")
    # randomly sample objects
    for _ in range(1000):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_during_get(object_spilling_config, shutdown_only):
    ray.init(
        num_cpus=4,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            # NOTE(swang): Use infinite retries because the OOM timer can still
            # get accidentally triggered when objects are released too slowly
            # (see github.com/ray-project/ray/issues/12040).
            "object_store_full_max_retries": -1,
            "max_io_workers": 1,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )

    @ray.remote
    def f():
        return np.zeros(10 * 1024 * 1024)

    ids = []
    for i in range(10):
        x = f.remote()
        print(i, x)
        ids.append(x)

    # Concurrent gets, which require restoring from external storage, while
    # objects are being created.
    for x in ids:
        print(ray.get(x).shape)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_deadlock(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

    # Create objects of more than 400 MiB.
    for _ in range(50):
        ref = None
        while ref is None:
            ref = ray.put(arr)
            replay_buffer.append(ref)
        # This is doing random sampling with 50% prob.
        if random.randint(0, 9) < 5:
            for _ in range(5):
                ref = random.choice(replay_buffer)
                sample = ray.get(ref, timeout=0)
                assert np.array_equal(sample, arr)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_delete_objects(tmp_path, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "min_spilling_size": 0,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": str(temp_folder)
                }
            }),
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

    for _ in range(80):
        ref = None
        while ref is None:
            ref = ray.put(arr)
            replay_buffer.append(ref)

    print("-----------------------------------")

    def is_dir_empty():
        num_files = 0
        for path in temp_folder.iterdir():
            num_files += 1
        return num_files == 0

    del replay_buffer
    del ref
    wait_for_condition(is_dir_empty)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_delete_objects_delete_while_creating(tmp_path, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "min_spilling_size": 0,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": str(temp_folder)
                }
            }),
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

    for _ in range(80):
        ref = None
        while ref is None:
            ref = ray.put(arr)
            replay_buffer.append(ref)
        # Remove the replay buffer with 60% probability.
        if random.randint(0, 9) < 6:
            replay_buffer.pop()

    # Do random sampling.
    for _ in range(200):
        ref = random.choice(replay_buffer)
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, arr)

    def is_dir_empty():
        num_files = 0
        for path in temp_folder.iterdir():
            num_files += 1
        return num_files == 0

    # After all, make sure all objects are killed without race condition.
    del replay_buffer
    del ref
    wait_for_condition(is_dir_empty, timeout=1000)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_delete_objects_on_worker_failure(tmp_path, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": str(temp_folder)
                }
            }),
            "min_spilling_size": 0,
        })

    arr = np.random.rand(1024 * 1024)  # 8 MB data

    @ray.remote
    class Actor:
        def __init__(self):
            self.replay_buffer = []

        def get_pid(self):
            return os.getpid()

        def create_objects(self):
            for _ in range(80):
                ref = None
                while ref is None:
                    ref = ray.put(arr)
                    self.replay_buffer.append(ref)
                # Remove the replay buffer with 60% probability.
                if random.randint(0, 9) < 6:
                    self.replay_buffer.pop()

            # Do random sampling.
            for _ in range(200):
                ref = random.choice(self.replay_buffer)
                sample = ray.get(ref, timeout=0)
                assert np.array_equal(sample, arr)

    a = Actor.remote()
    actor_pid = ray.get(a.get_pid.remote())
    ray.get(a.create_objects.remote())
    os.kill(actor_pid, 9)

    def wait_until_actor_dead():
        try:
            ray.get(a.get_pid.remote())
        except ray.exceptions.RayActorError:
            return True
        return False

    wait_for_condition(wait_until_actor_dead)

    def is_dir_empty():
        num_files = 0
        for path in temp_folder.iterdir():
            num_files += 1
        return num_files == 0

    # After all, make sure all objects are deleted upon worker failures.
    wait_for_condition(is_dir_empty, timeout=1000)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_delete_objects_multi_node(tmp_path, ray_start_cluster):
    # Limit our object store to 75 MiB of memory.
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    cluster = ray_start_cluster
    # Head node.
    cluster.add_node(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 2,
            "min_spilling_size": 20 * 1024 * 1024,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": str(temp_folder)
                }
            }),
        })
    # Add 2 worker nodes.
    for _ in range(2):
        cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    ray.init(address=cluster.address)

    arr = np.random.rand(1024 * 1024)  # 8 MB data

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            self.replay_buffer = []

        def ping(self):
            return

        def create_objects(self):
            for _ in range(80):
                ref = None
                while ref is None:
                    ref = ray.put(arr)
                    self.replay_buffer.append(ref)
                # Remove the replay buffer with 60% probability.
                if random.randint(0, 9) < 6:
                    self.replay_buffer.pop()

            # Do random sampling.
            for _ in range(200):
                ref = random.choice(self.replay_buffer)
                sample = ray.get(ref, timeout=0)
                assert np.array_equal(sample, arr)

    actors = [Actor.remote() for _ in range(3)]
    ray.get([actor.create_objects.remote() for actor in actors])

    def wait_until_actor_dead(actor):
        try:
            ray.get(actor.ping.remote())
        except ray.exceptions.RayActorError:
            return True
        return False

    def is_dir_empty():
        num_files = 0
        for path in temp_folder.iterdir():
            num_files += 1
        return num_files == 0

    # Kill actors to remove all references.
    for actor in actors:
        ray.kill(actor)
        wait_for_condition(lambda: wait_until_actor_dead(actor))
    # The multi node deletion should work.
    wait_for_condition(is_dir_empty)


def test_fusion_objects(tmp_path, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    min_spilling_size = 10 * 1024 * 1024
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 3,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": str(temp_folder)
                }
            }),
            "min_spilling_size": min_spilling_size,
        })
    replay_buffer = []
    solution_buffer = []
    buffer_length = 100

    # Create objects of more than 800 MiB.
    for _ in range(buffer_length):
        ref = None
        while ref is None:
            multiplier = random.choice([1, 2, 3])
            arr = np.random.rand(multiplier * 1024 * 1024)
            ref = ray.put(arr)
            replay_buffer.append(ref)
            solution_buffer.append(arr)

    print("-----------------------------------")
    # randomly sample objects
    for _ in range(1000):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)

    is_test_passing = False
    for path in temp_folder.iterdir():
        file_size = path.stat().st_size
        # Make sure there are at least one
        # file_size that exceeds the min_spilling_size.
        # If we don't fusion correctly, this cannot happen.
        if file_size >= min_spilling_size:
            is_test_passing = True
    assert is_test_passing


# https://github.com/ray-project/ray/issues/12912
def do_test_release_resource(tmp_path, expect_released):
    temp_folder = tmp_path / "spill"
    ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "release_resources_during_plasma_fetch": expect_released,
            "automatic_object_spilling_enabled": True,
            "object_spilling_config": json.dumps({
                "type": "filesystem",
                "params": {
                    "directory_path": str(temp_folder)
                }
            }),
        })
    plasma_obj = ray.put(np.ones(50 * 1024 * 1024, dtype=np.uint8))
    for _ in range(5):
        ray.put(np.ones(50 * 1024 * 1024, dtype=np.uint8))  # Force spilling

    @ray.remote
    def sneaky_task_tries_to_steal_released_resources():
        print("resources were released!")

    @ray.remote
    def f(dep):
        while True:
            try:
                ray.get(dep[0], timeout=0.001)
            except ray.exceptions.GetTimeoutError:
                pass

    done = f.remote([plasma_obj])  # noqa
    canary = sneaky_task_tries_to_steal_released_resources.remote()
    ready, _ = ray.wait([canary], timeout=2)
    if expect_released:
        assert ready
    else:
        assert not ready


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_no_release_during_plasma_fetch(tmp_path, shutdown_only):
    do_test_release_resource(tmp_path, expect_released=False)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_release_during_plasma_fetch(tmp_path, shutdown_only):
    do_test_release_resource(tmp_path, expect_released=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
