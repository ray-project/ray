import copy
import json
import os
import random
import platform
import sys
import time

import numpy as np
import pytest
import psutil
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


@pytest.mark.skip("This test is for local benchmark.")
def test_sample_benchmark(object_spilling_config, shutdown_only):
    # --Config values--
    max_io_workers = 10
    object_store_limit = 500 * 1024 * 1024
    eight_mb = 1024 * 1024
    object_size = 12 * eight_mb
    spill_cnt = 50

    # Limit our object store to 200 MiB of memory.
    ray.init(
        object_store_memory=object_store_limit,
        _system_config={
            "object_store_full_max_retries": 0,
            "max_io_workers": max_io_workers,
            "object_spilling_config": object_spilling_config,
            "automatic_object_deletion_enabled": False,
        })
    arr = np.random.rand(object_size)
    replay_buffer = []
    pinned_objects = set()

    # Create objects of more than 200 MiB.
    spill_start = time.perf_counter()
    for _ in range(spill_cnt):
        ref = None
        while ref is None:
            try:
                ref = ray.put(arr)
                replay_buffer.append(ref)
                pinned_objects.add(ref)
            except ray.exceptions.ObjectStoreFullError:
                ref_to_spill = pinned_objects.pop()
                ray.experimental.force_spill_objects([ref_to_spill])
    spill_end = time.perf_counter()

    # Make sure to remove unpinned objects.
    del pinned_objects
    restore_start = time.perf_counter()
    while replay_buffer:
        ref = replay_buffer.pop()
        sample = ray.get(ref)  # noqa
    restore_end = time.perf_counter()

    print(f"Object spilling benchmark for the config {object_spilling_config}")
    print(f"Spilling {spill_cnt} number of objects of size {object_size}B "
          f"takes {spill_end - spill_start} seconds with {max_io_workers} "
          "number of io workers.")
    print(f"Getting all objects takes {restore_end - restore_start} seconds.")


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
def test_spill_objects_manually(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "object_store_full_max_retries": 0,
            "automatic_object_spilling_enabled": False,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
            "automatic_object_deletion_enabled": False,
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []
    pinned_objects = set()

    # Create objects of more than 200 MiB.
    for _ in range(25):
        ref = None
        while ref is None:
            try:
                ref = ray.put(arr)
                replay_buffer.append(ref)
                pinned_objects.add(ref)
            except ray.exceptions.ObjectStoreFullError:
                ref_to_spill = pinned_objects.pop()
                ray.experimental.force_spill_objects([ref_to_spill])

    def is_worker(cmdline):
        return cmdline and cmdline[0].startswith("ray::")

    # Make sure io workers are spawned with proper name.
    processes = [
        x.cmdline()[0] for x in psutil.process_iter(attrs=["cmdline"])
        if is_worker(x.info["cmdline"])
    ]
    assert (
        ray.ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE in processes)

    # Spill 2 more objects so we will always have enough space for
    # restoring objects back.
    refs_to_spill = (pinned_objects.pop(), pinned_objects.pop())
    ray.experimental.force_spill_objects(refs_to_spill)

    # randomly sample objects
    for _ in range(100):
        ref = random.choice(replay_buffer)
        sample = ray.get(ref)
        assert np.array_equal(sample, arr)

    # Make sure io workers are spawned with proper name.
    processes = [
        x.cmdline()[0] for x in psutil.process_iter(attrs=["cmdline"])
        if is_worker(x.info["cmdline"])
    ]
    assert (
        ray.ray_constants.WORKER_PROCESS_TYPE_RESTORE_WORKER_IDLE in processes)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_objects_manually_from_workers(object_spilling_config,
                                             shutdown_only):
    # Limit our object store to 100 MiB of memory.
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "object_store_full_max_retries": 0,
            "automatic_object_spilling_enabled": False,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
            "automatic_object_deletion_enabled": False,
        })

    @ray.remote
    def _worker():
        arr = np.random.rand(1024 * 1024)  # 8 MB data
        ref = ray.put(arr)
        ray.experimental.force_spill_objects([ref])
        return ref

    # Create objects of more than 200 MiB.
    replay_buffer = [ray.get(_worker.remote()) for _ in range(25)]
    values = {ref: np.copy(ray.get(ref)) for ref in replay_buffer}
    # Randomly sample objects.
    for _ in range(100):
        ref = random.choice(replay_buffer)
        sample = ray.get(ref)
        assert np.array_equal(sample, values[ref])


@pytest.mark.skip(reason="Not implemented yet.")
def test_spill_objects_manually_with_workers(object_spilling_config,
                                             shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "object_store_full_max_retries": 0,
            "automatic_object_spilling_enabled": False,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
            "automatic_object_deletion_enabled": False,
        })
    arrays = [np.random.rand(100 * 1024) for _ in range(50)]
    objects = [ray.put(arr) for arr in arrays]

    @ray.remote
    def _worker(object_refs):
        ray.experimental.force_spill_objects(object_refs)

    ray.get([_worker.remote([o]) for o in objects])

    for restored, arr in zip(ray.get(objects), arrays):
        assert np.array_equal(restored, arr)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 0,
        "object_store_memory": 75 * 1024 * 1024,
        "_system_config": {
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
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
            "object_store_full_initial_delay_ms": 100,
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
            "object_store_full_initial_delay_ms": 100,
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
            "object_store_full_initial_delay_ms": 100,
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
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
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
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
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
            "object_store_full_initial_delay_ms": 100,
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
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
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
    min_spilling_size = 30 * 1024 * 1024
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_max_retries": 4,
            "object_store_full_initial_delay_ms": 100,
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
