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
from ray.internal.internal_api import memory_summary

bucket_name = "object-spilling-test"
spill_local_path = "/tmp/spill"
file_system_object_spilling_config = {
    "type": "filesystem",
    "params": {
        "directory_path": spill_local_path
    }
}
# Since we have differet protocol for a local external storage (e.g., fs)
# and distributed external storage (e.g., S3), we need to test both cases.
# This mocks the distributed fs with cluster utils.
mock_distributed_fs_object_spilling_config = {
    "type": "mock_distributed_fs",
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


def create_object_spilling_config(request, tmp_path):
    if (request.param["type"] == "filesystem"
            or request.param["type"] == "mock_distributed_fs"):
        temp_folder = tmp_path / "spill"
        temp_folder.mkdir()
        request.param["params"]["directory_path"] = str(temp_folder)
    return json.dumps(request.param), temp_folder


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        # TODO(sang): Add a mock dependency to test S3.
        # smart_open_object_spilling_config,
    ])
def object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        mock_distributed_fs_object_spilling_config
    ])
def multi_node_object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


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
def test_spilling_not_done_for_pinned_object(object_spilling_config,
                                             shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
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
def test_spill_remote_object(ray_start_cluster,
                             multi_node_object_spilling_config):
    cluster = ray_start_cluster
    object_spilling_config, _ = multi_node_object_spilling_config
    cluster.add_node(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        })
    ray.init(address=cluster.address)
    cluster.add_node(object_store_memory=75 * 1024 * 1024)
    cluster.wait_for_nodes()

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
    object_spilling_config, _ = object_spilling_config
    ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
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

    # randomly sample objects
    for _ in range(1000):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)


@pytest.mark.skipif(
    platform.system() in ["Darwin", "Windows"], reason="Failing on Windows.")
def test_spill_stats(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = object_spilling_config
    ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "max_io_workers": 100,
            "min_spilling_size": 1,
            "object_spilling_config": object_spilling_config
        },
    )

    @ray.remote
    def f():
        return np.zeros(50 * 1024 * 1024, dtype=np.uint8)

    ids = []
    for _ in range(4):
        x = f.remote()
        ids.append(x)

    while ids:
        print(ray.get(ids.pop()))

    x_id = f.remote()  # noqa
    ray.get(x_id)
    s = memory_summary()
    assert "Plasma memory usage 50 MiB, 1 objects, 50.0% full" in s, s
    assert "Spilled 200 MiB, 4 objects" in s, s
    assert "Restored 150 MiB, 3 objects" in s, s


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_during_get(object_spilling_config, shutdown_only):
    object_spilling_config, _ = object_spilling_config
    ray.init(
        num_cpus=4,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
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
    object_spilling_config, _ = object_spilling_config
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "automatic_object_spilling_enabled": True,
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
def test_delete_objects(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "min_spilling_size": 0,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
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
    platform.system() in ["Windows", "Darwin"], reason="Failing on Windows.")
def test_delete_objects_delete_while_creating(object_spilling_config,
                                              shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "min_spilling_size": 0,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
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
    platform.system() in ["Windows", "Darwin"], reason="Failing on Windows.")
def test_delete_objects_on_worker_failure(object_spilling_config,
                                          shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
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
def test_delete_objects_multi_node(multi_node_object_spilling_config,
                                   ray_start_cluster):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = multi_node_object_spilling_config
    cluster = ray_start_cluster
    # Head node.
    cluster.add_node(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 2,
            "min_spilling_size": 20 * 1024 * 1024,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
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


@pytest.mark.skipif(platform.system() == "Windows", reason="Flaky on Windows.")
def test_fusion_objects(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    min_spilling_size = 10 * 1024 * 1024
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 3,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
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
def do_test_release_resource(object_spilling_config, expect_released):
    object_spilling_config, temp_folder = object_spilling_config
    ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "release_resources_during_plasma_fetch": expect_released,
            "automatic_object_spilling_enabled": True,
            "object_spilling_config": object_spilling_config,
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
def test_no_release_during_plasma_fetch(object_spilling_config, shutdown_only):
    do_test_release_resource(object_spilling_config, expect_released=False)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_release_during_plasma_fetch(object_spilling_config, shutdown_only):
    do_test_release_resource(object_spilling_config, expect_released=True)


@pytest.mark.skip(
    reason="This hangs due to a deadlock between a worker getting its "
    "arguments and the node pulling arguments for the next task queued.")
@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
@pytest.mark.timeout(30)
def test_spill_objects_on_object_transfer(object_spilling_config,
                                          ray_start_cluster):
    object_spilling_config, _ = object_spilling_config
    # This test checks that objects get spilled to make room for transferred
    # objects.
    cluster = ray_start_cluster
    object_size = int(1e7)
    num_objects = 10
    num_tasks = 10
    # Head node can fit all of the objects at once.
    cluster.add_node(
        num_cpus=0,
        object_store_memory=2 * num_tasks * num_objects * object_size,
        _system_config={
            "max_io_workers": 1,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0
        })
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Worker node can fit 1 tasks at a time.
    cluster.add_node(
        num_cpus=1, object_store_memory=1.5 * num_objects * object_size)
    cluster.wait_for_nodes()

    @ray.remote
    def foo(*args):
        return

    @ray.remote
    def allocate(*args):
        return np.zeros(object_size, dtype=np.uint8)

    # Allocate some objects that must be spilled to make room for foo's
    # arguments.
    allocated = [allocate.remote() for _ in range(num_objects)]
    ray.get(allocated)
    print("done allocating")

    args = []
    for _ in range(num_tasks):
        task_args = [
            ray.put(np.zeros(object_size, dtype=np.uint8))
            for _ in range(num_objects)
        ]
        args.append(task_args)

    # Check that tasks scheduled to the worker node have enough room after
    # spilling.
    tasks = [foo.remote(*task_args) for task_args in args]
    ray.get(tasks)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
