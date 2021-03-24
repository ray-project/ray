import copy
import json
import os
import random
import platform
import subprocess
import sys
from collections import defaultdict

import numpy as np
import pytest
import ray
from ray.external_storage import (create_url_with_offset,
                                  parse_url_with_offset)
from ray.test_utils import wait_for_condition, run_string_as_driver
from ray.internal.internal_api import memory_summary

# -- Smart open param --
bucket_name = "object-spilling-test"

# -- File system param --
spill_local_path = "/tmp/spill"

# -- Spilling configs --
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
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    if (request.param["type"] == "filesystem"
            or request.param["type"] == "mock_distributed_fs"):
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


def run_basic_workload():
    """Run the workload that requires spilling."""
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    refs = []
    refs.append([ray.put(arr) for _ in range(2)])
    ray.get(ray.put(arr))


def is_dir_empty(temp_folder,
                 append_path=ray.ray_constants.DEFAULT_OBJECT_PREFIX):
    # append_path is used because the file based spilling will append
    # new directory path.
    num_files = 0
    temp_folder = temp_folder / append_path
    for path in temp_folder.iterdir():
        num_files += 1
    return num_files == 0


def assert_no_thrashing(address):
    state = ray.state.GlobalState()
    state._initialize_global_state(address,
                                   ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    summary = memory_summary(address=address, stats_only=True)
    restored_bytes = 0
    consumed_bytes = 0

    for line in summary.split("\n"):
        if "Restored" in line:
            restored_bytes = int(line.split(" ")[1])
        if "consumed" in line:
            consumed_bytes = int(line.split(" ")[-2])
    assert consumed_bytes >= restored_bytes, (
        f"consumed: {consumed_bytes}, restored: {restored_bytes}")


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
def test_default_config(shutdown_only):
    ray.init(num_cpus=0, object_store_memory=75 * 1024 * 1024)
    # Make sure the object spilling configuration is properly set.
    config = json.loads(
        ray.worker._global_node._config["object_spilling_config"])
    assert config["type"] == "filesystem"
    assert (config["params"]["directory_path"] ==
            ray.worker._global_node._session_dir)
    # Make sure the basic workload can succeed.
    run_basic_workload()
    ray.shutdown()

    # Make sure config is not initalized if spilling is not enabled..
    ray.init(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": False,
            "object_store_full_delay_ms": 100
        })
    assert "object_spilling_config" not in ray.worker._global_node._config
    with pytest.raises(ray.exceptions.ObjectStoreFullError):
        run_basic_workload()
    ray.shutdown()

    # Make sure when we use a different config, it is reflected.
    ray.init(
        num_cpus=0,
        _system_config={
            "object_spilling_config": (
                json.dumps(mock_distributed_fs_object_spilling_config))
        })
    config = json.loads(
        ray.worker._global_node._config["object_spilling_config"])
    assert config["type"] == "mock_distributed_fs"


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_default_config_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(cluster.address)
    worker_nodes = []
    worker_nodes.append(
        cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024))
    cluster.wait_for_nodes()

    # Run the basic spilling workload on both
    # worker nodes and make sure they are working.
    @ray.remote
    def task():
        arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
        refs = []
        refs.append([ray.put(arr) for _ in range(2)])
        ray.get(ray.put(arr))

    ray.get([task.remote() for _ in range(2)])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spilling_not_done_for_pinned_object(object_spilling_config,
                                             shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    address = ray.init(
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

    wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["redis_address"])


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
    assert_no_thrashing(cluster.address)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_objects_automatically(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = object_spilling_config
    address = ray.init(
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
    print("spill done.")
    # randomly sample objects
    for _ in range(1000):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() in ["Windows"], reason="Failing on Windows.")
def test_spill_stats(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = object_spilling_config
    address = ray.init(
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
    s = memory_summary(address=address["redis_address"], stats_only=True)
    assert "Plasma memory usage 50 MiB, 1 objects, 50.0% full" in s, s
    assert "Spilled 200 MiB, 4 objects" in s, s
    assert "Restored 150 MiB, 3 objects" in s, s

    # Test if consumed bytes are correctly calculated.
    obj = ray.put(np.zeros(30 * 1024 * 1024, dtype=np.uint8))

    @ray.remote
    def func_with_ref(obj):
        return True

    ray.get(func_with_ref.remote(obj))

    s = memory_summary(address=address["redis_address"], stats_only=True)
    # 50MB * 5 references + 30MB used for task execution.
    assert "Objects consumed by Ray tasks: 280 MiB." in s, s
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_during_get(object_spilling_config, shutdown_only):
    object_spilling_config, _ = object_spilling_config
    address = ray.init(
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
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_deadlock(object_spilling_config, shutdown_only):
    object_spilling_config, _ = object_spilling_config
    # Limit our object store to 75 MiB of memory.
    address = ray.init(
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
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_delete_objects(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config

    address = ray.init(
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

    del replay_buffer
    del ref
    wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() in ["Windows"], reason="Failing on Windows.")
def test_delete_objects_delete_while_creating(object_spilling_config,
                                              shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config

    address = ray.init(
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

    # After all, make sure all objects are killed without race condition.
    del replay_buffer
    del ref
    wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() in ["Windows"], reason="Failing on Windows.")
def test_delete_objects_on_worker_failure(object_spilling_config,
                                          shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config

    address = ray.init(
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

    # After all, make sure all objects are deleted upon worker failures.
    wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() in ["Windows"], reason="Failing on Windows and MacOS.")
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
    ray.init(address=cluster.address)
    # Add 2 worker nodes.
    for _ in range(2):
        cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    cluster.wait_for_nodes()

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
            for _ in range(50):
                ref = random.choice(self.replay_buffer)
                sample = ray.get(ref, timeout=10)
                assert np.array_equal(sample, arr)

    actors = [Actor.remote() for _ in range(3)]
    ray.get([actor.create_objects.remote() for actor in actors])

    def wait_until_actor_dead(actor):
        try:
            ray.get(actor.ping.remote())
        except ray.exceptions.RayActorError:
            return True
        return False

    # Kill actors to remove all references.
    for actor in actors:
        ray.kill(actor)
        wait_for_condition(lambda: wait_until_actor_dead(actor))
    # The multi node deletion should work.
    wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(cluster.address)


@pytest.mark.skipif(platform.system() == "Windows", reason="Flaky on Windows.")
def test_fusion_objects(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    min_spilling_size = 10 * 1024 * 1024
    address = ray.init(
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
    # Since we'd like to see the temp directory that stores the files,
    # we need to append this directory.
    temp_folder = temp_folder / ray.ray_constants.DEFAULT_OBJECT_PREFIX
    for path in temp_folder.iterdir():
        file_size = path.stat().st_size
        # Make sure there are at least one
        # file_size that exceeds the min_spilling_size.
        # If we don't fusion correctly, this cannot happen.
        if file_size >= min_spilling_size:
            is_test_passing = True
    assert is_test_passing
    assert_no_thrashing(address["redis_address"])


# https://github.com/ray-project/ray/issues/12912
def do_test_release_resource(object_spilling_config, expect_released):
    object_spilling_config, temp_folder = object_spilling_config
    address = ray.init(
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
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_no_release_during_plasma_fetch(object_spilling_config, shutdown_only):
    do_test_release_resource(object_spilling_config, expect_released=False)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_release_during_plasma_fetch(object_spilling_config, shutdown_only):
    do_test_release_resource(object_spilling_config, expect_released=True)


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
    assert_no_thrashing(cluster.address)


@pytest.mark.skipif(
    platform.system() in ["Windows"], reason="Failing on "
    "Windows and Mac.")
def test_file_deleted_when_driver_exits(tmp_path, shutdown_only):
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()

    driver = """
import json
import os
import signal
import numpy as np
import ray
ray.init(
    object_store_memory=75 * 1024 * 1024,
    _system_config={{
        "max_io_workers": 2,
        "min_spilling_size": 0,
        "automatic_object_spilling_enabled": True,
        "object_store_full_delay_ms": 100,
        "object_spilling_config": json.dumps({{
            "type": "filesystem",
            "params": {{
                "directory_path": "{temp_dir}"
            }}
        }}),
    }})
arr = np.random.rand(1024 * 1024)  # 8 MB data
replay_buffer = []
# Spill lots of objects
for _ in range(30):
    ref = None
    while ref is None:
        ref = ray.put(arr)
        replay_buffer.append(ref)
# Send sigterm to itself.
signum = {signum}
sig = None
if signum == 2:
    sig = signal.SIGINT
elif signum == 15:
    sig = signal.SIGTERM
os.kill(os.getpid(), sig)
"""

    # Run a driver with sigint.
    print("Sending sigint...")
    with pytest.raises(subprocess.CalledProcessError):
        print(
            run_string_as_driver(
                driver.format(temp_dir=str(temp_folder), signum=2)))
    wait_for_condition(lambda: is_dir_empty(temp_folder, append_path=""))


@pytest.mark.skipif(
    platform.system() in ["Windows"], reason="Failing on "
    "Windows.")
def test_multiple_directories(tmp_path, shutdown_only):
    num_dirs = 3
    temp_dirs = []
    for i in range(num_dirs):
        temp_folder = tmp_path / f"spill_{i}"
        temp_folder.mkdir()
        temp_dirs.append(temp_folder)

    # Limit our object store to 75 MiB of memory.
    min_spilling_size = 0
    object_spilling_config = json.dumps({
        "type": "filesystem",
        "params": {
            "directory_path": [str(directory) for directory in temp_dirs]
        }
    })
    address = ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 5,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": min_spilling_size,
        })

    arr = np.ones(74 * 1024 * 1024, dtype=np.uint8)  # 74MB.
    object_refs = []
    # Now the storage is full.
    object_refs.append(ray.put(arr))

    num_object_spilled = 20
    for _ in range(num_object_spilled):
        object_refs.append(ray.put(arr))

    num_files = defaultdict(int)
    for temp_dir in temp_dirs:
        temp_folder = temp_dir / ray.ray_constants.DEFAULT_OBJECT_PREFIX
        for path in temp_folder.iterdir():
            num_files[str(temp_folder)] += 1

    for ref in object_refs:
        assert np.array_equal(ray.get(ref), arr)

    print("Check distribution...")
    min_count = 5
    is_distributed = [n_files >= min_count for n_files in num_files.values()]
    assert all(is_distributed)

    print("Check deletion...")
    # Empty object refs.
    object_refs = []
    # Add a new object so that the last entry is evicted.
    ref = ray.put(arr)
    for temp_dir in temp_dirs:
        temp_folder = temp_dir
        wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["redis_address"])

    # Now kill ray and see all directories are deleted.
    print("Check directories are deleted...")
    ray.shutdown()
    for temp_dir in temp_dirs:
        wait_for_condition(lambda: is_dir_empty(temp_dir, append_path=""))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
