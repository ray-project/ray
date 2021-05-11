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
from ray.test_utils import wait_for_condition, run_string_as_driver
from ray.tests.test_object_spilling import is_dir_empty, assert_no_thrashing


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
