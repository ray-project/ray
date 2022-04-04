import copy
import json
import platform
import random
import sys
from datetime import datetime, timedelta

import numpy as np
import pytest
import ray
from ray.tests.conftest import (
    file_system_object_spilling_config,
    buffer_object_spilling_config,
    mock_distributed_fs_object_spilling_config,
)
from ray.external_storage import create_url_with_offset, parse_url_with_offset
from ray._private.test_utils import wait_for_condition
from ray.internal.internal_api import memory_summary
from ray._raylet import GcsClientOptions


def run_basic_workload():
    """Run the workload that requires spilling."""
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    refs = []
    refs.append([ray.put(arr) for _ in range(2)])
    ray.get(ray.put(arr))


def is_dir_empty(temp_folder, append_path=ray.ray_constants.DEFAULT_OBJECT_PREFIX):
    # append_path is used because the file based spilling will append
    # new directory path.
    num_files = 0
    temp_folder = temp_folder / append_path
    if not temp_folder.exists():
        return True
    for path in temp_folder.iterdir():
        num_files += 1
    return num_files == 0


def assert_no_thrashing(address):
    state = ray.state.GlobalState()
    options = GcsClientOptions.from_gcs_address(address)
    state._initialize_global_state(options)
    summary = memory_summary(address=address, stats_only=True)
    restored_bytes = 0
    consumed_bytes = 0

    for line in summary.split("\n"):
        if "Restored" in line:
            restored_bytes = int(line.split(" ")[1])
        if "consumed" in line:
            consumed_bytes = int(line.split(" ")[-2])
    assert (
        consumed_bytes >= restored_bytes
    ), f"consumed: {consumed_bytes}, restored: {restored_bytes}"


def test_invalid_config_raises_exception(shutdown_only):
    # Make sure ray.init raises an exception before
    # it starts processes when invalid object spilling
    # config is given.
    with pytest.raises(ValueError):
        ray.init(
            _system_config={
                "object_spilling_config": json.dumps({"type": "abc"}),
            }
        )

    with pytest.raises(Exception):
        copied_config = copy.deepcopy(file_system_object_spilling_config)
        # Add invalid params to the config.
        copied_config["params"].update({"random_arg": "abc"})
        ray.init(
            _system_config={
                "object_spilling_config": json.dumps(copied_config),
            }
        )

    with pytest.raises(Exception):
        copied_config = copy.deepcopy(file_system_object_spilling_config)
        # Add invalid value type to the config.
        copied_config["params"].update({"buffer_size": "abc"})
        ray.init(
            _system_config={
                "object_spilling_config": json.dumps(copied_config),
            }
        )


def test_url_generation_and_parse():
    url = "s3://abc/def/ray_good"
    offset = 10
    size = 30
    url_with_offset = create_url_with_offset(url=url, offset=offset, size=size)
    parsed_result = parse_url_with_offset(url_with_offset)
    assert parsed_result.base_url == url
    assert parsed_result.offset == offset
    assert parsed_result.size == size


def test_default_config(shutdown_only):
    ray.init(num_cpus=0, object_store_memory=75 * 1024 * 1024)
    # Make sure the object spilling configuration is properly set.
    config = json.loads(ray.worker._global_node._config["object_spilling_config"])
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == ray.worker._global_node._session_dir
    # Make sure the basic workload can succeed.
    run_basic_workload()
    ray.shutdown()

    # Make sure config is not initalized if spilling is not enabled..
    ray.init(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": False,
            "object_store_full_delay_ms": 100,
        },
    )
    assert "object_spilling_config" not in ray.worker._global_node._config
    run_basic_workload()
    ray.shutdown()

    # Make sure when we use a different config, it is reflected.
    ray.init(
        num_cpus=0,
        _system_config={
            "object_spilling_config": (
                json.dumps(mock_distributed_fs_object_spilling_config)
            )
        },
    )
    config = json.loads(ray.worker._global_node._config["object_spilling_config"])
    assert config["type"] == "mock_distributed_fs"


def test_default_config_buffering(shutdown_only):
    ray.init(
        num_cpus=0,
        _system_config={
            "object_spilling_config": (json.dumps(buffer_object_spilling_config))
        },
    )
    config = json.loads(ray.worker._global_node._config["object_spilling_config"])
    assert config["type"] == buffer_object_spilling_config["type"]
    assert (
        config["params"]["buffer_size"]
        == buffer_object_spilling_config["params"]["buffer_size"]
    )


def test_default_config_cluster(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=0)
    ray.init(cluster.address)
    worker_nodes = []
    worker_nodes.append(
        cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024)
    )
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


@pytest.mark.skipif(platform.system() == "Windows", reason="Hangs on Windows.")
def test_spilling_not_done_for_pinned_object(object_spilling_config, shutdown_only):
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
        },
    )
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    ref = ray.get(ray.put(arr))  # noqa
    ref2 = ray.put(arr)  # noqa

    wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["address"])


def test_spill_remote_object(
    ray_start_cluster_enabled, multi_node_object_spilling_config
):
    cluster = ray_start_cluster_enabled
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
        },
    )
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


@pytest.mark.skipif(platform.system() == "Windows", reason="Hangs on Windows.")
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
            "min_spilling_size": 0,
        },
    )
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
    assert_no_thrashing(address["address"])


@pytest.mark.skipif(
    platform.system() in ["Darwin"],
    reason="Very flaky on OSX.",
)
def test_unstable_spill_objects_automatically(unstable_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = unstable_spilling_config
    address = ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )
    replay_buffer = []
    solution_buffer = []
    buffer_length = 20

    # Each object averages 16MiB => 320MiB total.
    for _ in range(buffer_length):
        multiplier = random.choice([1, 2, 3])
        arr = np.random.rand(multiplier * 1024 * 1024)
        ref = ray.put(arr)
        replay_buffer.append(ref)
        solution_buffer.append(arr)
    print("spill done.")
    # randomly sample objects
    for _ in range(10):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)
    assert_no_thrashing(address["address"])


def test_slow_spill_objects_automatically(slow_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = slow_spilling_config
    address = ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )
    replay_buffer = []
    solution_buffer = []
    buffer_length = 10

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
    for _ in range(buffer_length):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)
    assert_no_thrashing(address["address"])


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
            "object_spilling_config": object_spilling_config,
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
    s = memory_summary(address=address["address"], stats_only=True)
    assert "Plasma memory usage 50 MiB, 1 objects, 50.0% full" in s, s
    assert "Spilled 200 MiB, 4 objects" in s, s
    assert "Restored 150 MiB, 3 objects" in s, s

    # Test if consumed bytes are correctly calculated.
    obj = ray.put(np.zeros(30 * 1024 * 1024, dtype=np.uint8))

    @ray.remote
    def func_with_ref(obj):
        return True

    ray.get(func_with_ref.remote(obj))

    s = memory_summary(address=address["address"], stats_only=True)
    # 50MB * 5 references + 30MB used for task execution.
    assert "Objects consumed by Ray tasks: 280 MiB." in s, s
    assert_no_thrashing(address["address"])


@pytest.mark.skipif(platform.system() == "Darwin", reason="Failing on macOS.")
@pytest.mark.asyncio
@pytest.mark.parametrize("is_async", [False, True])
async def test_spill_during_get(object_spilling_config, shutdown_only, is_async):
    object_spilling_config, _ = object_spilling_config
    address = ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "max_io_workers": 1,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
            "worker_register_timeout_seconds": 600,
        },
    )

    if is_async:

        @ray.remote(num_cpus=0)
        class Actor:
            async def f(self):
                return np.zeros(10 * 1024 * 1024)

    else:

        @ray.remote(num_cpus=0)
        def f():
            return np.zeros(10 * 1024 * 1024)

    if is_async:
        a = Actor.remote()
    ids = []
    for i in range(10):
        if is_async:
            x = a.f.remote()
        else:
            x = f.remote()
        print(i, x)
        ids.append(x)

    start = datetime.now()
    # Concurrent gets, which require restoring from external storage, while
    # objects are being created.
    for x in ids:
        if is_async:
            obj = await x
        else:
            obj = ray.get(x)
        print(obj.shape)
        del obj

    timeout_seconds = 30
    duration = datetime.now() - start
    assert duration <= timedelta(
        seconds=timeout_seconds
    ), "Concurrent gets took too long. Maybe IO workers are not started properly."  # noqa: E501
    assert_no_thrashing(address["address"])


@pytest.mark.parametrize(
    "ray_start_regular",
    [
        {
            "object_store_memory": 75 * 1024 * 1024,
            "_system_config": {"max_io_workers": 1},
        }
    ],
    indirect=True,
)
def test_spill_worker_failure(ray_start_regular):
    def run_workload():
        @ray.remote
        def f():
            return np.zeros(50 * 1024 * 1024, dtype=np.uint8)

        ids = []
        for _ in range(5):
            x = f.remote()
            ids.append(x)
        for id in ids:
            ray.get(id)
        del ids

    run_workload()

    def get_spill_worker():
        import psutil

        for proc in psutil.process_iter():
            try:
                name = ray.ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE
                if name in proc.name():
                    return proc
                # for macOS
                if proc.cmdline() and name in proc.cmdline()[0]:
                    return proc
                # for Windows
                if proc.cmdline() and "--worker-type=SPILL_WORKER" in proc.cmdline():
                    return proc
            except psutil.AccessDenied:
                pass
            except psutil.NoSuchProcess:
                pass

    # Spilling occurred. Get the PID of the spill worker.
    spill_worker_proc = get_spill_worker()
    assert spill_worker_proc

    # Kill the spill worker
    spill_worker_proc.kill()
    spill_worker_proc.wait()

    # Now we trigger spilling again
    run_workload()

    # A new spill worker should be created
    spill_worker_proc = get_spill_worker()
    assert spill_worker_proc


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
