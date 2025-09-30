import copy
import json
import os
import platform
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

import ray
import ray.remote_function
from ray._common.test_utils import wait_for_condition
from ray._private.external_storage import (
    ExternalStorageSmartOpenImpl,
    FileSystemStorage,
    _get_unique_spill_filename,
    create_url_with_offset,
    parse_url_with_offset,
)
from ray._private.internal_api import memory_summary
from ray.tests.conftest import (
    buffer_object_spilling_config,
    file_system_object_spilling_config,
    mock_distributed_fs_object_spilling_config,
)

import psutil

# Note: Disk write speed can be as low as 6 MiB/s in AWS Mac instances, so we have to
# increase the timeout.
pytestmark = [pytest.mark.timeout(900 if platform.system() == "Darwin" else 180)]


def run_basic_workload():
    """Run the workload that requires spilling."""
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    refs = []
    refs.append([ray.put(arr) for _ in range(2)])
    ray.get(ray.put(arr))


def is_dir_empty(temp_folder, node_id, append_path=True):
    """Test if directory temp_folder/f"{DEFAULT_OBJECT_PREFIX}_{node_id}" is empty.
    For file based spilling, this is where the objects are spilled.
    For other use cases, specify append_path as False so that temp_folder itself
    is tested for emptiness.
    """
    # append_path is used because the file based spilling will append
    # new directory path.
    num_files = 0
    if append_path:
        temp_folder = (
            temp_folder
            / f"{ray._private.ray_constants.DEFAULT_OBJECT_PREFIX}_{node_id}"
        )
    if not temp_folder.exists():
        return True
    for path in temp_folder.iterdir():
        num_files += 1
    return num_files == 0


@pytest.mark.skipif(platform.system() == "Windows", reason="Doesn't support Windows.")
def test_spill_file_uniqueness(shutdown_only):
    ray_context = ray.init(num_cpus=0, object_store_memory=75 * 1024 * 1024)
    arr = np.random.rand(128 * 1024)  # 1 MB
    refs = []
    refs.append([ray.put(arr)])

    # for the same object_ref, generating spill urls 10 times yields
    # 10 different urls
    spill_url_set = {_get_unique_spill_filename(refs) for _ in range(10)}
    assert len(spill_url_set) == 10

    for StorageType in [FileSystemStorage, ExternalStorageSmartOpenImpl]:
        with patch.object(
            StorageType, "_get_objects_from_store"
        ) as mock_get_objects_from_store:
            mock_get_objects_from_store.return_value = [
                (b"somedata", b"metadata", None)
            ]
            storage = StorageType(ray_context["node_id"], "/tmp")
            spilled_url_set = {
                storage.spill_objects(refs, [b"localhost"])[0] for _ in range(10)
            }
            assert len(spilled_url_set) == 10


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
    ray_context = ray.init(num_cpus=0, object_store_memory=75 * 1024 * 1024)
    # Make sure the object spilling configuration is properly set.
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert (
        config["params"]["directory_path"]
        == ray._private.worker._global_node._session_dir
    )

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(
        Path(ray._private.worker._global_node._session_dir), ray_context["node_id"]
    )

    # Make sure the basic workload can succeed and the spill directory is not empty.
    run_basic_workload()
    assert not is_dir_empty(
        Path(ray._private.worker._global_node._session_dir), ray_context["node_id"]
    )
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
    assert "object_spilling_config" not in ray._private.worker._global_node._config
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
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "mock_distributed_fs"


def test_default_config_buffering(shutdown_only):
    ray.init(
        num_cpus=0,
        _system_config={
            "object_spilling_config": (json.dumps(buffer_object_spilling_config))
        },
    )
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
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


def test_custom_spill_dir_env_var(shutdown_only):
    os.environ["RAY_object_spilling_directory"] = "/tmp/custom_spill_dir"
    ray_context = ray.init(num_cpus=0, object_store_memory=75 * 1024 * 1024)
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == "/tmp/custom_spill_dir"

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    # Make sure the basic workload can succeed and the spill directory is not empty.
    run_basic_workload()
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


def test_custom_spill_dir_system_config(shutdown_only):
    ray_context = ray.init(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={"object_spilling_directory": "/tmp/custom_spill_dir"},
    )
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == "/tmp/custom_spill_dir"

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    # Make sure the basic workload can succeed and the spill directory is not empty.
    run_basic_workload()
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


def test_custom_spill_dir(shutdown_only):
    # Make sure the object spilling directory can be set by the user
    ray_context = ray.init(
        object_spilling_directory="/tmp/custom_spill_dir",
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
    )
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == "/tmp/custom_spill_dir"

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    # Make sure the basic workload can succeed and the spill directory is not empty.
    run_basic_workload()
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --object-spilling-directory=/tmp/custom_spill_dir --num-cpus 0 --object-store-memory 78643200"
    ],
    indirect=True,
)
def test_custom_spill_dir_cli(call_ray_start, shutdown_only):
    ray_context = ray.init(address=call_ray_start)

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    # Make sure the basic workload can succeed and the spill directory is not empty.
    run_basic_workload()
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


def test_custom_spill_dir_set_ray_params_and_system_config(shutdown_only):
    # Set directory in both ray params and system config.
    # ray params should take precedence.
    ray_context = ray.init(
        object_spilling_directory="/tmp/custom_spill_dir",
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={"object_spilling_directory": "/tmp/custom_spill_dir2"},
    )
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == "/tmp/custom_spill_dir"

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    run_basic_workload()
    # Make sure the spill directory is not empty after running the workload.
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


def test_custom_spill_dir_set_system_config_and_env_var(shutdown_only):
    # Set directory in both system config and env var.
    # system config should take precedence.
    os.environ["RAY_object_spilling_directory"] = "/tmp/custom_spill_dir2"
    ray_context = ray.init(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={"object_spilling_directory": "/tmp/custom_spill_dir"},
    )
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == "/tmp/custom_spill_dir"

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    run_basic_workload()
    # Make sure the spill directory is not empty after running the workload.
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


def test_set_custom_spill_dir_in_env_var_and_spill_config_in_system_config(
    shutdown_only,
):
    # Set directory in env var and object spilling config in system config.
    # the directory in env var should take precedence.
    os.environ["RAY_object_spilling_directory"] = "/tmp/custom_spill_dir"
    ray_context = ray.init(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "object_spilling_config": json.dumps(file_system_object_spilling_config)
        },
    )
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == "/tmp/custom_spill_dir"

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    run_basic_workload()
    # Make sure the spill directory is not empty after running the workload.
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


def test_set_object_spilling_config_in_system_config_and_env_var(shutdown_only):
    # Set object spilling config in both system config and env var.
    # the object spilling config in system config should take precedence.
    custom_object_spilling_config = {
        "type": "filesystem",
        "params": {"directory_path": "/tmp/custom_spill_dir"},
    }
    ray_context = ray.init(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "object_spilling_config": json.dumps(custom_object_spilling_config)
        },
    )
    os.environ["RAY_object_spilling_config"] = json.dumps(
        file_system_object_spilling_config
    )
    config = json.loads(
        ray._private.worker._global_node._config["object_spilling_config"]
    )
    assert config["type"] == "filesystem"
    assert config["params"]["directory_path"] == "/tmp/custom_spill_dir"

    # Make sure the spill directory is empty before running the workload.
    assert is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])

    run_basic_workload()
    # Make sure the spill directory is not empty after running the workload.
    assert not is_dir_empty(Path("/tmp/custom_spill_dir"), ray_context["node_id"])


def test_node_id_in_spill_dir_name():
    node_id = ray.NodeID.from_random().hex()
    session_dir = "test_session_dir"
    storage = ray._private.external_storage.setup_external_storage(
        file_system_object_spilling_config, node_id, session_dir
    )

    dir_prefix = ray._private.ray_constants.DEFAULT_OBJECT_PREFIX
    expected_dir_name = f"{dir_prefix}_{node_id}"
    for path in storage._directory_paths:
        dir_name = os.path.basename(path)
        assert dir_name == expected_dir_name

    # Clean up
    storage.destroy_external_storage()


@pytest.mark.skipif(platform.system() == "Windows", reason="Hangs on Windows.")
def test_spilling_not_done_for_pinned_object(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    ray_context = ray.init(
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

    print(type(temp_folder))
    wait_for_condition(lambda: is_dir_empty(temp_folder, ray_context["node_id"]))


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


@pytest.mark.skipif(platform.system() == "Windows", reason="Hangs on Windows.")
def test_spill_objects_automatically(fs_only_object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = fs_only_object_spilling_config
    ray.init(
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
        sample = ray.get(ref, timeout=None)
        assert np.array_equal(sample, solution)


@pytest.mark.skipif(
    platform.system() in ["Darwin"],
    reason="Very flaky on OSX.",
)
def test_unstable_spill_objects_automatically(unstable_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = unstable_spilling_config
    ray.init(
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
        sample = ray.get(ref, timeout=None)
        assert np.array_equal(sample, solution)


def test_slow_spill_objects_automatically(slow_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = slow_spilling_config
    ray.init(
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
        sample = ray.get(ref, timeout=None)
        assert np.array_equal(sample, solution)


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


@pytest.mark.skipif(platform.system() == "Darwin", reason="Failing on macOS.")
@pytest.mark.asyncio
@pytest.mark.parametrize("is_async", [False, True])
async def test_spill_during_get(object_spilling_config, shutdown_only, is_async):
    object_spilling_config, _ = object_spilling_config
    ray.init(
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
def test_recover_from_spill_worker_failure(ray_start_regular):
    @ray.remote
    def f():
        return np.zeros(50 * 1024 * 1024, dtype=np.uint8)

    def _run_spilling_workload():
        for obj_ref in [f.remote() for _ in range(5)]:
            ray.get(obj_ref)

    def get_spill_worker():
        for proc in psutil.process_iter():
            try:
                name = ray._private.ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE
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

    # Run a workload that forces spilling to occur.
    _run_spilling_workload()

    # Get the PID of the spill worker that was created and kill it.
    spill_worker_proc = get_spill_worker()
    assert spill_worker_proc
    spill_worker_proc.kill()
    spill_worker_proc.wait()

    # Run the workload again and ensure that it succeeds.
    _run_spilling_workload()

    # Check that the spilled files are cleaned up after the workload finishes.
    wait_for_condition(
        lambda: is_dir_empty(
            Path(ray._private.worker._global_node._session_dir),
            ray.get_runtime_context().get_node_id(),
        )
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
