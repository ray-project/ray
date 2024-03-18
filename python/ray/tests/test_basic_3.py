# coding: utf-8
import logging
import random
import sys
import time
import os

import pytest
import subprocess
import tempfile

import ray
import ray.cluster_utils
from ray._private.test_utils import dicts_equal

logger = logging.getLogger(__name__)


def test_auto_global_gc(shutdown_only):
    # 100MB
    ray.init(num_cpus=1, object_store_memory=100 * 1024 * 1024)

    @ray.remote
    class Test:
        def __init__(self):
            self.collected = False
            import gc

            gc.disable()

            def gc_called(phase, info):
                self.collected = True

            gc.callbacks.append(gc_called)

        def circular_ref(self):
            # 20MB
            buf1 = b"0" * (10 * 1024 * 1024)
            buf2 = b"1" * (10 * 1024 * 1024)
            ref1 = ray.put(buf1)
            ref2 = ray.put(buf2)
            b = []
            a = []
            b.append(a)
            a.append(b)
            b.append(ref1)
            a.append(ref2)
            return a

        def collected(self):
            return self.collected

    test = Test.remote()
    # 60MB
    for i in range(3):
        ray.get(test.circular_ref.remote())
    time.sleep(2)
    assert not ray.get(test.collected.remote())
    # 80MB
    for _ in range(1):
        ray.get(test.circular_ref.remote())
    time.sleep(2)
    assert ray.get(test.collected.remote())


@pytest.mark.skipif(
    sys.version_info >= (3, 10, 0),
    reason=("Currently not passing for Python 3.10"),
)
def test_many_fractional_resources(shutdown_only):
    ray.init(num_cpus=2, num_gpus=2, resources={"Custom": 2})

    @ray.remote
    def g():
        return 1

    @ray.remote
    def f(block, accepted_resources):
        true_resources = {
            resource: value[0][1]
            for resource, value in ray._private.worker.get_resource_ids().items()
        }
        if block:
            ray.get(g.remote())
        return dicts_equal(true_resources, accepted_resources)

    # Check that the resource are assigned correctly.
    result_ids = []
    for i in range(100):
        rand1 = random.random()
        rand2 = random.random()
        rand3 = random.random()

        resource_set = {"CPU": int(rand1 * 10000) / 10000}
        result_ids.append(
            f._remote([False, resource_set], num_cpus=resource_set["CPU"])
        )

        resource_set = {"CPU": 1, "GPU": int(rand1 * 10000) / 10000}
        result_ids.append(
            f._remote([False, resource_set], num_gpus=resource_set["GPU"])
        )

        resource_set = {"CPU": 1, "Custom": int(rand1 * 10000) / 10000}
        result_ids.append(
            f._remote(
                [False, resource_set], resources={"Custom": resource_set["Custom"]}
            )
        )

        resource_set = {
            "CPU": int(rand1 * 10000) / 10000,
            "GPU": int(rand2 * 10000) / 10000,
            "Custom": int(rand3 * 10000) / 10000,
        }
        result_ids.append(
            f._remote(
                [False, resource_set],
                num_cpus=resource_set["CPU"],
                num_gpus=resource_set["GPU"],
                resources={"Custom": resource_set["Custom"]},
            )
        )
        result_ids.append(
            f._remote(
                [True, resource_set],
                num_cpus=resource_set["CPU"],
                num_gpus=resource_set["GPU"],
                resources={"Custom": resource_set["Custom"]},
            )
        )
    assert all(ray.get(result_ids))

    # Check that the available resources at the end are the same as the
    # beginning.
    stop_time = time.time() + 10
    correct_available_resources = False
    while time.time() < stop_time:
        available_resources = ray.available_resources()
        if (
            "CPU" in available_resources
            and ray.available_resources()["CPU"] == 2.0
            and "GPU" in available_resources
            and ray.available_resources()["GPU"] == 2.0
            and "Custom" in available_resources
            and ray.available_resources()["Custom"] == 2.0
        ):
            correct_available_resources = True
            break
    if not correct_available_resources:
        assert False, "Did not get correct available resources."


# It uses slashes in Linux and MacOS, and backslashes in Windows.
WORKING_DIR_SUB_PATH = os.path.join("runtime_resources", "working_dir_files")


def test_worker_use_working_dir_path(shutdown_only):
    """
    Tests that, in a working_dir job, the worker uses the working dir as
    module search path, and NOT use the driver's path. This means the `lib` module used
    in the worker is from the working dir, not the driver's dir.
    """

    lib_code = """
def get_file_path():
    return __file__
"""

    runner_code = """
import ray
import lib

ray.init(runtime_env={"working_dir": "."})

@ray.remote
def my_file():
    return lib.get_file_path()

print(ray.get(my_file.remote()))
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "lib.py"), "w") as f:
            f.write(lib_code)
        with open(os.path.join(tmpdir, "runner.py"), "w") as f:
            f.write(runner_code)
        output = subprocess.check_output(
            [sys.executable, "runner.py"], cwd=tmpdir
        ).decode()
        assert WORKING_DIR_SUB_PATH in output, output


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="Jobs are not in Ray minimal",
)
@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 1,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_worker_use_working_dir_path_job(ray_start_cluster):
    """
    Same test as `test_worker_use_working_dir_path` but in job submission.
    """
    lib_code = """
def get_file_path():
    return __file__
"""

    runner_code = """
import ray
import lib

ray.init()

@ray.remote
def my_file():
    return lib.get_file_path()

print(ray.get(my_file.remote()))
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "lib.py"), "w") as f:
            f.write(lib_code)
        with open(os.path.join(tmpdir, "runner.py"), "w") as f:
            f.write(runner_code)

        output = subprocess.check_output(
            [
                "ray",
                "job",
                "submit",
                "--working-dir",
                tmpdir,
                "--",
                "python",
                "runner.py",
            ],
            cwd=tmpdir,
        ).decode()
        assert WORKING_DIR_SUB_PATH in output, output


def test_worker_use_driver_path_if_no_working_dir(shutdown_only):
    """
    Tests that, in a NO working_dir job, the worker adds the driver's path in the search
    path. This means the `lib` module used in the worker is from the driver's dir.

    """
    lib_code = """
def get_file_path():
    return __file__
"""

    runner_code = """
import ray
import lib

ray.init()

job_config = ray.worker.global_worker.core_worker.get_job_config()
driver_node_id = ray.NodeID(job_config.driver_node_id)
my_node_id = ray.worker.global_worker.current_node_id
assert driver_node_id == my_node_id

@ray.remote
def my_file():
    return lib.get_file_path()

print(ray.get(my_file.remote()))
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "lib.py"), "w") as f:
            f.write(lib_code)
        with open(os.path.join(tmpdir, "runner.py"), "w") as f:
            f.write(runner_code)
        output = subprocess.check_output([sys.executable, "runner.py"], cwd=tmpdir)
        output = output.decode()
        assert os.path.join(tmpdir, "lib.py") in output, output


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="Jobs are not in Ray minimal",
)
@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 1,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
def test_worker_use_driver_path_if_no_working_dir_job(ray_start_cluster):
    lib_code = """
def get_file_path():
    return __file__
"""

    runner_code = """
import ray
import lib

ray.init()

@ray.remote
def my_file():
    return lib.get_file_path()

print(ray.get(my_file.remote()))
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "lib.py"), "w") as f:
            f.write(lib_code)
        with open(os.path.join(tmpdir, "runner.py"), "w") as f:
            f.write(runner_code)

        output = subprocess.check_output(
            [
                "ray",
                "job",
                "submit",
                "--",
                "python",
                os.path.join(tmpdir, "runner.py"),
            ],
            cwd=tmpdir,
        ).decode()
        assert os.path.join(tmpdir, "lib.py") in output, output


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 1,
            "num_nodes": 2,
        },
    ],
    indirect=True,
)
def test_worker_exception_if_no_working_dir_diff_node(ray_start_cluster):
    """
    Tests that, in a cluster of 2 nodes, a job with no working_dir that runs
    a function with an import from driver's path, on both nodes.

    On the driver's node it should work, but on the other node it should raise an
    error about failing to import.
    """
    lib_code = """
def get_file_path():
    return __file__
"""

    runner_code = """
import os
import ray
import lib
ray.init()

lib_path = lib.get_file_path()

@ray.remote
def my_file():
    return lib.get_file_path()

node_ids = set(n['NodeID'] for n in ray.nodes())
my_node = ray.get_runtime_context().get_node_id()
other_node = (node_ids - set([my_node])).pop()

# runs on same node, works
assert ray.get(my_file.options(
    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=my_node,
        soft=False,
    )
).remote()) == lib_path

# runs on different node, fails
exc = None
try:
    ray.get(my_file.options(
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=other_node,
                soft=False,
            )
    ).remote())
except ray.exceptions.RayTaskError as e:
    exc = e
assert isinstance(exc, ray.exceptions.RayTaskError), type(exc)
assert "The remote function failed to import on the worker" in str(exc), str(exc)
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "lib.py"), "w") as f:
            f.write(lib_code)

        with open(os.path.join(tmpdir, "runner.py"), "w") as f:
            f.write(runner_code)

        subprocess.check_output([sys.executable, "runner.py"], cwd=tmpdir)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
