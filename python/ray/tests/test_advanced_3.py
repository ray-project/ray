# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import logging
import os
import setproctitle
import shutil
import sys
import socket
import subprocess
import tempfile
import time

import numpy as np
import pickle
import pytest

import ray
from ray import signature
import ray.ray_constants as ray_constants
import ray.cluster_utils
import ray.test_utils

from ray.test_utils import RayTestTimeoutException

logger = logging.getLogger(__name__)


def attempt_to_load_balance(remote_function,
                            args,
                            total_tasks,
                            num_nodes,
                            minimum_count,
                            num_attempts=100):
    attempts = 0
    while attempts < num_attempts:
        locations = ray.get(
            [remote_function.remote(*args) for _ in range(total_tasks)])
        names = set(locations)
        counts = [locations.count(name) for name in names]
        logger.info("Counts are {}.".format(counts))
        if (len(names) == num_nodes
                and all(count >= minimum_count for count in counts)):
            break
        attempts += 1
    assert attempts < num_attempts


def test_load_balancing(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets
    # in a roughly equal manner.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 7
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.01)
        return ray.worker.global_worker.node.unique_id

    attempt_to_load_balance(f, [], 100, num_nodes, 10)
    attempt_to_load_balance(f, [], 1000, num_nodes, 100)


def test_load_balancing_with_dependencies(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        time.sleep(0.010)
        return ray.worker.global_worker.node.unique_id

    # This object will be local to one of the raylets. Make sure
    # this doesn't prevent tasks from being scheduled on other raylets.
    x = ray.put(np.zeros(1000000))

    attempt_to_load_balance(f, [x], 100, num_nodes, 25)


def wait_for_num_tasks(num_tasks, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.tasks()) >= num_tasks:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


def wait_for_num_objects(num_objects, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.objects()) >= num_objects:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
@pytest.mark.skipif(
    ray_constants.direct_call_enabled(), reason="state API not supported")
def test_global_state_api(shutdown_only):

    error_message = ("The ray global state API cannot be used "
                     "before ray.init has been called.")

    with pytest.raises(Exception, match=error_message):
        ray.objects()

    with pytest.raises(Exception, match=error_message):
        ray.tasks()

    with pytest.raises(Exception, match=error_message):
        ray.nodes()

    with pytest.raises(Exception, match=error_message):
        ray.jobs()

    ray.init(num_cpus=5, num_gpus=3, resources={"CustomResource": 1})

    assert ray.cluster_resources()["CPU"] == 5
    assert ray.cluster_resources()["GPU"] == 3
    assert ray.cluster_resources()["CustomResource"] == 1

    assert ray.objects() == {}

    job_id = ray.utils.compute_job_id_from_driver(
        ray.WorkerID(ray.worker.global_worker.worker_id))
    driver_task_id = ray.worker.global_worker.current_task_id.hex()

    # One task is put in the task table which corresponds to this driver.
    wait_for_num_tasks(1)
    task_table = ray.tasks()
    assert len(task_table) == 1
    assert driver_task_id == list(task_table.keys())[0]
    task_spec = task_table[driver_task_id]["TaskSpec"]
    nil_unique_id_hex = ray.UniqueID.nil().hex()
    nil_actor_id_hex = ray.ActorID.nil().hex()

    assert task_spec["TaskID"] == driver_task_id
    assert task_spec["ActorID"] == nil_actor_id_hex
    assert task_spec["Args"] == []
    assert task_spec["JobID"] == job_id.hex()
    assert task_spec["FunctionID"] == nil_unique_id_hex
    assert task_spec["ReturnObjectIDs"] == []

    client_table = ray.nodes()
    node_ip_address = ray.worker.global_worker.node_ip_address

    assert len(client_table) == 1
    assert client_table[0]["NodeManagerAddress"] == node_ip_address

    @ray.remote
    def f(*xs):
        return 1

    x_id = ray.put(1)
    result_id = f.remote(1, "hi", x_id)

    # Wait for one additional task to complete.
    wait_for_num_tasks(1 + 1)
    task_table = ray.tasks()
    assert len(task_table) == 1 + 1
    task_id_set = set(task_table.keys())
    task_id_set.remove(driver_task_id)
    task_id = list(task_id_set)[0]

    task_spec = task_table[task_id]["TaskSpec"]
    assert task_spec["ActorID"] == nil_actor_id_hex
    assert task_spec["Args"] == [
        signature.DUMMY_TYPE, 1, signature.DUMMY_TYPE, "hi",
        signature.DUMMY_TYPE, x_id
    ]
    assert task_spec["JobID"] == job_id.hex()
    assert task_spec["ReturnObjectIDs"] == [result_id]

    assert task_table[task_id] == ray.tasks(task_id)

    # Wait for two objects, one for the x_id and one for result_id.
    wait_for_num_objects(2)

    def wait_for_object_table():
        timeout = 10
        start_time = time.time()
        while time.time() - start_time < timeout:
            object_table = ray.objects()
            tables_ready = (object_table[x_id]["ManagerIDs"] is not None and
                            object_table[result_id]["ManagerIDs"] is not None)
            if tables_ready:
                return
            time.sleep(0.1)
        raise RayTestTimeoutException(
            "Timed out while waiting for object table to "
            "update.")

    object_table = ray.objects()
    assert len(object_table) == 2

    assert object_table[x_id] == ray.objects(x_id)
    object_table_entry = ray.objects(result_id)
    assert object_table[result_id] == object_table_entry

    job_table = ray.jobs()

    assert len(job_table) == 1
    assert job_table[0]["JobID"] == job_id.hex()
    assert job_table[0]["NodeManagerAddress"] == node_ip_address


# TODO(rkn): Pytest actually has tools for capturing stdout and stderr, so we
# should use those, but they seem to conflict with Ray's use of faulthandler.
class CaptureOutputAndError(object):
    """Capture stdout and stderr of some span.

    This can be used as follows.

        captured = {}
        with CaptureOutputAndError(captured):
            # Do stuff.
        # Access captured["out"] and captured["err"].
    """

    def __init__(self, captured_output_and_error):
        if sys.version_info >= (3, 0):
            import io
            self.output_buffer = io.StringIO()
            self.error_buffer = io.StringIO()
        else:
            import cStringIO
            self.output_buffer = cStringIO.StringIO()
            self.error_buffer = cStringIO.StringIO()
        self.captured_output_and_error = captured_output_and_error

    def __enter__(self):
        sys.stdout.flush()
        sys.stderr.flush()
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        sys.stdout = self.output_buffer
        sys.stderr = self.error_buffer

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr
        self.captured_output_and_error["out"] = self.output_buffer.getvalue()
        self.captured_output_and_error["err"] = self.error_buffer.getvalue()


def test_logging_to_driver(shutdown_only):
    ray.init(num_cpus=1, log_to_driver=True)

    @ray.remote
    def f():
        # It's important to make sure that these print statements occur even
        # without calling sys.stdout.flush() and sys.stderr.flush().
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)

    captured = {}
    with CaptureOutputAndError(captured):
        ray.get(f.remote())
        time.sleep(1)

    output_lines = captured["out"]
    for i in range(200):
        assert str(i) in output_lines

    # TODO(rkn): Check that no additional logs appear beyond what we expect
    # and that there are no duplicate logs. Once we address the issue
    # described in https://github.com/ray-project/ray/pull/5462, we should
    # also check that nothing is logged to stderr.


def test_not_logging_to_driver(shutdown_only):
    ray.init(num_cpus=1, log_to_driver=False)

    @ray.remote
    def f():
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()

    captured = {}
    with CaptureOutputAndError(captured):
        ray.get(f.remote())
        time.sleep(1)

    output_lines = captured["out"]
    assert len(output_lines) == 0

    # TODO(rkn): Check that no additional logs appear beyond what we expect
    # and that there are no duplicate logs. Once we address the issue
    # described in https://github.com/ray-project/ray/pull/5462, we should
    # also check that nothing is logged to stderr.


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_workers(shutdown_only):
    num_workers = 3
    ray.init(num_cpus=num_workers)

    @ray.remote
    def f():
        return id(ray.worker.global_worker), os.getpid()

    # Wait until all of the workers have started.
    worker_ids = set()
    while len(worker_ids) != num_workers:
        worker_ids = set(ray.get([f.remote() for _ in range(10)]))


def test_specific_job_id():
    dummy_driver_id = ray.JobID.from_int(1)
    ray.init(num_cpus=1, job_id=dummy_driver_id)

    # in driver
    assert dummy_driver_id == ray._get_runtime_context().current_driver_id

    # in worker
    @ray.remote
    def f():
        return ray._get_runtime_context().current_driver_id

    assert dummy_driver_id == ray.get(f.remote())

    ray.shutdown()


def test_object_id_properties():
    id_bytes = b"00112233445566778899"
    object_id = ray.ObjectID(id_bytes)
    assert object_id.binary() == id_bytes
    object_id = ray.ObjectID.nil()
    assert object_id.is_nil()
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(id_bytes + b"1234")
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(b"0123456789")
    object_id = ray.ObjectID.from_random()
    assert not object_id.is_nil()
    assert object_id.binary() != id_bytes
    id_dumps = pickle.dumps(object_id)
    id_from_dumps = pickle.loads(id_dumps)
    assert id_from_dumps == object_id


@pytest.fixture
def shutdown_only_with_initialization_check():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    assert not ray.is_initialized()


def test_initialized(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0)
    assert ray.is_initialized()


def test_initialized_local_mode(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0, local_mode=True)
    assert ray.is_initialized()


def test_wait_reconstruction(shutdown_only):
    ray.init(num_cpus=1, object_store_memory=int(10**8))

    @ray.remote
    def f():
        return np.zeros(6 * 10**7, dtype=np.uint8)

    x_id = f.remote()
    ray.wait([x_id])
    ray.wait([f.remote()])
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ready_ids, _ = ray.wait([x_id])
    assert len(ready_ids) == 1


def test_ray_setproctitle(ray_start_2_cpus):
    @ray.remote
    class UniqueName(object):
        def __init__(self):
            assert setproctitle.getproctitle() == "ray::UniqueName.__init__()"

        def f(self):
            assert setproctitle.getproctitle() == "ray::UniqueName.f()"

    @ray.remote
    def unique_1():
        assert "unique_1" in setproctitle.getproctitle()

    actor = UniqueName.remote()
    ray.get(actor.f.remote())
    ray.get(unique_1.remote())


def test_duplicate_error_messages(shutdown_only):
    ray.init(num_cpus=0)

    driver_id = ray.WorkerID.nil()
    error_data = ray.gcs_utils.construct_error_message(driver_id, "test",
                                                       "message", 0)

    # Push the same message to the GCS twice (they are the same because we
    # do not include a timestamp).

    r = ray.worker.global_worker.redis_client

    r.execute_command("RAY.TABLE_APPEND",
                      ray.gcs_utils.TablePrefix.Value("ERROR_INFO"),
                      ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB"),
                      driver_id.binary(), error_data)

    # Before https://github.com/ray-project/ray/pull/3316 this would
    # give an error
    r.execute_command("RAY.TABLE_APPEND",
                      ray.gcs_utils.TablePrefix.Value("ERROR_INFO"),
                      ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB"),
                      driver_id.binary(), error_data)


@pytest.mark.skipif(
    os.getenv("TRAVIS") is None,
    reason="This test should only be run on Travis.")
def test_ray_stack(ray_start_2_cpus):
    def unique_name_1():
        time.sleep(1000)

    @ray.remote
    def unique_name_2():
        time.sleep(1000)

    @ray.remote
    def unique_name_3():
        unique_name_1()

    unique_name_2.remote()
    unique_name_3.remote()

    success = False
    start_time = time.time()
    while time.time() - start_time < 30:
        # Attempt to parse the "ray stack" call.
        output = ray.utils.decode(subprocess.check_output(["ray", "stack"]))
        if ("unique_name_1" in output and "unique_name_2" in output
                and "unique_name_3" in output):
            success = True
            break

    if not success:
        raise Exception("Failed to find necessary information with "
                        "'ray stack'")


def test_pandas_parquet_serialization():
    # Only test this if pandas is installed
    pytest.importorskip("pandas")

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    tempdir = tempfile.mkdtemp()
    filename = os.path.join(tempdir, "parquet-test")
    pd.DataFrame({"col1": [0, 1], "col2": [0, 1]}).to_parquet(filename)
    with open(os.path.join(tempdir, "parquet-compression"), "wb") as f:
        table = pa.Table.from_arrays([pa.array([1, 2, 3])], ["hello"])
        pq.write_table(table, f, compression="lz4")
    # Clean up
    shutil.rmtree(tempdir)


def test_socket_dir_not_existing(shutdown_only):
    random_name = ray.ObjectID.from_random().hex()
    temp_raylet_socket_dir = "/tmp/ray/tests/{}".format(random_name)
    temp_raylet_socket_name = os.path.join(temp_raylet_socket_dir,
                                           "raylet_socket")
    ray.init(num_cpus=1, raylet_socket_name=temp_raylet_socket_name)


def test_raylet_is_robust_to_random_messages(ray_start_regular):
    node_manager_address = None
    node_manager_port = None
    for client in ray.nodes():
        if "NodeManagerAddress" in client:
            node_manager_address = client["NodeManagerAddress"]
            node_manager_port = client["NodeManagerPort"]
    assert node_manager_address
    assert node_manager_port
    # Try to bring down the node manager:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((node_manager_address, node_manager_port))
    s.send(1000 * b"asdf")

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


def test_non_ascii_comment(ray_start_regular):
    @ray.remote
    def f():
        # 日本語 Japanese comment
        return 1

    assert ray.get(f.remote()) == 1


def test_shutdown_disconnect_global_state():
    ray.init(num_cpus=0)
    ray.shutdown()

    with pytest.raises(Exception) as e:
        ray.objects()
    assert str(e.value).endswith("ray.init has been called.")


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [150 * 1024 * 1024], indirect=True)
def test_put_pins_object(ray_start_object_store_memory):
    x_id = ray.put("HI")
    x_copy = ray.ObjectID(x_id.binary())
    assert ray.get(x_copy) == "HI"

    # x cannot be evicted since x_id pins it
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    assert ray.get(x_id) == "HI"
    assert ray.get(x_copy) == "HI"

    # now it can be evicted since x_id pins it but x_copy does not
    del x_id
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(x_copy)

    # weakref put
    y_id = ray.put("HI", weakref=True)
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(y_id)

    @ray.remote
    def check_no_buffer_ref(x):
        assert x[0].get_buffer_ref() is None

    z_id = ray.put("HI")
    assert z_id.get_buffer_ref() is not None
    ray.get(check_no_buffer_ref.remote([z_id]))


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [150 * 1024 * 1024], indirect=True)
def test_redis_lru_with_set(ray_start_object_store_memory):
    x = np.zeros(8 * 10**7, dtype=np.uint8)
    x_id = ray.put(x, weakref=True)

    # Remove the object from the object table to simulate Redis LRU eviction.
    removed = False
    start_time = time.time()
    while time.time() < start_time + 10:
        if ray.state.state.redis_clients[0].delete(b"OBJECT" +
                                                   x_id.binary()) == 1:
            removed = True
            break
    assert removed

    # Now evict the object from the object store.
    ray.put(x)  # This should not crash.


def test_decorated_function(ray_start_regular):
    def function_invocation_decorator(f):
        def new_f(args, kwargs):
            # Reverse the arguments.
            return f(args[::-1], {"d": 5}), kwargs

        return new_f

    def f(a, b, c, d=None):
        return a, b, c, d

    f.__ray_invocation_decorator__ = function_invocation_decorator
    f = ray.remote(f)

    result_id, kwargs = f.remote(1, 2, 3, d=4)
    assert kwargs == {"d": 4}
    assert ray.get(result_id) == (3, 2, 1, 5)


def test_get_postprocess(ray_start_regular):
    def get_postprocessor(object_ids, values):
        return [value for value in values if value > 0]

    ray.worker.global_worker._post_get_hooks.append(get_postprocessor)

    assert ray.get(
        [ray.put(i) for i in [0, 1, 3, 5, -1, -3, 4]]) == [1, 3, 5, 4]


def test_export_after_shutdown(ray_start_regular):
    # This test checks that we can use actor and remote function definitions
    # across multiple Ray sessions.

    @ray.remote
    def f():
        pass

    @ray.remote
    class Actor(object):
        def method(self):
            pass

    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray and use the remote function and actor again.
    ray.init(num_cpus=1)
    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray again and make sure that these definitions can be exported from
    # workers.
    ray.init(num_cpus=2)

    @ray.remote
    def export_definitions_from_worker(remote_function, actor_class):
        ray.get(remote_function.remote())
        actor_handle = actor_class.remote()
        ray.get(actor_handle.method.remote())

    ray.get(export_definitions_from_worker.remote(f, Actor))


def test_invalid_unicode_in_worker_log(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    # Wait till first worker log file is created.
    while True:
        log_file_paths = glob.glob("{}/worker*.out".format(logs_dir))
        if len(log_file_paths) == 0:
            time.sleep(0.2)
        else:
            break

    with open(log_file_paths[0], "wb") as f:
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.flush()

    # Wait till the log monitor reads the file.
    time.sleep(1.0)

    # Make sure that nothing has died.
    assert ray.services.remaining_processes_alive()


@pytest.mark.skip(reason="This test is too expensive to run.")
def test_move_log_files_to_old(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    @ray.remote
    class Actor(object):
        def f(self):
            print("function f finished")

    # First create a temporary actor.
    actors = [
        Actor.remote() for i in range(ray_constants.LOG_MONITOR_MAX_OPEN_FILES)
    ]
    ray.get([a.f.remote() for a in actors])

    # Make sure no log files are in the "old" directory before the actors
    # are killed.
    assert len(glob.glob("{}/old/worker*.out".format(logs_dir))) == 0

    # Now kill the actors so the files get moved to logs/old/.
    [a.__ray_terminate__.remote() for a in actors]

    while True:
        log_file_paths = glob.glob("{}/old/worker*.out".format(logs_dir))
        if len(log_file_paths) > 0:
            with open(log_file_paths[0], "r") as f:
                assert "function f finished\n" in f.readlines()
            break

    # Make sure that nothing has died.
    assert ray.services.remaining_processes_alive()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
