# coding: utf-8
import glob
import logging
import os
import json
import sys
import socket
import time

import numpy as np
import pickle
import pytest

import ray
import ray.ray_constants as ray_constants
import ray.cluster_utils
import ray.test_utils
from ray import resource_spec
import setproctitle

from ray.test_utils import (check_call_ray, RayTestTimeoutException,
                            wait_for_num_actors)

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


def wait_for_num_objects(num_objects, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.objects()) >= num_objects:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


def test_global_state_api(shutdown_only):

    error_message = ("The ray global state API cannot be used "
                     "before ray.init has been called.")

    with pytest.raises(Exception, match=error_message):
        ray.objects()

    with pytest.raises(Exception, match=error_message):
        ray.actors()

    with pytest.raises(Exception, match=error_message):
        ray.nodes()

    with pytest.raises(Exception, match=error_message):
        ray.jobs()

    ray.init(num_cpus=5, num_gpus=3, resources={"CustomResource": 1})

    assert ray.cluster_resources()["CPU"] == 5
    assert ray.cluster_resources()["GPU"] == 3
    assert ray.cluster_resources()["CustomResource"] == 1

    # A driver/worker creates a temporary object during startup. Although the
    # temporary object is freed immediately, in a rare case, we can still find
    # the object ref in GCS because Raylet removes the object ref from GCS
    # asynchronously.
    # Because we can't control when workers create the temporary objects, so
    # We can't assert that `ray.objects()` returns an empty dict. Here we just
    # make sure `ray.objects()` succeeds.
    assert len(ray.objects()) >= 0

    job_id = ray.utils.compute_job_id_from_driver(
        ray.WorkerID(ray.worker.global_worker.worker_id))

    client_table = ray.nodes()
    node_ip_address = ray.worker.global_worker.node_ip_address

    assert len(client_table) == 1
    assert client_table[0]["NodeManagerAddress"] == node_ip_address

    @ray.remote
    class Actor:
        def __init__(self):
            pass

    _ = Actor.remote()  # noqa: F841
    # Wait for actor to be created
    wait_for_num_actors(1)

    actor_table = ray.actors()
    assert len(actor_table) == 1

    actor_info, = actor_table.values()
    assert actor_info["JobID"] == job_id.hex()
    assert "IPAddress" in actor_info["Address"]
    assert "IPAddress" in actor_info["OwnerAddress"]
    assert actor_info["Address"]["Port"] != actor_info["OwnerAddress"]["Port"]

    job_table = ray.jobs()

    assert len(job_table) == 1
    assert job_table[0]["JobID"] == job_id.hex()
    assert job_table[0]["DriverIPAddress"] == node_ip_address


# TODO(rkn): Pytest actually has tools for capturing stdout and stderr, so we
# should use those, but they seem to conflict with Ray's use of faulthandler.
class CaptureOutputAndError:
    """Capture stdout and stderr of some span.

    This can be used as follows.

        captured = {}
        with CaptureOutputAndError(captured):
            # Do stuff.
        # Access captured["out"] and captured["err"].
    """

    def __init__(self, captured_output_and_error):
        import io
        self.output_buffer = io.StringIO()
        self.error_buffer = io.StringIO()
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
        for i in range(10):
            print(i, end=" ")
            print(100 + i, end=" ", file=sys.stderr)

    captured = {}
    with CaptureOutputAndError(captured):
        ray.get(f.remote())
        time.sleep(1)

    out_lines = captured["out"]
    err_lines = captured["err"]
    for i in range(10):
        assert str(i) in out_lines

    for i in range(100, 110):
        assert str(i) in err_lines


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

    err_lines = captured["err"]
    assert len(err_lines) == 0


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
    assert dummy_driver_id == ray.worker.global_worker.current_job_id

    # in worker
    @ray.remote
    def f():
        return ray.worker.global_worker.current_job_id

    assert dummy_driver_id == ray.get(f.remote())

    ray.shutdown()


def test_object_ref_properties():
    id_bytes = b"00112233445566778899"
    object_ref = ray.ObjectRef(id_bytes)
    assert object_ref.binary() == id_bytes
    object_ref = ray.ObjectRef.nil()
    assert object_ref.is_nil()
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectRef(id_bytes + b"1234")
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectRef(b"0123456789")
    object_ref = ray.ObjectRef.from_random()
    assert not object_ref.is_nil()
    assert object_ref.binary() != id_bytes
    id_dumps = pickle.dumps(object_ref)
    id_from_dumps = pickle.loads(id_dumps)
    assert id_from_dumps == object_ref


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
    ray.init(
        num_cpus=1,
        object_store_memory=int(10**8),
        _internal_config=json.dumps({
            "object_pinning_enabled": 0
        }))

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
    class UniqueName:
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
        output = ray.utils.decode(
            check_call_ray(["stack"], capture_stdout=True))
        if ("unique_name_1" in output and "unique_name_2" in output
                and "unique_name_3" in output):
            success = True
            break

    if not success:
        raise Exception("Failed to find necessary information with "
                        "'ray stack'")


def test_socket_dir_not_existing(shutdown_only):
    if sys.platform != "win32":
        random_name = ray.ObjectRef.from_random().hex()
        temp_raylet_socket_dir = os.path.join(ray.utils.get_ray_temp_dir(),
                                              "tests", random_name)
        temp_raylet_socket_name = os.path.join(temp_raylet_socket_dir,
                                               "raylet_socket")
        ray.init(num_cpus=2, raylet_socket_name=temp_raylet_socket_name)

        @ray.remote
        def foo(x):
            time.sleep(1)
            return 2 * x

        ray.get([foo.remote(i) for i in range(2)])


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
    obj = np.ones(200 * 1024, dtype=np.uint8)
    x_id = ray.put(obj)
    x_binary = x_id.binary()
    assert (ray.get(ray.ObjectRef(x_binary)) == obj).all()

    # x cannot be evicted since x_id pins it
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    assert (ray.get(x_id) == obj).all()
    assert (ray.get(ray.ObjectRef(x_binary)) == obj).all()

    # now it can be evicted since x_id pins it but x_binary does not
    del x_id
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    assert not ray.worker.global_worker.core_worker.object_exists(
        ray.ObjectRef(x_binary))

    # weakref put
    y_id = ray.put(obj, weakref=True)
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(y_id)


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
    def get_postprocessor(object_refs, values):
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
    class Actor:
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
    class Actor:
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


def test_lease_request_leak(shutdown_only):
    ray.init(
        num_cpus=1,
        _internal_config=json.dumps({
            "initial_reconstruction_timeout_milliseconds": 200
        }))
    assert len(ray.objects()) == 0

    @ray.remote
    def f(x):
        time.sleep(0.1)
        return

    # Submit pairs of tasks. Tasks in a pair can reuse the same worker leased
    # from the raylet.
    tasks = []
    for _ in range(10):
        obj_ref = ray.put(1)
        for _ in range(2):
            tasks.append(f.remote(obj_ref))
        del obj_ref
    ray.get(tasks)

    time.sleep(
        1)  # Sleep for an amount longer than the reconstruction timeout.
    assert len(ray.objects()) == 0, ray.objects()


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 0,
        "num_nodes": 1,
        "do_init": False,
    }],
    indirect=True)
def test_ray_address_environment_variable(ray_start_cluster):
    address = ray_start_cluster.address
    # In this test we use zero CPUs to distinguish between starting a local
    # ray cluster and connecting to an existing one.

    # Make sure we connect to an existing cluster if
    # RAY_ADDRESS is set.
    os.environ["RAY_ADDRESS"] = address
    ray.init()
    assert "CPU" not in ray.state.cluster_resources()
    del os.environ["RAY_ADDRESS"]
    ray.shutdown()

    # Make sure we start a new cluster if RAY_ADDRESS is not set.
    ray.init()
    assert "CPU" in ray.state.cluster_resources()
    ray.shutdown()


def test_ray_resources_environment_variable(ray_start_cluster):
    address = ray_start_cluster.address

    os.environ["RAY_OVERRIDE_RESOURCES"] = "{\"custom1\":1, \"custom2\":2}"
    ray.init(address=address, resources={"custom1": 3, "custom3": 3})

    cluster_resources = ray.cluster_resources()
    print(cluster_resources)
    assert cluster_resources["custom1"] == 1
    assert cluster_resources["custom2"] == 2
    assert cluster_resources["custom3"] == 3


def test_gpu_info_parsing():
    info_string = """Model:           Tesla V100-SXM2-16GB
IRQ:             107
GPU UUID:        GPU-8eaaebb8-bb64-8489-fda2-62256e821983
Video BIOS:      88.00.4f.00.09
Bus Type:        PCIe
DMA Size:        47 bits
DMA Mask:        0x7fffffffffff
Bus Location:    0000:00:1e.0
Device Minor:    0
Blacklisted:     No
    """
    constraints_dict = resource_spec._constraints_from_gpu_info(info_string)
    expected_dict = {
        "{}V100".format(ray_constants.RESOURCE_CONSTRAINT_PREFIX): 1
    }
    assert constraints_dict == expected_dict

    info_string = """Model:           Tesla T4
IRQ:             10
GPU UUID:        GPU-415fe7a8-f784-6e3d-a958-92ecffacafe2
Video BIOS:      90.04.84.00.06
Bus Type:        PCIe
DMA Size:        47 bits
DMA Mask:        0x7fffffffffff
Bus Location:    0000:00:1b.0
Device Minor:    0
Blacklisted:     No
    """
    constraints_dict = resource_spec._constraints_from_gpu_info(info_string)
    expected_dict = {
        "{}T4".format(ray_constants.RESOURCE_CONSTRAINT_PREFIX): 1
    }
    assert constraints_dict == expected_dict

    assert resource_spec._constraints_from_gpu_info(None) == {}


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
