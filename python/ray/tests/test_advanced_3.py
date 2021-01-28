# coding: utf-8
import collections
import glob
import logging
import os
import sys
import socket
import tempfile
import time

import numpy as np
import pickle
import pytest

import ray
import ray.ray_constants as ray_constants
import ray.util.accelerators
import ray.cluster_utils
import ray.test_utils
from ray import resource_spec
import setproctitle
import subprocess

from ray.test_utils import (check_call_ray, RayTestTimeoutException,
                            wait_for_condition, wait_for_num_actors,
                            new_scheduler_enabled)

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
        counts = collections.Counter(locations)
        logger.info(f"Counts are {counts}")
        if (len(counts) == num_nodes
                and counts.most_common()[-1][1] >= minimum_count):
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


def test_local_scheduling_first(ray_start_cluster):
    cluster = ray_start_cluster
    num_cpus = 8
    # Disable worker caching.
    cluster.add_node(
        num_cpus=num_cpus,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
        })
    cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.01)
        return ray.worker.global_worker.node.unique_id

    def local():
        return ray.get(f.remote()) == ray.worker.global_worker.node.unique_id

    # Wait for a worker to get started.
    wait_for_condition(local)

    # Check that we are scheduling locally while there are resources available.
    for i in range(20):
        assert local()


@pytest.mark.parametrize("fast", [True, False])
def test_load_balancing_with_dependencies(ray_start_cluster, fast):
    if fast and new_scheduler_enabled:
        # Load-balancing on new scheduler can be inefficient if (task
        # duration:heartbeat interval) is small enough.
        pytest.skip()

    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        if fast:
            time.sleep(0.010)
        else:
            time.sleep(0.1)
        return ray.worker.global_worker.node.unique_id

    # This object will be local to one of the raylets. Make sure
    # this doesn't prevent tasks from being scheduled on other raylets.
    x = ray.put(np.zeros(1000000))

    attempt_to_load_balance(f, [x], 100, num_nodes, 25)


def test_locality_aware_leasing(ray_start_cluster):
    # This test ensures that a task will run where its task dependencies are
    # located. We run an initial non_local() task that is pinned to a
    # non-local node via a custom resource constraint, and then we run an
    # unpinned task f() that depends on the output of non_local(), ensuring
    # that f() runs on the same node as non_local().
    cluster = ray_start_cluster

    # Disable worker caching so worker leases are not reused, and disable
    # inlining of return objects so return objects are always put into Plasma.
    cluster.add_node(
        num_cpus=1,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 0,
        })
    # Use a custom resource for pinning tasks to a node.
    non_local_node = cluster.add_node(num_cpus=1, resources={"pin": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin": 1})
    def non_local():
        return ray.worker.global_worker.node.unique_id

    @ray.remote
    def f(x):
        return ray.worker.global_worker.node.unique_id

    # Test that task f() runs on the same node as non_local().
    assert ray.get(f.remote(non_local.remote())) == non_local_node.unique_id


def wait_for_num_objects(num_objects, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.objects()) >= num_objects:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


def test_global_state_api(shutdown_only):

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

    _ = Actor.options(name="test_actor").remote()  # noqa: F841
    # Wait for actor to be created
    wait_for_num_actors(1)

    actor_table = ray.actors()
    assert len(actor_table) == 1

    actor_info, = actor_table.values()
    assert actor_info["JobID"] == job_id.hex()
    assert actor_info["Name"] == "test_actor"
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


def test_object_ref_properties():
    id_bytes = b"0011223344556677889900001111"
    object_ref = ray.ObjectRef(id_bytes)
    assert object_ref.binary() == id_bytes
    object_ref = ray.ObjectRef.nil()
    assert object_ref.is_nil()
    with pytest.raises(ValueError, match=r".*needs to have length.*"):
        ray.ObjectRef(id_bytes + b"1234")
    with pytest.raises(ValueError, match=r".*needs to have length.*"):
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
        _system_config={"object_pinning_enabled": 0})

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


def test_ray_task_name_setproctitle(ray_start_2_cpus):
    method_task_name = "foo"

    @ray.remote
    class UniqueName:
        def __init__(self):
            assert setproctitle.getproctitle() == "ray::UniqueName.__init__()"

        def f(self):
            assert setproctitle.getproctitle() == f"ray::{method_task_name}"

    task_name = "bar"

    @ray.remote
    def unique_1():
        assert task_name in setproctitle.getproctitle()

    actor = UniqueName.remote()
    ray.get(actor.f.options(name=method_task_name).remote())
    ray.get(unique_1.options(name=task_name).remote())


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


def test_ray_start_and_stop():
    for i in range(10):
        subprocess.check_call(["ray", "start", "--head"])
        subprocess.check_call(["ray", "stop"])


def test_invalid_unicode_in_worker_log(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    # Wait till first worker log file is created.
    while True:
        log_file_paths = glob.glob(f"{logs_dir}/worker*.out")
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
    assert ray._private.services.remaining_processes_alive()


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
    assert len(glob.glob(f"{logs_dir}/old/worker*.out")) == 0

    # Now kill the actors so the files get moved to logs/old/.
    [a.__ray_terminate__.remote() for a in actors]

    while True:
        log_file_paths = glob.glob(f"{logs_dir}/old/worker*.out")
        if len(log_file_paths) > 0:
            with open(log_file_paths[0], "r") as f:
                assert "function f finished\n" in f.readlines()
            break

    # Make sure that nothing has died.
    assert ray._private.services.remaining_processes_alive()


def test_lease_request_leak(shutdown_only):
    ray.init(num_cpus=1, _system_config={"object_timeout_milliseconds": 200})
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

    os.environ[
        "RAY_OVERRIDE_RESOURCES"] = "{\"custom1\":1, \"custom2\":2, \"CPU\":3}"
    ray.init(address=address, resources={"custom1": 3, "custom3": 3})

    cluster_resources = ray.cluster_resources()
    print(cluster_resources)
    assert cluster_resources["custom1"] == 1
    assert cluster_resources["custom2"] == 2
    assert cluster_resources["custom3"] == 3
    assert cluster_resources["CPU"] == 3


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
        f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}V100": 1,
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
        f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}T4": 1,
    }
    assert constraints_dict == expected_dict

    assert resource_spec._constraints_from_gpu_info(None) == {}


def test_accelerator_type_api(shutdown_only):
    v100 = ray.util.accelerators.NVIDIA_TESLA_V100
    resource_name = f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}{v100}"
    ray.init(num_cpus=4, resources={resource_name: 1})

    quantity = 1

    @ray.remote(accelerator_type=v100)
    def decorated_func(quantity):
        wait_for_condition(
            lambda: ray.available_resources()[resource_name] < quantity)
        return True

    assert ray.get(decorated_func.remote(quantity))

    def via_options_func(quantity):
        wait_for_condition(
            lambda: ray.available_resources()[resource_name] < quantity)
        return True

    assert ray.get(
        ray.remote(via_options_func).options(
            accelerator_type=v100).remote(quantity))

    @ray.remote(accelerator_type=v100)
    class DecoratedActor:
        def __init__(self):
            pass

        def initialized(self):
            pass

    class ActorWithOptions:
        def __init__(self):
            pass

        def initialized(self):
            pass

    decorated_actor = DecoratedActor.remote()
    # Avoid a race condition where the actor hasn't been initialized and
    # claimed the resources yet.
    ray.get(decorated_actor.initialized.remote())
    wait_for_condition(
        lambda: ray.available_resources()[resource_name] < quantity)

    quantity = ray.available_resources()[resource_name]
    with_options = ray.remote(ActorWithOptions).options(
        accelerator_type=v100).remote()
    ray.get(with_options.initialized.remote())
    wait_for_condition(
        lambda: ray.available_resources()[resource_name] < quantity)


def test_detect_docker_cpus():
    # No limits set
    with tempfile.NamedTemporaryFile(
            "w") as quota_file, tempfile.NamedTemporaryFile(
                "w") as period_file, tempfile.NamedTemporaryFile(
                    "w") as cpuset_file:
        quota_file.write("-1")
        period_file.write("100000")
        cpuset_file.write("0-63")
        quota_file.flush()
        period_file.flush()
        cpuset_file.flush()
        assert ray.utils._get_docker_cpus(
            cpu_quota_file_name=quota_file.name,
            cpu_share_file_name=period_file.name,
            cpuset_file_name=cpuset_file.name) == 64

    # No cpuset used
    with tempfile.NamedTemporaryFile(
            "w") as quota_file, tempfile.NamedTemporaryFile(
                "w") as period_file, tempfile.NamedTemporaryFile(
                    "w") as cpuset_file:
        quota_file.write("-1")
        period_file.write("100000")
        cpuset_file.write("0-10,20,50-63")
        quota_file.flush()
        period_file.flush()
        cpuset_file.flush()
        assert ray.utils._get_docker_cpus(
            cpu_quota_file_name=quota_file.name,
            cpu_share_file_name=period_file.name,
            cpuset_file_name=cpuset_file.name) == 26

    # Quota set
    with tempfile.NamedTemporaryFile(
            "w") as quota_file, tempfile.NamedTemporaryFile(
                "w") as period_file, tempfile.NamedTemporaryFile(
                    "w") as cpuset_file:
        quota_file.write("42")
        period_file.write("100")
        cpuset_file.write("0-63")
        quota_file.flush()
        period_file.flush()
        cpuset_file.flush()
        assert ray.utils._get_docker_cpus(
            cpu_quota_file_name=quota_file.name,
            cpu_share_file_name=period_file.name,
            cpuset_file_name=cpuset_file.name) == 0.42


def test_override_environment_variables_task(ray_start_regular):
    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    assert (ray.get(
        get_env.options(override_environment_variables={
            "a": "b",
        }).remote("a")) == "b")


def test_override_environment_variables_actor(ray_start_regular):
    @ray.remote
    class EnvGetter:
        def get(self, key):
            return os.environ.get(key)

    a = EnvGetter.options(override_environment_variables={
        "a": "b",
        "c": "d",
    }).remote()
    assert (ray.get(a.get.remote("a")) == "b")
    assert (ray.get(a.get.remote("c")) == "d")


def test_override_environment_variables_nested_task(ray_start_regular):
    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    @ray.remote
    def get_env_wrapper(key):
        return ray.get(get_env.remote(key))

    assert (ray.get(
        get_env_wrapper.options(override_environment_variables={
            "a": "b",
        }).remote("a")) == "b")


def test_override_environment_variables_multitenancy(shutdown_only):
    ray.init(
        job_config=ray.job_config.JobConfig(worker_env={
            "foo1": "bar1",
            "foo2": "bar2",
        }))

    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    assert ray.get(get_env.remote("foo1")) == "bar1"
    assert ray.get(get_env.remote("foo2")) == "bar2"
    assert ray.get(
        get_env.options(override_environment_variables={
            "foo1": "baz1",
        }).remote("foo1")) == "baz1"
    assert ray.get(
        get_env.options(override_environment_variables={
            "foo1": "baz1",
        }).remote("foo2")) == "bar2"


def test_override_environment_variables_complex(shutdown_only):
    ray.init(
        job_config=ray.job_config.JobConfig(worker_env={
            "a": "job_a",
            "b": "job_b",
            "z": "job_z",
        }))

    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    @ray.remote
    class NestedEnvGetter:
        def get(self, key):
            return os.environ.get(key)

        def get_task(self, key):
            return ray.get(get_env.remote(key))

    @ray.remote
    class EnvGetter:
        def get(self, key):
            return os.environ.get(key)

        def get_task(self, key):
            return ray.get(get_env.remote(key))

        def nested_get(self, key):
            aa = NestedEnvGetter.options(override_environment_variables={
                "c": "e",
                "d": "dd",
            }).remote()
            return ray.get(aa.get.remote(key))

    a = EnvGetter.options(override_environment_variables={
        "a": "b",
        "c": "d",
    }).remote()
    assert (ray.get(a.get.remote("a")) == "b")
    assert (ray.get(a.get_task.remote("a")) == "b")
    assert (ray.get(a.nested_get.remote("a")) == "b")
    assert (ray.get(a.nested_get.remote("c")) == "e")
    assert (ray.get(a.nested_get.remote("d")) == "dd")
    assert (ray.get(
        get_env.options(override_environment_variables={
            "a": "b",
        }).remote("a")) == "b")

    assert (ray.get(a.get.remote("z")) == "job_z")
    assert (ray.get(a.get_task.remote("z")) == "job_z")
    assert (ray.get(a.nested_get.remote("z")) == "job_z")
    assert (ray.get(
        get_env.options(override_environment_variables={
            "a": "b",
        }).remote("z")) == "job_z")


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
