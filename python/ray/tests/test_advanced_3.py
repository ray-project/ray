# coding: utf-8
import glob
import logging
import os
import sys
import socket
import tempfile
import time
from unittest import mock

import numpy as np
import pickle
import pytest

import ray
from ray.new_dashboard import k8s_utils
import ray.ray_constants as ray_constants
import ray.util.accelerators
import ray._private.utils
import ray.cluster_utils
import ray.test_utils
from ray import resource_spec
import setproctitle

from ray.test_utils import (check_call_ray, wait_for_condition,
                            wait_for_num_actors)

logger = logging.getLogger(__name__)


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

    job_id = ray._private.utils.compute_job_id_from_driver(
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
        output = ray._private.utils.decode(
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
        assert ray._private.utils._get_docker_cpus(
            cpu_quota_file_name=quota_file.name,
            cpu_period_file_name=period_file.name,
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
        assert ray._private.utils._get_docker_cpus(
            cpu_quota_file_name=quota_file.name,
            cpu_period_file_name=period_file.name,
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
        assert ray._private.utils._get_docker_cpus(
            cpu_quota_file_name=quota_file.name,
            cpu_period_file_name=period_file.name,
            cpuset_file_name=cpuset_file.name) == 0.42


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="No need to test on Windows.")
def test_k8s_cpu():
    """Test all the functions in dashboard/k8s_utils.py.
    Also test ray._private.utils.get_num_cpus when running in a  K8s pod.
    Files were obtained from within a K8s pod with 2 CPU request, CPU limit
    unset, with 1 CPU of stress applied.
    """

    # Some experimentally-obtained K8S CPU usage files for use in test_k8s_cpu.
    PROCSTAT1 = \
    """cpu  2945022 98 3329420 148744854 39522 0 118587 0 0 0
    cpu0 370299 14 413841 18589778 5304 0 15288 0 0 0
    cpu1 378637 10 414414 18589275 5283 0 14731 0 0 0
    cpu2 367328 8 420914 18590974 4844 0 14416 0 0 0
    cpu3 368378 11 423720 18572899 4948 0 14394 0 0 0
    cpu4 369051 13 414615 18607285 4736 0 14383 0 0 0
    cpu5 362958 10 415984 18576655 4590 0 16614 0 0 0
    cpu6 362536 13 414430 18605197 4785 0 14353 0 0 0
    cpu7 365833 15 411499 18612787 5028 0 14405 0 0 0
    intr 1000694027 125 0 0 39 154 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1028 0 2160913 0 2779605 8 0 3981333 3665198 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
    ctxt 1574979439
    btime 1615208601
    processes 857411
    procs_running 6
    procs_blocked 0
    softirq 524311775 0 230142964 27143 63542182 0 0 171 74042767 0 156556548
    """ # noqa

    PROCSTAT2 = \
    """cpu  2945152 98 3329436 148745483 39522 0 118587 0 0 0
    cpu0 370399 14 413841 18589778 5304 0 15288 0 0 0
    cpu1 378647 10 414415 18589362 5283 0 14731 0 0 0
    cpu2 367329 8 420916 18591067 4844 0 14416 0 0 0
    cpu3 368381 11 423724 18572989 4948 0 14395 0 0 0
    cpu4 369052 13 414618 18607374 4736 0 14383 0 0 0
    cpu5 362968 10 415986 18576741 4590 0 16614 0 0 0
    cpu6 362537 13 414432 18605290 4785 0 14353 0 0 0
    cpu7 365836 15 411502 18612878 5028 0 14405 0 0 0
    intr 1000700905 125 0 0 39 154 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1028 0 2160923 0 2779605 8 0 3981353 3665218 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
    ctxt 1574988760
    btime 1615208601
    processes 857411
    procs_running 4
    procs_blocked 0
    softirq 524317451 0 230145523 27143 63542930 0 0 171 74043232 0 156558452
    """ # noqa

    CPUACCTUSAGE1 = "2268980984108"

    CPUACCTUSAGE2 = "2270120061999"

    CPUSHARES = "2048"

    shares_file, cpu_file, proc_stat_file = [
        tempfile.NamedTemporaryFile("w+") for _ in range(3)
    ]
    shares_file.write(CPUSHARES)
    cpu_file.write(CPUACCTUSAGE1)
    proc_stat_file.write(PROCSTAT1)
    for file in shares_file, cpu_file, proc_stat_file:
        file.flush()
    with mock.patch("ray._private.utils.os.environ",
                    {"KUBERNETES_SERVICE_HOST"}),\
            mock.patch("ray.new_dashboard.k8s_utils.CPU_USAGE_PATH",
                       cpu_file.name),\
            mock.patch("ray.new_dashboard.k8s_utils.PROC_STAT_PATH",
                       proc_stat_file.name),\
            mock.patch("ray._private.utils.get_k8s_cpus.__defaults__",
                       (shares_file.name,)):

        # Test helpers
        assert ray._private.utils.get_num_cpus() == 2
        assert k8s_utils._cpu_usage() == 2268980984108
        assert k8s_utils._system_usage() == 1551775030000000
        assert k8s_utils._host_num_cpus() == 8

        # No delta for first computation, return 0.
        assert k8s_utils.cpu_percent() == 0.0

        # Write new usage info obtained after 1 sec wait.
        for file in cpu_file, proc_stat_file:
            file.truncate(0)
            file.seek(0)
        cpu_file.write(CPUACCTUSAGE2)
        proc_stat_file.write(PROCSTAT2)
        for file in cpu_file, proc_stat_file:
            file.flush()

        # Files were extracted under 1 CPU of load on a 2 CPU pod
        assert 50 < k8s_utils.cpu_percent() < 60


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


def test_sync_job_config(shutdown_only):
    num_java_workers_per_process = 8
    worker_env = {
        "key": "value",
    }

    ray.init(
        job_config=ray.job_config.JobConfig(
            num_java_workers_per_process=num_java_workers_per_process,
            worker_env=worker_env))

    # Check that the job config is synchronized at the driver side.
    job_config = ray.worker.global_worker.core_worker.get_job_config()
    assert (job_config.num_java_workers_per_process ==
            num_java_workers_per_process)
    assert (job_config.worker_env == worker_env)

    @ray.remote
    def get_job_config():
        job_config = ray.worker.global_worker.core_worker.get_job_config()
        return job_config.SerializeToString()

    # Check that the job config is synchronized at the worker side.
    job_config = ray.gcs_utils.JobConfig()
    job_config.ParseFromString(ray.get(get_job_config.remote()))
    assert (job_config.num_java_workers_per_process ==
            num_java_workers_per_process)
    assert (job_config.worker_env == worker_env)


def test_duplicated_arg(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def task_with_dup_arg(*args):
        return sum(args)

    # Basic verification.
    arr = np.ones(1 * 1024 * 1024, dtype=np.uint8)  # 1MB
    ref = ray.put(arr)
    assert np.array_equal(
        ray.get(task_with_dup_arg.remote(ref, ref, ref)), sum([arr, arr, arr]))

    # Make sure it works when it is mixed with other args.
    ref2 = ray.put(arr)
    assert np.array_equal(
        ray.get(task_with_dup_arg.remote(ref, ref2, ref)), sum([arr, arr,
                                                                arr]))

    # Test complicated scenario with multi nodes.
    cluster.add_node(num_cpus=1, resources={"worker_1": 1})
    cluster.add_node(num_cpus=1, resources={"worker_2": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def create_remote_ref(arr):
        return ray.put(arr)

    @ray.remote
    def task_with_dup_arg_ref(*args):
        args = ray.get(list(args))
        return sum(args)

    ref1 = create_remote_ref.options(resources={"worker_1": 1}).remote(arr)
    ref2 = create_remote_ref.options(resources={"worker_2": 1}).remote(arr)
    ref3 = create_remote_ref.remote(arr)
    np.array_equal(
        ray.get(
            task_with_dup_arg_ref.remote(ref1, ref2, ref3, ref1, ref2, ref3)),
        sum([arr] * 6))


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
