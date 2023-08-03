import subprocess
import tempfile
import time

from ray.tests.conftest import *  # noqa
import pytest
from filelock import FileLock
import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray import workflow
from ray.workflow import workflow_storage
from ray.workflow.storage.debug import DebugStorage
from ray.workflow.tests import utils
from ray.workflow.exceptions import WorkflowNotResumableError


@ray.remote
def identity(x):
    return x


@ray.remote
def gather(*args):
    return args


@pytest.mark.skip(reason="TODO (suquark): Support debug storage.")
@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4,  # increase CPUs to add pressure
        }
    ],
    indirect=True,
)
def test_dedupe_downloads_list(workflow_start_regular):
    with tempfile.TemporaryDirectory() as temp_dir:
        debug_store = DebugStorage(temp_dir)
        utils._alter_storage(debug_store)

        numbers = [ray.put(i) for i in range(5)]
        workflows = [identity.bind(numbers) for _ in range(100)]

        workflow.run(gather.bind(*workflows))

        ops = debug_store._logged_storage.get_op_counter()
        get_objects_count = 0
        for key in ops["get"]:
            if "objects" in key:
                get_objects_count += 1
        assert get_objects_count == 5


@pytest.mark.skip(reason="TODO (suquark): Support debug storage.")
@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4,  # increase CPUs to add pressure
        }
    ],
    indirect=True,
)
def test_dedupe_download_raw_ref(workflow_start_regular):
    with tempfile.TemporaryDirectory() as temp_dir:
        debug_store = DebugStorage(temp_dir)
        utils._alter_storage(debug_store)

        ref = ray.put("hello")
        workflows = [identity.bind(ref) for _ in range(100)]

        workflow.run(gather.bind(*workflows))

        ops = debug_store._logged_storage.get_op_counter()
        get_objects_count = 0
        for key in ops["get"]:
            if "objects" in key:
                get_objects_count += 1
        assert get_objects_count == 1


@pytest.mark.skip(reason="TODO (suquark): Support debug storage.")
@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4,  # increase CPUs to add pressure
        }
    ],
    indirect=True,
)
def test_nested_workflow_no_download(workflow_start_regular):
    """Test that we _only_ load from storage on recovery. For a nested workflow
    task, we should checkpoint the input/output, but continue to reuse the
    in-memory value.
    """

    @ray.remote
    def recursive(ref, count):
        if count == 0:
            return ref
        return workflow.continuation(recursive.bind(ref, count - 1))

    with tempfile.TemporaryDirectory() as temp_dir:
        debug_store = DebugStorage(temp_dir)
        utils._alter_storage(debug_store)

        ref = ray.put("hello")
        result = workflow.run(recursive.bind([ref], 10))

        ops = debug_store._logged_storage.get_op_counter()
        get_objects_count = 0
        for key in ops["get"]:
            if "objects" in key:
                get_objects_count += 1
        assert get_objects_count == 1, "We should only get once when resuming."
        put_objects_count = 0
        for key in ops["put"]:
            if "objects" in key:
                print(key)
                put_objects_count += 1
        assert (
            put_objects_count == 1
        ), "We should detect the object exists before uploading"
        assert ray.get(result) == ["hello"]


@ray.remote
def the_failed_task(x):
    if not utils.check_global_mark():
        import os

        os.kill(os.getpid(), 9)
    return "foo(" + x + ")"


def test_recovery_simple_1(workflow_start_regular):
    utils.unset_global_mark()
    workflow_id = "test_recovery_simple_1"
    with pytest.raises(workflow.WorkflowExecutionError):
        # internally we get WorkerCrashedError
        workflow.run(the_failed_task.bind("x"), workflow_id=workflow_id)

    assert workflow.get_status(workflow_id) == workflow.WorkflowStatus.FAILED

    utils.set_global_mark()
    assert workflow.resume(workflow_id) == "foo(x)"
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    assert workflow.resume(workflow_id) == "foo(x)"


def test_recovery_simple_2(workflow_start_regular):
    @ray.remote
    def simple(x):
        return workflow.continuation(the_failed_task.bind(x))

    utils.unset_global_mark()
    workflow_id = "test_recovery_simple_2"
    with pytest.raises(workflow.WorkflowExecutionError):
        # internally we get WorkerCrashedError
        workflow.run(simple.bind("x"), workflow_id=workflow_id)

    assert workflow.get_status(workflow_id) == workflow.WorkflowStatus.FAILED

    utils.set_global_mark()
    assert workflow.resume(workflow_id) == "foo(x)"
    utils.unset_global_mark()
    # resume from workflow output checkpoint

    assert workflow.resume(workflow_id) == "foo(x)"


def test_recovery_simple_3(workflow_start_regular):
    @ray.remote
    def append1(x):
        return x + "[append1]"

    @ray.remote
    def append2(x):
        return x + "[append2]"

    @ray.remote
    def simple(x):
        x = append1.bind(x)
        y = the_failed_task.bind(x)
        z = append2.bind(y)
        return workflow.continuation(z)

    utils.unset_global_mark()
    workflow_id = "test_recovery_simple_3"
    with pytest.raises(workflow.WorkflowExecutionError):
        # internally we get WorkerCrashedError
        workflow.run(simple.bind("x"), workflow_id=workflow_id)

    assert workflow.get_status(workflow_id) == workflow.WorkflowStatus.FAILED

    utils.set_global_mark()
    assert workflow.resume(workflow_id) == "foo(x[append1])[append2]"
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    assert workflow.resume(workflow_id) == "foo(x[append1])[append2]"


def test_recovery_complex(workflow_start_regular):
    @ray.remote
    def source1():
        return "[source1]"

    @ray.remote
    def append1(x):
        return x + "[append1]"

    @ray.remote
    def append2(x):
        return x + "[append2]"

    @ray.remote
    def join(x, y):
        return f"join({x}, {y})"

    @ray.remote
    def complex(x1):
        x2 = source1.bind()
        v = join.bind(x1, x2)
        y = append1.bind(x1)
        y = the_failed_task.bind(y)
        z = append2.bind(x2)
        u = join.bind(y, z)
        return workflow.continuation(join.bind(u, v))

    utils.unset_global_mark()
    workflow_id = "test_recovery_complex"
    with pytest.raises(workflow.WorkflowExecutionError):
        # internally we get WorkerCrashedError
        workflow.run(complex.bind("x"), workflow_id=workflow_id)

    assert workflow.get_status(workflow_id) == workflow.WorkflowStatus.FAILED

    utils.set_global_mark()
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert workflow.resume(workflow_id) == r
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert workflow.resume(workflow_id) == r


def test_recovery_non_exists_workflow(workflow_start_regular):
    with pytest.raises(WorkflowNotResumableError):
        workflow.resume("this_workflow_id_does_not_exist")


def test_recovery_cluster_failure(tmp_path, shutdown_only):
    ray.shutdown()
    subprocess.check_call(["ray", "start", "--head", f"--storage={tmp_path}"])
    time.sleep(1)
    proc = run_string_as_driver_nonblocking(
        """
import time
import ray
from ray import workflow

@ray.remote
def foo(x):
    print("Executing", x)
    time.sleep(1)
    if x < 20:
        return workflow.continuation(foo.bind(x + 1))
    else:
        return 20

if __name__ == "__main__":
    ray.init()
    assert workflow.run(foo.bind(0), workflow_id="cluster_failure") == 20
"""
    )
    time.sleep(10)
    subprocess.check_call(["ray", "stop"])
    proc.kill()
    time.sleep(1)
    ray.init(storage=str(tmp_path))
    workflow.init()
    assert workflow.resume("cluster_failure") == 20
    ray.shutdown()


def test_recovery_cluster_failure_resume_all(tmp_path, shutdown_only):
    ray.shutdown()

    tmp_path = tmp_path
    workflow_dir = tmp_path / "workflow"
    subprocess.check_call(["ray", "start", "--head", f"--storage={workflow_dir}"])
    time.sleep(1)
    lock_file = tmp_path / "lock_file"
    lock = FileLock(lock_file)
    lock.acquire()

    proc = run_string_as_driver_nonblocking(
        f"""
import time
import ray
from ray import workflow
from filelock import FileLock

@ray.remote
def foo(x):
    with FileLock("{str(lock_file)}"):
        return 20

if __name__ == "__main__":
    ray.init()
    assert workflow.run(foo.bind(0), workflow_id="cluster_failure") == 20
"""
    )
    time.sleep(10)
    subprocess.check_call(["ray", "stop"])
    proc.kill()
    time.sleep(1)
    lock.release()
    ray.init(storage=str(workflow_dir))
    workflow.init()
    resumed = workflow.resume_all()
    assert len(resumed) == 1
    (wid, obj_ref) = resumed[0]
    assert wid == "cluster_failure"
    assert ray.get(obj_ref) == 20


def test_shortcut(workflow_start_regular):
    @ray.remote
    def recursive_chain(x):
        if x < 100:
            return workflow.continuation(recursive_chain.bind(x + 1))
        else:
            return 100

    assert workflow.run(recursive_chain.bind(0), workflow_id="shortcut") == 100

    from ray._private.client_mode_hook import client_mode_wrap

    # the shortcut points to the task with output checkpoint
    @client_mode_wrap
    def check():
        store = workflow_storage.WorkflowStorage("shortcut")
        task_id = store.get_entrypoint_task_id()
        output_task_id = store.inspect_task(task_id).output_task_id
        return store.inspect_task(output_task_id).output_object_valid

    assert check()


def test_resume_different_storage(shutdown_only, tmp_path):
    @ray.remote
    def constant():
        return 31416

    ray.init(storage=str(tmp_path))
    workflow.init()
    workflow.run(constant.bind(), workflow_id="const")
    assert workflow.resume(workflow_id="const") == 31416


def test_no_side_effects_of_resuming(workflow_start_regular):
    with pytest.raises(Exception):
        workflow.resume("doesnt_exist")

    assert workflow.list_all() == [], "Shouldn't list the resume that didn't work"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
