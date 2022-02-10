import subprocess
import tempfile
import time

from ray.tests.conftest import *  # noqa
import pytest
from filelock import FileLock
import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.exceptions import RaySystemError
from ray import workflow
from ray.workflow import workflow_storage
from ray.workflow.storage import get_global_storage
from ray.workflow.storage.debug import DebugStorage
from ray.workflow.tests import utils


@workflow.step
def the_failed_step(x):
    if not utils.check_global_mark():
        import os

        os.kill(os.getpid(), 9)
    return "foo(" + x + ")"


@workflow.step
def source1():
    return "[source1]"


@workflow.step
def append1(x):
    return x + "[append1]"


@workflow.step
def append2(x):
    return x + "[append2]"


@workflow.step
def join(x, y):
    return f"join({x}, {y})"


@workflow.step
def complex(x1):
    x2 = source1.step()
    v = join.step(x1, x2)
    y = append1.step(x1)
    y = the_failed_step.step(y)
    z = append2.step(x2)
    u = join.step(y, z)
    return join.step(u, v)


@workflow.step
def simple(x):
    x = append1.step(x)
    y = the_failed_step.step(x)
    z = append2.step(y)
    return z


@workflow.step
def identity(x):
    return x


@workflow.step
def recursive(ref, count):
    if count == 0:
        return ref
    return recursive.step(ref, count - 1)


@workflow.step
def gather(*args):
    return args


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
        debug_store = DebugStorage(get_global_storage(), temp_dir)
        utils._alter_storage(debug_store)

        numbers = [ray.put(i) for i in range(5)]
        workflows = [identity.step(numbers) for _ in range(100)]

        gather.step(*workflows).run()

        ops = debug_store._logged_storage.get_op_counter()
        get_objects_count = 0
        for key in ops["get"]:
            if "objects" in key:
                get_objects_count += 1
        assert get_objects_count == 5


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
        debug_store = DebugStorage(get_global_storage(), temp_dir)
        utils._alter_storage(debug_store)

        ref = ray.put("hello")
        workflows = [identity.step(ref) for _ in range(100)]

        gather.step(*workflows).run()

        ops = debug_store._logged_storage.get_op_counter()
        get_objects_count = 0
        for key in ops["get"]:
            if "objects" in key:
                get_objects_count += 1
        assert get_objects_count == 1


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
    step, we should checkpoint the input/output, but continue to reuse the
    in-memory value.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        debug_store = DebugStorage(get_global_storage(), temp_dir)
        utils._alter_storage(debug_store)

        ref = ray.put("hello")
        result = recursive.step([ref], 10).run()

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


def test_recovery_simple(workflow_start_regular):
    utils.unset_global_mark()
    workflow_id = "test_recovery_simple"
    with pytest.raises(RaySystemError):
        # internally we get WorkerCrashedError
        simple.step("x").run(workflow_id=workflow_id)

    assert workflow.get_status(workflow_id) == workflow.WorkflowStatus.RESUMABLE

    utils.set_global_mark()
    output = workflow.resume(workflow_id)
    assert ray.get(output) == "foo(x[append1])[append2]"
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    output = workflow.resume(workflow_id)
    assert ray.get(output) == "foo(x[append1])[append2]"


def test_recovery_complex(workflow_start_regular):
    utils.unset_global_mark()
    workflow_id = "test_recovery_complex"
    with pytest.raises(RaySystemError):
        # internally we get WorkerCrashedError
        complex.step("x").run(workflow_id=workflow_id)
    utils.set_global_mark()
    output = workflow.resume(workflow_id)
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert ray.get(output) == r
    utils.unset_global_mark()
    # resume from workflow output checkpoint
    output = workflow.resume(workflow_id)
    r = "join(join(foo(x[append1]), [source1][append2]), join(x, [source1]))"
    assert ray.get(output) == r


def test_recovery_non_exists_workflow(workflow_start_regular):
    with pytest.raises(ValueError):
        ray.get(workflow.resume("this_workflow_id_does_not_exist"))


driver_script = """
import time
from ray import workflow


@workflow.step
def foo(x):
    print("Executing", x)
    time.sleep(1)
    if x < 20:
        return foo.step(x + 1)
    else:
        return 20


if __name__ == "__main__":
    workflow.init("{tmp_path}")
    assert foo.step(0).run(workflow_id="cluster_failure") == 20
"""


def test_recovery_cluster_failure(reset_workflow, tmp_path):
    subprocess.check_call(["ray", "start", "--head"])
    time.sleep(1)
    proc = run_string_as_driver_nonblocking(
        driver_script.format(tmp_path=str(tmp_path))
    )
    time.sleep(10)
    subprocess.check_call(["ray", "stop"])
    proc.kill()
    time.sleep(1)
    workflow.init(str(tmp_path))
    assert ray.get(workflow.resume("cluster_failure")) == 20
    workflow.storage.set_global_storage(None)
    ray.shutdown()


def test_recovery_cluster_failure_resume_all(reset_workflow, tmp_path):
    tmp_path = tmp_path
    subprocess.check_call(["ray", "start", "--head"])
    time.sleep(1)
    workflow_dir = tmp_path / "workflow"
    lock_file = tmp_path / "lock_file"
    driver_script = f"""
import time
from ray import workflow
from filelock import FileLock
@workflow.step
def foo(x):
    with FileLock("{str(lock_file)}"):
        return 20

if __name__ == "__main__":
    workflow.init("{str(workflow_dir)}")
    assert foo.step(0).run(workflow_id="cluster_failure") == 20
"""
    lock = FileLock(lock_file)
    lock.acquire()

    proc = run_string_as_driver_nonblocking(driver_script)
    time.sleep(10)
    subprocess.check_call(["ray", "stop"])
    proc.kill()
    time.sleep(1)
    lock.release()
    workflow.init(str(workflow_dir))
    resumed = workflow.resume_all()
    assert len(resumed) == 1
    (wid, obj_ref) = resumed[0]
    assert wid == "cluster_failure"
    assert ray.get(obj_ref) == 20
    workflow.storage.set_global_storage(None)
    ray.shutdown()


@workflow.step
def recursive_chain(x):
    if x < 100:
        return recursive_chain.step(x + 1)
    else:
        return 100


def test_shortcut(workflow_start_regular):
    assert recursive_chain.step(0).run(workflow_id="shortcut") == 100
    # the shortcut points to the step with output checkpoint
    store = workflow_storage.get_workflow_storage("shortcut")
    step_id = store.get_entrypoint_step_id()
    output_step_id = store.inspect_step(step_id).output_step_id
    assert store.inspect_step(output_step_id).output_object_valid


@workflow.step
def constant():
    return 31416


def test_resume_different_storage(ray_start_regular, tmp_path, reset_workflow):
    workflow.init(storage=str(tmp_path))
    constant.step().run(workflow_id="const")
    assert ray.get(workflow.resume(workflow_id="const")) == 31416
    workflow.storage.set_global_storage(None)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
