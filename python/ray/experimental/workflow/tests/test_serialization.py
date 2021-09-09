from filelock import FileLock
import pytest
import ray
from ray.experimental import workflow
from ray.experimental.workflow import serialization
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.tests.conftest import *  # noqa
import time


@workflow.step
def identity(x):
    return x


@workflow.step
def gather(*args):
    return args

def get_num_uploads():
    manager = serialization.get_or_create_manager()
    stats = ray.get(manager.export_stats.remote())
    return stats.get("num_uploads", 0)


@pytest.mark.skip(
    reason="TODO (Alex): After removing the special casing for"
    "objectrefs in `WorkflowInputs` we can enable this stronger test.")
def test_dedupe_serialization(workflow_start_regular_shared):
    @ray.remote(num_cpus=0)
    class Counter:
        def __init__(self):
            self.count = 0

        def incr(self):
            self.count += 1

        def get_count(self):
            return self.count

    counter = Counter.remote()

    class CustomClass:
        def __getstate__(self):
            # Count the number of times this class is serialized.
            ray.get(counter.incr.remote())
            return {}

    ref = ray.put(CustomClass())
    list_of_refs = [ref for _ in range(2)]

    # One for the ray.put
    assert ray.get(counter.get_count.remote()) == 1

    single = identity.step((ref, ))
    double = identity.step(list_of_refs)

    gather.step(single, double).run()

    # One more for hashing the ref, and for uploading.
    assert ray.get(counter.get_count.remote()) == 3


def test_dedupe_serialization_2(workflow_start_regular_shared):
    from ray.experimental.workflow import serialization
    ref = ray.put("hello world 12345")
    list_of_refs = [ref for _ in range(20)]

    assert get_num_uploads() == 0

    single = identity.step((ref, ))
    double = identity.step(list_of_refs)

    result_ref, result_list = gather.step(single, double).run()
    print(result_ref, result_list)

    for result in result_list:
        assert ray.get(*result_ref) == ray.get(result)

    # The object ref will be different before and after recovery, so it will
    # get uploaded twice.
    assert get_num_uploads() == 2


def test_dedupe_cluster_failure(shutdown_only, tmp_path):
    """
    ======== driver 1 ===========
    1. Checkpoing the input args
        * Uploads
    2. Begin to run step
        * Crash

    ====== driver 2 ============
    1. Recover inputs
        * Creates a new object ref
    2. Finish running step
    3. Checkpoint step output
        * Should not trigger upload
    """
    lock_file = tmp_path / "lock"
    workflow_dir = tmp_path / "workflow"

    driver_script = f"""
import time
import ray
from ray.experimental import workflow
from filelock import FileLock

@workflow.step
def foo(objrefs):
    with FileLock("{str(lock_file)}"):
        return objrefs

if __name__ == "__main__":
    workflow.init("{str(workflow_dir)}")
    arg = ray.put("hello world")

    foo.step([arg, arg]).run()
    assert False
    """


    lock = FileLock(lock_file)
    lock.acquire()
    # print(driver_script)
    # input()

    proc = run_string_as_driver_nonblocking(driver_script)

    time.sleep(10)

    subprocess.check_call(["ray", "stop", "--force"])
    import os
    import signal
    os.kill(proc.pid, signal.SIGTERM)

    print("=========stdout===========")
    print(proc.stdout.read())
    print("=========stderr===========")
    print(proc.stderr.read())
    print("==========================")

    lock.release()
    workflow.init(str(workflow_dir))
    resumed = workflow.resume_all()
    assert len(resumed) == 1
    objref = resumed.pop()[1]
    result = ray.get(objref)

    # The object ref will be different before and after recovery, so it will
    # get uploaded twice.
    assert get_num_uploads() == 1


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
