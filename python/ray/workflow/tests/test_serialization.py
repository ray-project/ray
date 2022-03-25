from filelock import FileLock
import pytest
import ray
from ray import workflow
from ray.workflow import serialization
from ray.workflow import storage
from ray.workflow import workflow_storage
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.tests.conftest import *  # noqa
import subprocess
import time


@ray.remote
def identity(x):
    return x


@ray.remote
def gather(*args):
    return args


def get_num_uploads():
    manager = serialization.get_or_create_manager()
    stats = ray.get(manager.export_stats.remote())
    return stats.get("num_uploads", 0)


@pytest.mark.skip(
    reason="TODO (Alex): After removing the special casing for"
    "objectrefs in `WorkflowInputs` we can enable this stronger test."
)
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

    single = identity.bind((ref,))
    double = identity.bind(list_of_refs)

    workflow.create(gather.bind(single, double)).run()

    # One more for hashing the ref, and for uploading.
    assert ray.get(counter.get_count.remote()) == 3


def test_dedupe_serialization_2(workflow_start_regular_shared):
    ref = ray.put("hello world 12345")
    list_of_refs = [ref for _ in range(20)]

    assert get_num_uploads() == 0

    single = identity.bind((ref,))
    double = identity.bind(list_of_refs)

    result_ref, result_list = workflow.create(gather.bind(single, double)).run()

    for result in result_list:
        assert ray.get(*result_ref) == ray.get(result)

    # One upload for the initial checkpoint, and one for the object ref after
    # resuming.
    assert get_num_uploads() == 2


def test_same_object_many_workflows(workflow_start_regular_shared):
    """Ensure that when we dedupe uploads, we upload the object once per workflow,
    since different workflows shouldn't look in each others object directories.
    """

    @ray.remote
    def f(a):
        return [a[0]]

    x = {0: ray.put(10)}

    result1 = workflow.create(f.bind(x)).run()
    result2 = workflow.create(f.bind(x)).run()
    print(result1)
    print(result2)

    assert ray.get(*result1) == 10
    assert ray.get(*result2) == 10


def test_dedupe_cluster_failure(reset_workflow, tmp_path):
    ray.shutdown()
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
from ray import workflow
from filelock import FileLock

@ray.remote
def foo(objrefs):
    with FileLock("{str(lock_file)}"):
        return objrefs

if __name__ == "__main__":
    workflow.init("{str(workflow_dir)}")
    arg = ray.put("hello world")

    workflow.create(foo.bind([arg, arg])).run()
    assert False
    """

    lock = FileLock(lock_file)
    lock.acquire()

    run_string_as_driver_nonblocking(driver_script)

    time.sleep(10)

    subprocess.check_call(["ray", "stop", "--force"])

    lock.release()
    workflow.init(str(workflow_dir))
    resumed = workflow.resume_all()
    assert len(resumed) == 1
    objref = resumed.pop()[1]
    ray.get(objref)

    # The object ref will be different before and after recovery, so it will
    # get uploaded twice.
    assert get_num_uploads() == 1
    workflow.storage.set_global_storage(None)
    ray.shutdown()


def test_embedded_objectrefs(workflow_start_regular):
    workflow_id = test_embedded_objectrefs.__name__
    base_storage = storage.get_global_storage()

    class ObjectRefsWrapper:
        def __init__(self, refs):
            self.refs = refs

    url = base_storage.storage_url

    wrapped = ObjectRefsWrapper([ray.put(1), ray.put(2)])

    promise = serialization.dump_to_storage(["key"], wrapped, workflow_id, base_storage)
    workflow_storage.asyncio_run(promise)

    # Be extremely explicit about shutting down. We want to make sure the
    # `_get` call deserializes the full object and puts it in the object store.
    # Shutting down the cluster should guarantee we don't accidently get the
    # old object and pass the test.
    ray.shutdown()
    subprocess.check_output("ray stop --force", shell=True)

    workflow.init(url)
    storage2 = workflow_storage.get_workflow_storage(workflow_id)

    result = workflow_storage.asyncio_run(storage2._get(["key"]))
    assert ray.get(result.refs) == [1, 2]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
