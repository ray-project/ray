import subprocess
import time

import pytest
from filelock import FileLock

import ray
from ray import workflow
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.tests.conftest import *  # noqa
from ray.workflow import serialization, workflow_storage


def get_num_uploads():
    manager = serialization.get_or_create_manager()
    stats = ray.get(manager.export_stats.remote())
    return stats.get("num_uploads", 0)


def test_dedupe_cluster_failure(shutdown_only, tmp_path):
    """
    ======== driver 1 ===========
    1. Checkpoing the input args
        * Uploads
    2. Begin to run task
        * Crash

    ====== driver 2 ============
    1. Recover inputs
        * Creates a new object ref
    2. Finish running task
    3. Checkpoint task output
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
    ray.init(storage="{str(workflow_dir)}")
    workflow.init()
    arg = ray.put("hello world")

    workflow.run(foo.bind([arg, arg]))
    assert False
    """

    with FileLock(lock_file):
        run_string_as_driver_nonblocking(driver_script)
        time.sleep(10)
        subprocess.check_call(["ray", "stop", "--force"])

    ray.init(storage=str(workflow_dir))
    workflow.init()
    resumed = workflow.resume_all()
    assert len(resumed) == 1
    objref = resumed.pop()[1]
    ray.get(objref)

    # The object ref will be different before and after recovery, so it will
    # get uploaded twice.
    assert get_num_uploads() == 1
    ray.shutdown()


def test_embedded_objectrefs(workflow_start_regular):
    from ray.workflow.tests.utils import skip_client_mode_test

    # This test uses low-level storage APIs and restarts the cluster,
    # so it is not for client mode test
    skip_client_mode_test()

    workflow_id = test_embedded_objectrefs.__name__

    class ObjectRefsWrapper:
        def __init__(self, refs):
            self.refs = refs

    from ray._private.storage import _storage_uri

    wrapped = ObjectRefsWrapper([ray.put(1), ray.put(2)])

    store = workflow_storage.WorkflowStorage(workflow_id)
    serialization.dump_to_storage("key", wrapped, workflow_id, store)

    # Be extremely explicit about shutting down. We want to make sure the
    # `_get` call deserializes the full object and puts it in the object store.
    # Shutting down the cluster should guarantee we don't accidently get the
    # old object and pass the test.
    ray.shutdown()
    subprocess.check_output("ray stop --force", shell=True)

    ray.init(storage=_storage_uri)
    workflow.init()
    storage2 = workflow_storage.WorkflowStorage(workflow_id)

    result = storage2._get("key")
    assert ray.get(result.refs) == [1, 2]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
