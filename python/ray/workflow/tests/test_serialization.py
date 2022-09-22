import pytest

import ray
from ray import workflow
from ray.tests.conftest import *  # noqa
from ray.workflow import serialization


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

    workflow.run(gather.bind(single, double))

    # One more for hashing the ref, and for uploading.
    assert ray.get(counter.get_count.remote()) == 3


def test_dedupe_serialization_2(workflow_start_regular_shared):
    from ray.workflow.tests.utils import skip_client_mode_test

    # TODO(suquark): Fix workflow with ObjectRefs as inputs under client mode.
    skip_client_mode_test()

    ref = ray.put("hello world 12345")
    list_of_refs = [ref for _ in range(20)]

    assert get_num_uploads() == 0

    single = identity.bind((ref,))
    double = identity.bind(list_of_refs)

    result_ref, result_list = workflow.run(gather.bind(single, double))

    for result in result_list:
        assert ray.get(*result_ref) == ray.get(result)

    # One upload for the initial checkpoint, and one for the object ref after
    # resuming.
    assert get_num_uploads() == 1


def test_same_object_many_workflows(workflow_start_regular_shared):
    """Ensure that when we dedupe uploads, we upload the object once per workflow,
    since different workflows shouldn't look in each others object directories.
    """
    from ray.workflow.tests.utils import skip_client_mode_test

    # TODO(suquark): Fix workflow with ObjectRefs as inputs under client mode.
    skip_client_mode_test()

    @ray.remote
    def f(a):
        return [a[0]]

    x = {0: ray.put(10)}

    result1 = workflow.run(f.bind(x))
    result2 = workflow.run(f.bind(x))
    print(result1)
    print(result2)

    assert ray.get(*result1) == 10
    assert ray.get(*result2) == 10


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
