import pytest
import ray
from ray.experimental import workflow

@workflow.step
def identity(x):
    return x


@workflow.step
def gather(*args):
    return args


@pytest.mark.skip(reason="TODO (Alex): After removing the special casing for"
    "objectrefs in `WorkflowInputs` we can enable this stronger test.")
def test_dedupe_serialization(workflow_start_regular_shared):
    @ray.remote(num_cpus = 0)
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

    single = identity.step((ref,))
    double = identity.step(list_of_refs)

    gather.step(single, double).run()

    import time; time.sleep(1)
    # One more for hashing the ref, and for uploading.
    assert ray.get(counter.get_count.remote()) == 3


def test_dedupe_serialization_2(workflow_start_regular_shared):
    from ray.experimental.workflow import serialization
    manager = serialization.get_or_create_manager()
    ref = ray.put("hello world 12345")
    list_of_refs = [ref for _ in range(20)]

    assert ray.get(manager.get_num_uploads.remote()) == 0

    single = identity.step((ref,))
    double = identity.step(list_of_refs)

    result_ref, result_list = gather.step(single, double).run()
    print(result_ref, result_list)

    for result in result_list:
        assert ray.get(*result_ref) == ray.get(result)

    # The object ref will be different before and after recovery, so it will
    # get uploaded twice.
    assert ray.get(manager.get_num_uploads.remote()) == 2
