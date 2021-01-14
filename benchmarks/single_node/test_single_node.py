import numpy as np
import ray
import ray.autoscaler.sdk
from ray.test_utils import Semaphore

from time import sleep
from tqdm import trange, tqdm


MAX_RETURNS = 1000
MAX_RAY_GET_ARGS = 10000
MAX_QUEUED_TASKS = 1_000_000
MAX_RAY_GET_SIZE = 100 * 2**30

def test_many_returns():
    @ray.remote(num_returns=MAX_RETURNS)
    def f():
        to_return = []
        for _ in range(MAX_RETURNS):
            obj = list(range(10000))
            to_return.append(obj)

        return tuple(to_return)

    returned_refs = f.remote()
    assert len(returned_refs) == MAX_RETURNS

    for ref in returned_refs:
        expected = list(range(10000))
        obj = ray.get(ref)
        assert obj == expected

def test_ray_get_args():
    def with_dese():
        print("Putting test objects:")
        refs = []
        for _ in trange(MAX_RAY_GET_ARGS):
            obj = list(range(10000))
            refs.append(ray.put(obj))

        print("Getting objects")
        results = ray.get(refs)
        assert len(results) == MAX_RAY_GET_ARGS

        print("Asserting correctness")
        for obj in tqdm(results):
            expected = list(range(10000))
            assert obj == expected

    def with_zero_copy():
        print("Putting test objects:")
        refs = []
        for _ in trange(MAX_RAY_GET_ARGS):
            obj = np.arange(10000)
            refs.append(ray.put(obj))

        print("Getting objects")
        results = ray.get(refs)
        assert len(results) == MAX_RAY_GET_ARGS

        print("Asserting correctness")
        for obj in tqdm(results):
            expected = np.arange(10000)
            assert (obj == expected).all()

    with_dese()
    print("Done with dese")
    with_zero_copy()
    print("Done with zero copy")

def test_many_queued_tasks():
    sema = Semaphore.remote(0)
    @ray.remote(num_cpus=1)
    def block():
        ray.get(sema.acquire.remote())

    @ray.remote(num_cpus=1)
    def f():
        pass

    num_cpus = int(ray.cluster_resources()["CPU"])
    blocked_tasks = []
    for _ in range(num_cpus):
        blocked_tasks.append(block.remote())

    print("Submitting many tasks")
    pending_tasks = []
    for _ in trange(MAX_QUEUED_TASKS):
        pending_tasks.append(f.remote())

    # Make sure all the tasks can actually run.
    for _ in range(num_cpus):
        sema.release.remote()

    print("Unblocking tasks")
    for ref in tqdm(pending_tasks):
        assert ray.get(ref) is None

def test_large_object():
    # int is 4 bytes
    num_elements = MAX_RAY_GET_SIZE // 4

    print("Putting object")
    ref = ray.put([2**31 for _ in range(num_elements)])
    print("Getting object")
    big_obj = ray.get(ref)

    print("Verifying object")
    for val in tqdm(big_obj):
        assert val == 2**31


ray.init(address="auto")

# test_many_returns()
# print("Finished many returns")
# test_ray_get_args()
# print("Finished ray.get on many objects")
# test_many_queued_tasks()
# print("Finished queueing many tasks")
test_large_object()
