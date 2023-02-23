import numpy as np
import time
import ray
import ray.autoscaler.sdk
from ray._private.test_utils import Semaphore

import json
import os
from time import perf_counter
from tqdm import trange, tqdm

MAX_ARGS = 10000
MAX_RETURNS = 3000
MAX_RAY_GET_ARGS = 10000
MAX_QUEUED_TASKS = 1_000_000
MAX_RAY_GET_SIZE = 100 * 2**30


def assert_no_leaks():
    total = ray.cluster_resources()
    current = ray.available_resources()
    total.pop("memory")
    total.pop("object_store_memory")
    current.pop("memory")
    current.pop("object_store_memory")
    assert total == current, (total, current)


def test_many_args():
    @ray.remote
    def sum_args(*args):
        return sum(sum(arg) for arg in args)

    args = [[1 for _ in range(10000)] for _ in range(MAX_ARGS)]
    result = ray.get(sum_args.remote(*args))
    assert result == MAX_ARGS * 10000


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
    print("Generating object")
    obj = np.zeros(MAX_RAY_GET_SIZE, dtype=np.int8)
    print("Putting object")
    ref = ray.put(obj)
    del obj
    print("Getting object")
    big_obj = ray.get(ref)

    assert big_obj[0] == 0
    assert big_obj[-1] == 0


ray.init(address="auto")

args_start = perf_counter()
test_many_args()
args_end = perf_counter()

time.sleep(5)
assert_no_leaks()
print("Finished many args")

returns_start = perf_counter()
test_many_returns()
returns_end = perf_counter()

time.sleep(5)
assert_no_leaks()
print("Finished many returns")

get_start = perf_counter()
test_ray_get_args()
get_end = perf_counter()

time.sleep(5)
assert_no_leaks()
print("Finished ray.get on many objects")

queued_start = perf_counter()
test_many_queued_tasks()
queued_end = perf_counter()

time.sleep(5)
assert_no_leaks()
print("Finished queueing many tasks")

large_object_start = perf_counter()
test_large_object()
large_object_end = perf_counter()

time.sleep(5)
assert_no_leaks()
print("Done")

args_time = args_end - args_start
returns_time = returns_end - returns_start
get_time = get_end - get_start
queued_time = queued_end - queued_start
large_object_time = large_object_end - large_object_start

print(f"Many args time: {args_time} ({MAX_ARGS} args)")
print(f"Many returns time: {returns_time} ({MAX_RETURNS} returns)")
print(f"Ray.get time: {get_time} ({MAX_RAY_GET_ARGS} args)")
print(f"Queued task time: {queued_time} ({MAX_QUEUED_TASKS} tasks)")
print(f"Ray.get large object time: {large_object_time} " f"({MAX_RAY_GET_SIZE} bytes)")

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "args_time": args_time,
        "num_args": MAX_ARGS,
        "returns_time": returns_time,
        "num_returns": MAX_RETURNS,
        "get_time": get_time,
        "num_get_args": MAX_RAY_GET_ARGS,
        "queued_time": queued_time,
        "num_queued": MAX_QUEUED_TASKS,
        "large_object_time": large_object_time,
        "large_object_size": MAX_RAY_GET_SIZE,
        "success": "1",
    }
    results["perf_metrics"] = [
        {
            "perf_metric_name": f"{MAX_ARGS}_args_time",
            "perf_metric_value": args_time,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"{MAX_RETURNS}_returns_time",
            "perf_metric_value": returns_time,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"{MAX_RAY_GET_ARGS}_get_time",
            "perf_metric_value": get_time,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"{MAX_QUEUED_TASKS}_queued_time",
            "perf_metric_value": queued_time,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": f"{MAX_RAY_GET_SIZE}_large_object_time",
            "perf_metric_value": large_object_time,
            "perf_metric_type": "LATENCY",
        },
    ]
    json.dump(results, out_file)
