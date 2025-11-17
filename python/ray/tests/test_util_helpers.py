import sys

import pytest

import ray
from ray._common.test_utils import SignalActor
from ray.util import as_completed, map_unordered


@pytest.fixture(scope="module")
def ray_init_4_cpu_shared():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.mark.parametrize("yield_obj_refs", [True, False])
def test_as_completed_chunk_size_1(ray_init_4_cpu_shared, yield_obj_refs):
    """Test as_completed with chunk_size=1.

    Use SignalActor to control task completion order and mimic time.sleep(x) behavior.

    """
    inputs = [10, 8, 6, 4, 2]

    # Create signals for each task
    signals = [SignalActor.remote() for _ in range(len(inputs))]

    # Create tasks
    @ray.remote
    def f(x, signal):
        ray.get(signal.wait.remote())
        return x

    # Submit tasks with their corresponding signals in the original order
    refs = [f.remote(x, signal) for x, signal in zip(inputs, signals)]

    # Use as_completed() lazily
    it = as_completed(refs, chunk_size=1, yield_obj_refs=yield_obj_refs)

    # Send signal in reverse order to mimic time.sleep(x), i.e.,
    # smallest value releases first. At the same time, collect results

    results = []
    for signal in reversed(signals):
        ray.get(signal.send.remote())
        results.append(next(it))

    if yield_obj_refs:
        results = ray.get(results)

    assert results == [2, 4, 6, 8, 10]


@pytest.mark.parametrize("yield_obj_refs", [True, False])
def test_as_completed_chunk_size_2(ray_init_4_cpu_shared, yield_obj_refs):
    """Test as_completed with chunk_size=2.

    Use SignalActor to control task completion order and mimic time.sleep(x) behavior.

    """
    inputs = [10, 8, 6, 4, 2]

    # Create signals for each task
    signals = [SignalActor.remote() for _ in range(len(inputs))]

    # Create tasks
    @ray.remote
    def f(x, signal):
        ray.get(signal.wait.remote())
        return x

    # Submit tasks with their corresponding signals in the original order
    refs = [f.remote(x, signal) for x, signal in zip(inputs, signals)]

    # Use as_completed() lazily
    it = as_completed(refs, chunk_size=2, yield_obj_refs=yield_obj_refs)

    # Send signal in reverse order to mimic time.sleep(x), i.e.,
    # smallest value releases first. At the same time, collect results

    results = []

    ray.get(signals[4].send.remote())
    ray.get(signals[3].send.remote())
    results.append(next(it))
    results.append(next(it))

    ray.get(signals[2].send.remote())
    ray.get(signals[1].send.remote())
    results.append(next(it))
    results.append(next(it))

    ray.get(signals[0].send.remote())
    results.append(next(it))

    if yield_obj_refs:
        results = ray.get(results)

    assert results == [4, 2, 8, 6, 10]


@pytest.mark.parametrize("yield_obj_refs", [True, False])
def test_map_unordered_chunk_size_1(ray_init_4_cpu_shared, yield_obj_refs):
    """Test map_unordered with chunk_size=1.

    Use SignalActor to control task completion order and mimic time.sleep(x) behavior.

    """
    inputs = [10, 8, 6, 4, 2]

    # Create signals for each task
    signals = [SignalActor.remote() for _ in range(len(inputs))]

    # Create tasks
    @ray.remote
    def f(args):
        x, signal = args
        ray.get(signal.wait.remote())
        return x

    # Submit tasks with their corresponding signals in the original order
    it = map_unordered(
        f, zip(inputs, signals), chunk_size=1, yield_obj_refs=yield_obj_refs
    )

    # Send signal in reverse order to mimic time.sleep(x), i.e.,
    # smallest value releases first. At the same time, collect results

    results = []
    for signal in reversed(signals):
        ray.get(signal.send.remote())
        results.append(next(it))

    if yield_obj_refs:
        results = ray.get(results)

    assert results == [2, 4, 6, 8, 10]


@pytest.mark.parametrize("yield_obj_refs", [True, False])
def test_map_unordered_chunk_size_2(ray_init_4_cpu_shared, yield_obj_refs):
    """Test map_unordered with chunk_size=2.

    Use SignalActor to control task completion order and mimic time.sleep(x) behavior.

    """
    inputs = [10, 8, 6, 4, 2]

    # Create signals for each task
    signals = [SignalActor.remote() for _ in range(len(inputs))]

    # Create tasks
    @ray.remote
    def f(args):
        x, signal = args
        ray.get(signal.wait.remote())
        return x

    # Submit tasks with their corresponding signals in the original order
    it = map_unordered(
        f, zip(inputs, signals), chunk_size=2, yield_obj_refs=yield_obj_refs
    )

    # Send signal in reverse order to mimic time.sleep(x), i.e.,
    # smallest value releases first. At the same time, collect results

    results = []

    ray.get(signals[4].send.remote())
    ray.get(signals[3].send.remote())
    results.append(next(it))
    results.append(next(it))

    ray.get(signals[2].send.remote())
    ray.get(signals[1].send.remote())
    results.append(next(it))
    results.append(next(it))

    ray.get(signals[0].send.remote())
    results.append(next(it))

    if yield_obj_refs:
        results = ray.get(results)

    assert results == [4, 2, 8, 6, 10]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
