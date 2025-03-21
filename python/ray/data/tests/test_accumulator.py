import pytest
import ray
import time
from typing import List, Dict
import sys

from ray.data.accumulator import BaseAccumulator, Accumulator


class SumAccumulator(BaseAccumulator[int]):
    """Simple accumulator that maintains a running sum."""

    def __init__(self):
        self.value = 0

    def update(self, x: int) -> None:
        self.value += x

    def get(self) -> int:
        return self.value

    def reset(self) -> None:
        self.value = 0


class ListAccumulator(BaseAccumulator[List]):
    """Accumulator that collects items in a list."""

    def __init__(self):
        self.items = []

    def update(self, item) -> None:
        self.items.append(item)

    def get(self) -> List:
        return self.items

    def reset(self) -> None:
        self.items = []


class DictAccumulator(BaseAccumulator[Dict]):
    """Accumulator that maintains counts in a dictionary."""

    def __init__(self):
        self.counts = {}

    def update(self, key) -> None:
        if key in self.counts:
            self.counts[key] += 1
        else:
            self.counts[key] = 1

    def get(self) -> Dict:
        return self.counts

    def reset(self) -> None:
        self.counts = {}


@pytest.fixture
def ray_start_4_cpus():
    """Fixture that starts Ray with 4 CPUs."""
    # Make sure ray is not already initialized
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=4)
    yield
    # Make sure ray is shut down properly after the test
    if ray.is_initialized():
        ray.shutdown()


def test_sum_accumulator_basics():
    """Test basic operations of the SumAccumulator."""
    acc = SumAccumulator()
    assert acc.get() == 0

    acc.update(5)
    assert acc.get() == 5

    acc.update(10)
    assert acc.get() == 15

    acc.reset()
    assert acc.get() == 0


def test_list_accumulator_basics():
    """Test basic operations of the ListAccumulator."""
    acc = ListAccumulator()
    assert acc.get() == []

    acc.update("a")
    assert acc.get() == ["a"]

    acc.update("b")
    assert acc.get() == ["a", "b"]

    acc.reset()
    assert acc.get() == []


def test_dict_accumulator_basics():
    """Test basic operations of the DictAccumulator."""
    acc = DictAccumulator()
    assert acc.get() == {}

    acc.update("a")
    assert acc.get() == {"a": 1}

    acc.update("a")
    acc.update("b")
    assert acc.get() == {"a": 2, "b": 1}

    acc.reset()
    assert acc.get() == {}


# For the Ray actor tests, we need to ensure the accumulator classes
# are available to the actors. We'll define them inside the test functions
# to ensure proper serialization.
def test_accumulator_wrapper(ray_start_4_cpus):
    """Test the Accumulator wrapper with SumAccumulator."""

    # Define a simple class that can be properly serialized by Ray
    class TestSumAccumulator(BaseAccumulator[int]):
        def __init__(self):
            self.value = 0

        def update(self, x: int) -> None:
            self.value += x

        def get(self) -> int:
            return self.value

        def reset(self) -> None:
            self.value = 0

    acc = Accumulator(TestSumAccumulator)

    # Test initial state
    assert acc.get() == 0

    # Test update and get
    acc.update(5)
    acc.update(7)
    assert acc.get() == 12

    # Test reset
    acc.reset()
    assert acc.get() == 0


def test_list_accumulator_wrapper(ray_start_4_cpus):
    """Test the Accumulator wrapper with ListAccumulator."""

    # Define a class that can be properly serialized by Ray
    class TestListAccumulator(BaseAccumulator[List]):
        def __init__(self):
            self.items = []

        def update(self, item) -> None:
            self.items.append(item)

        def get(self) -> List:
            return self.items

        def reset(self) -> None:
            self.items = []

    acc = Accumulator(TestListAccumulator)

    # Test initial state
    assert acc.get() == []

    # Test update and get
    acc.update("item1")
    acc.update("item2")
    assert acc.get() == ["item1", "item2"]

    # Test reset
    acc.reset()
    assert acc.get() == []


def test_dict_accumulator_wrapper(ray_start_4_cpus):
    """Test the Accumulator wrapper with DictAccumulator."""

    # Define a class that can be properly serialized by Ray
    class TestDictAccumulator(BaseAccumulator[Dict]):
        def __init__(self):
            self.counts = {}

        def update(self, key) -> None:
            if key in self.counts:
                self.counts[key] += 1
            else:
                self.counts[key] = 1

        def get(self) -> Dict:
            return self.counts

        def reset(self) -> None:
            self.counts = {}

    acc = Accumulator(TestDictAccumulator)

    # Test initial state
    assert acc.get() == {}

    # Test update and get
    acc.update("key1")
    acc.update("key1")
    acc.update("key2")
    assert acc.get() == {"key1": 2, "key2": 1}

    # Test reset
    acc.reset()
    assert acc.get() == {}


def test_parallel_updates(ray_start_4_cpus):
    """Test parallel updates to a SumAccumulator through multiple tasks."""

    # Define the accumulator inside the test function for proper serialization
    class TestSumAccumulator(BaseAccumulator[int]):
        def __init__(self):
            self.value = 0

        def update(self, x: int) -> None:
            self.value += x

        def get(self) -> int:
            return self.value

        def reset(self) -> None:
            self.value = 0

    acc = Accumulator(TestSumAccumulator)

    @ray.remote
    def increment(accumulator, value):
        accumulator.update(value)
        return True

    # Launch parallel tasks that update the accumulator
    num_tasks = 10
    tasks = [increment.remote(acc, i) for i in range(num_tasks)]
    ray.get(tasks)  # Wait for all tasks to complete

    # Check final value (sum of 0 to num_tasks-1)
    expected_sum = sum(range(num_tasks))
    assert acc.get() == expected_sum


def test_concurrent_operations(ray_start_4_cpus):
    """Test concurrent operations on an accumulator."""

    # Define the accumulator inside the test function for proper serialization
    class TestListAccumulator(BaseAccumulator[List]):
        def __init__(self):
            self.items = []

        def update(self, item) -> None:
            self.items.append(item)

        def get(self) -> List:
            return self.items

        def reset(self) -> None:
            self.items = []

    acc = Accumulator(TestListAccumulator)

    @ray.remote
    def update_and_get(accumulator, item):
        accumulator.update(item)
        time.sleep(0.1)  # Simulate work
        return accumulator.get()

    # Launch tasks that update and then immediately get
    results = ray.get([update_and_get.remote(acc, f"item-{i}") for i in range(5)])

    # Each result should have at least the item that task added
    for i, result in enumerate(results):
        assert f"item-{i}" in result

    # The final state should have all items (order might vary)
    final = acc.get()
    assert sorted(final) == sorted([f"item-{i}" for i in range(5)])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
