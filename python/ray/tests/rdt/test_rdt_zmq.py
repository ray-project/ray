import sys

import numpy
import pytest

import ray


def test_zmq_numpy_transport_basic(ray_start_regular):
    """Test basic send/receive between two actors on the same node."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="zmq_numpy")
        def echo(self, data):
            return data

        def non_rdt_echo(self, data):
            return data

        def sum(self, data):
            return data.sum().item()

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].echo.remote(numpy.array([1, 2, 3]))
    result = actors[1].sum.remote(ref)
    assert ray.get(result) == 6

    # Non-rdt methods should still work
    ref = actors[0].non_rdt_echo.remote(numpy.array([4, 5, 6]))
    result = actors[1].sum.remote(ref)
    assert ray.get(result) == 15


def test_zmq_numpy_transport_multiple_tensors(ray_start_regular):
    """Test sending a list of multiple numpy arrays."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="zmq_numpy")
        def produce(self):
            return [numpy.array([1, 2]), numpy.array([3, 4, 5]), numpy.array([6])]

        def total_sum(self, data):
            return sum(arr.sum().item() for arr in data)

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].produce.remote()
    result = actors[1].total_sum.remote(ref)
    assert ray.get(result) == 21


def test_zmq_numpy_transport_large_tensor(ray_start_regular):
    """Test sending a large array."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="zmq_numpy")
        def produce_large(self):
            return numpy.ones(1_000_000)

        def verify(self, data):
            return data.shape[0] == 1_000_000 and data.sum() == 1_000_000

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].produce_large.remote()
    result = actors[1].verify.remote(ref)
    assert ray.get(result)


def test_zmq_numpy_transport_concurrent_receivers(ray_start_regular):
    """Test multiple receivers fetching the same data."""

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="zmq_numpy")
        def produce(self):
            return numpy.array([10, 20, 30])

        def sum(self, data):
            return data.sum().item()

    producer = Actor.remote()
    consumers = [Actor.remote() for _ in range(3)]

    ref = producer.produce.remote()
    results = ray.get([c.sum.remote(ref) for c in consumers])
    assert all(r == 60 for r in results)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
