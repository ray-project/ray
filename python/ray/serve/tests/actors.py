import numpy as np
import pytest

import ray
from ray.serve import SingleQuery
from ray.serve.examples.adder import VectorizedAdder
from ray.serve.examples.counter import Counter, CustomCounter
from ray.serve.object_id import get_new_oid

INCREMENT = 3


@pytest.fixture(scope="module")
def init_ray():
    ray.init()
    yield
    ray.shutdown()


@pytest.fixture
def generated_inputs():
    deadline = 11111.11
    inputs = []
    input_arr = np.arange(10)
    for i in input_arr:
        oid = get_new_oid()
        inputs.append(SingleQuery(data=i, result_oid=oid, deadline_s=deadline))
    return inputs


def test_vadd(init_ray, generated_inputs):
    adder = VectorizedAdder.remote(INCREMENT)
    inputs = generated_inputs
    oids = [inp.result_oid for inp in inputs]
    input_data = [inp.data for inp in inputs]

    adder._dispatch.remote(inputs)
    result_arr = np.array(ray.get(oids))
    assert np.array_equal(result_arr, np.array(input_data) + INCREMENT)


def test_single_input(init_ray, generated_inputs):
    counter = Counter.remote()
    counter._dispatch.remote(generated_inputs)
    oids = [inp.result_oid for inp in generated_inputs]
    returned_query_ids = np.array(ray.get(oids))
    assert np.array_equal(returned_query_ids, np.arange(1, 11))


def test_custom_method(init_ray, generated_inputs):
    dummy = CustomCounter.remote()
    dummy._dispatch.remote(generated_inputs)
    oids = [inp.result_oid for inp in generated_inputs]
    returned_query_ids = np.array(ray.get(oids))
    assert np.array_equal(returned_query_ids, np.ones(10))


if __name__ == "__main__":
    ray.init()
    dummy = CustomCounter.remote()
    inputs = generated_inputs()
    dummy._dispatch.remote(inputs)
    oids = [inp.result_oid for inp in inputs]
    returned_query_ids = np.array(ray.get(oids))
    assert np.array_equal(returned_query_ids, np.ones(10))
    ray.shutdown()
