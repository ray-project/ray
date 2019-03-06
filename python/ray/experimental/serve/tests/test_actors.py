import numpy as np
import pytest

import ray
from ray.experimental.serve import SingleQuery
from ray.experimental.serve.examples.adder import ScalerAdder, VectorizedAdder
from ray.experimental.serve.examples.counter import Counter, CustomCounter
from ray.experimental.serve.object_id import get_new_oid

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


def test_exception(init_ray):
    adder = ScalerAdder.remote(INCREMENT)
    query = SingleQuery("this can't be added with int", get_new_oid(), 10)
    adder._dispatch.remote([query])
    with pytest.raises(ray.worker.RayTaskError):
        ray.get(query.result_oid)
