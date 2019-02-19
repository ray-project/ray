import numpy as np
import pytest
import ray
from ray.serve import SingleQuery
from ray.serve.examples import VectorizedAdder
from ray.serve.object_id import get_new_oid

INCREMENT = 3


@pytest.fixture
def init_ray():
    ray.init()


def test_vadd(init_ray):
    adder = VectorizedAdder.remote(INCREMENT)

    input_arr = np.arange(10)

    deadline = 11111.11
    inputs = []
    oids = []
    for i in input_arr:
        oid = get_new_oid()
        oids.append(oid)

        inputs.append(SingleQuery(data=i, result_oid=oid, deadline_s=deadline))

    adder._dispatch.remote(inputs)
    result_arr = np.array(ray.get(oids))
    assert np.array_equal(result_arr, input_arr + INCREMENT)
