from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import numpy as np
import pytest


# NOTE: These client tests run a gateway locally, which I don't think
# makes any sense in any real-world scenario. A true Client test would
# require multiple machines with a network configured more realistically.
@pytest.fixture
def ray_start():
    # Start the Ray processes.
    # ray.init(driver_mode=ray.CLIENT_MODE, gateway_port=5432, use_raylet=True)
    ray.init(driver_mode=ray.CLIENT_MODE, redis_address="127.0.0.1:21216", gateway_port=5432, use_raylet=True)
    yield None
    print("wuh")
    # The code after the yield will run as teardown code.
    ray.shutdown()

def testBasicClient(ray_start):
    data = 1
    a = ray.put(data)
    assert ray.get(a) == data

def testArrayClient(ray_start):
    data = [1, 2, 3]
    a = ray.put(data)

    ret = ray.get(a)
    for i in range(len(data)):
        assert ret[i] == data[i]

def testNumpyClient(ray_start):
    data = np.random.rand(4, 4)
    a = ray.put(data)
    assert ray.get(a) == data
