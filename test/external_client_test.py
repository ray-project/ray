from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import ray
import numpy as np
import pytest
import subprocess

from ray.test.test_utils import run_and_get_output


# NOTE: These external client tests run a gateway locally, which I don't think
# makes any sense in any real-world scenario. A true ExternalClient test would
# require multiple machines with a network configured more realistically.
@pytest.fixture(scope="module")
def ray_start():
    # Start the Ray processes.
    command = [
        "ray",
        "start",
        "--head",
        "--with-gateway",
        "--redis-port=21216",
        "--use-raylet"
    ]
    out = run_and_get_output(command)
    print(out)
    time.sleep(2)

    # Initialize Ray
    ray.init(redis_address="127.0.0.1:21216",
             gateway_socat_port=5001,
             gateway_data_port=5002,
             use_raylet=True)
    yield None

    # The code after the yield will run as teardown code.
    ray.shutdown()
    subprocess.Popen(["ray", "stop"])


def testBasicExternalClient(ray_start):
    data = 1
    a = ray.put(data)
    assert ray.get(a) == data


def testArrayExternalClient(ray_start):
    data = [1, 2, 3]
    a = ray.put(data)
    ret = ray.get(a)
    for i in range(len(data)):
        assert ret[i] == data[i]


def testNumpyExternalClient(ray_start):
    data = np.random.rand(4, 4)
    a = ray.put(data)

    np.testing.assert_array_almost_equal(ray.get(a), data)


def testMultipleObjectIDs(ray_start):
    pass

