# coding: utf-8
import logging
import os
import sys
import subprocess

import pytest

from ray._private.test_utils import run_string_as_driver

logger = logging.getLogger(__name__)


def build_env():
    env = os.environ.copy()
    if sys.platform == "win32" and "SYSTEMROOT" not in env:
        env["SYSTEMROOT"] = r"C:\Windows"

    return env


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_init_with_tls(use_tls):
    # Run as a new process to pick up environment variables set
    # in the use_tls fixture
    run_string_as_driver(
        """
import ray
try:
    ray.init()
finally:
    ray.shutdown()
    """,
        env=build_env(),
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_put_get_with_tls(use_tls):
    run_string_as_driver(
        """
import ray
ray.init()
try:
    for i in range(100):
        value_before = i * 10**6
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = i * 10**6 * 1.0
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = "h" * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = [1] * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after
finally:
    ray.shutdown()
    """,
        env=build_env(),
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True, scope="module")
def test_submit_with_tls(use_tls):
    run_string_as_driver(
        """
import ray
ray.init(num_cpus=2, num_gpus=1, resources={"Custom": 1})

@ray.remote
def f(n):
    return list(range(n))

id1, id2, id3 = f._remote(args=[3], num_returns=3)
assert ray.get([id1, id2, id3]) == [0, 1, 2]

@ray.remote
class Actor:
    def __init__(self, x, y=0):
        self.x = x
        self.y = y

    def method(self, a, b=0):
        return self.x, self.y, a, b

a = Actor._remote(
    args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})

id1, id2, id3, id4 = a.method._remote(
    args=["test"], kwargs={"b": 2}, num_returns=4)
assert ray.get([id1, id2, id3, id4]) == [0, 1, "test", 2]
    """,
        env=build_env(),
    )


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_client_connect_to_tls_server(use_tls, call_ray_start):
    tls_env = build_env()  # use_tls fixture sets TLS environment variables
    without_tls_env = {k: v for k, v in tls_env.items() if "TLS" not in k}

    # Attempt to connect without TLS
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_string_as_driver(
            """
from ray.util.client import ray as ray_client
ray_client.connect("localhost:10001")
     """,
            env=without_tls_env,
        )
    assert "ConnectionError" in exc_info.value.output.decode("utf-8")

    # Attempt to connect with TLS
    out = run_string_as_driver(
        """
import ray
from ray.util.client import ray as ray_client
ray_client.connect("localhost:10001")
print(ray.is_initialized())
     """,
        env=tls_env,
    )
    assert out.strip() == "True"


if __name__ == "__main__":
    import pytest
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
