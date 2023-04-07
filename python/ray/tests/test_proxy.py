import os
import pytest
from ray._private.test_utils import (
    client_test_enabled,
    run_string_as_driver,
)


# https://github.com/ray-project/ray/issues/6662
@pytest.mark.skipif(client_test_enabled(), reason="interferes with grpc")
def test_http_proxy(start_http_proxy, shutdown_only):
    # C++ config `grpc_enable_http_proxy` only initializes once, so we have to
    # run driver as a separate process to make sure the correct config value
    # is initialized.
    script = """
import ray

ray.init(num_cpus=1)

@ray.remote
def f():
    return 1

assert ray.get(f.remote()) == 1
"""

    env = start_http_proxy
    run_string_as_driver(script, dict(os.environ, **env))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
