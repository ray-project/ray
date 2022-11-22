import os
import sys
import pytest
from ray.util.executor.ray_executor import RayExecutor

def f(i, **kwargs):
    r = [x * x for x in range(i)]
    return len(r)

# @pytest.mark.skipif(
#     sys.platform == "win32", reason="PSUtil does not work the same on windows."
# )
# @pytest.mark.parametrize(
#     "call_ray_start",
#     ["ray start --head --ray-client-server-port 25001 --port 0"],
#     indirect=True,
# )
# def test_remote_function_runs_on_local_instance(call_ray_start):
def test_remote_function_runs_on_local_instance():
    ex = RayExecutor()
    result = ex.submit(f, 1_000)
    assert result == 1000


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
