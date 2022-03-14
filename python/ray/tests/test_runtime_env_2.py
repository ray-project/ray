import pytest
import sys
import time

import ray
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnv


bad_runtime_env_cache_ttl_seconds = 10


@pytest.mark.skipif(
    sys.platform == "win32", reason="conda in runtime_env unsupported on Windows."
)
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
@pytest.mark.parametrize(
    "set_bad_runtime_env_cache_ttl_seconds",
    [
        str(bad_runtime_env_cache_ttl_seconds),
    ],
    indirect=True,
)
def test_invalid_conda_env(
    shutdown_only, runtime_env_class, set_bad_runtime_env_cache_ttl_seconds
):
    ray.init()

    @ray.remote
    def f():
        pass

    @ray.remote
    class A:
        def f(self):
            pass

    start = time.time()
    bad_env = runtime_env_class(conda={"dependencies": ["this_doesnt_exist"]})
    with pytest.raises(
        RuntimeEnvSetupError,
        # The actual error message should be included in the exception.
        match="ResolvePackageNotFound",
    ):
        ray.get(f.options(runtime_env=bad_env).remote())
    first_time = time.time() - start

    # Check that another valid task can run.
    ray.get(f.remote())

    a = A.options(runtime_env=bad_env).remote()
    with pytest.raises(
        ray.exceptions.RuntimeEnvSetupError, match="ResolvePackageNotFound"
    ):
        ray.get(a.f.remote())

    # The second time this runs it should be faster as the error is cached.
    start = time.time()
    with pytest.raises(RuntimeEnvSetupError, match="ResolvePackageNotFound"):
        ray.get(f.options(runtime_env=bad_env).remote())

    assert (time.time() - start) < (first_time / 2.0)

    # Sleep to wait bad runtime env cache removed.
    time.sleep(bad_runtime_env_cache_ttl_seconds)

    # The third time this runs it should be slower as the error isn't cached.
    start = time.time()
    with pytest.raises(RuntimeEnvSetupError, match="ResolvePackageNotFound"):
        ray.get(f.options(runtime_env=bad_env).remote())

    assert (time.time() - start) > (first_time / 2.0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
