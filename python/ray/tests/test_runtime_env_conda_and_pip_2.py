import os
import sys
from unittest import mock

import pytest

import ray
from ray._private.test_utils import generate_runtime_env_dict
from ray.exceptions import RuntimeEnvSetupError

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.fixture(scope="session", autouse=True)
def override_runtime_env_retries():
    with mock.patch.dict(
        os.environ,
        {
            "RUNTIME_ENV_RETRY_TIMES": "1",
        },
    ):
        print("Exporting 'RUNTIME_ENV_RETRY_TIMES=1'")
        yield


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on windows")
@pytest.mark.parametrize("field", ["conda", "pip"])
def test_ray_init_install_failure(
    start_cluster_shared,
    tmp_path,
    field,
):
    cluster, address = start_cluster_shared
    using_ray_client = address.startswith("ray://")
    bad_runtime_env = generate_runtime_env_dict(
        field, "python_object", tmp_path, pip_list=["fake-package"]
    )

    if using_ray_client:
        # When using Ray client, ray.init will raise.
        with pytest.raises(ConnectionAbortedError) as excinfo:
            ray.init(address, runtime_env=bad_runtime_env)
            assert "fake-package" in str(excinfo.value)
    else:
        # When not using Ray client, the error will be surfaced when a task/actor
        # is scheduled.
        ray.init(address, runtime_env=bad_runtime_env)

        @ray.remote
        def g():
            pass

        with pytest.raises(RuntimeEnvSetupError, match="fake-package"):
            ray.get(g.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on windows")
@pytest.mark.parametrize("field", ["conda", "pip"])
def test_install_failure_logging(
    start_cluster_shared,
    field,
    tmp_path,
):
    cluster, address = start_cluster_shared
    ray.init(address)

    @ray.remote(
        runtime_env=generate_runtime_env_dict(
            field, "python_object", tmp_path, pip_list=["does-not-exist-actor"]
        )
    )
    class A:
        def f(self):
            pass

    a = A.remote()  # noqa
    with pytest.raises(RuntimeEnvSetupError, match="does-not-exist-actor"):
        ray.get(a.f.remote())

    @ray.remote(
        runtime_env=generate_runtime_env_dict(
            field, "python_object", tmp_path, pip_list=["does-not-exist-task"]
        )
    )
    def f():
        pass

    with pytest.raises(RuntimeEnvSetupError, match="does-not-exist-task"):
        ray.get(f.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
