import os
import sys
from unittest import mock

import pytest

import ray
from ray._private.ray_constants import RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT
from ray._private.runtime_env.packaging import (
    RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR,
    RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR,
)
from ray.exceptions import RuntimeEnvSetupError


def using_ray_client():
    return ray._private.client_mode_hook.is_client_mode_enabled


# Set scope to "class" to force this to run before start_cluster, whose scope
# is "function".  We need this environment variable to be set before Ray is started.
@pytest.fixture(scope="class")
def fail_download():
    with mock.patch.dict(
        os.environ,
        {
            RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR: "1",
        },
    ):
        print("RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING enabled.")
        yield


@pytest.mark.skipif(
    using_ray_client(),
    reason="Ray Client doesn't clean up global state properly on ray.init() failure.",
)
@pytest.mark.parametrize("plugin", ["working_dir", "py_modules"])
def test_fail_upload(tmpdir, monkeypatch, start_cluster, plugin):
    """Simulate failing to upload the working_dir to the GCS.

    Test that we raise an exception and don't hang.
    """
    monkeypatch.setenv(RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR, "1")
    _, address = start_cluster
    if plugin == "working_dir":
        runtime_env = {"working_dir": str(tmpdir)}
    else:
        runtime_env = {"py_modules": [str(tmpdir)]}

    with pytest.raises(RuntimeEnvSetupError) as e:
        ray.init(address, runtime_env=runtime_env)
    assert "Failed to upload" in str(e.value)


@pytest.mark.parametrize("plugin", ["working_dir", "py_modules"])
def test_fail_download(
    tmpdir,
    fail_download,
    start_cluster,
    plugin,
):
    """Simulate failing to download the working_dir from the GCS.

    Test that we raise an exception and don't hang.
    """
    _, address = start_cluster
    if plugin == "working_dir":
        runtime_env = {"working_dir": str(tmpdir)}
    else:
        runtime_env = {"py_modules": [str(tmpdir)]}

    # TODO(architkulkarni): After #25972 is resolved, we should raise an
    # exception in ray.init().  Until then, we need to `ray.get` a task
    # to raise the exception.
    ray.init(address, runtime_env=runtime_env)

    @ray.remote
    def f():
        pass

    with pytest.raises(RuntimeEnvSetupError) as e:
        ray.get(f.remote())
    assert "Failed to download" in str(e.value)
    assert (f"the default is {RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT}") in str(
        e.value
    )


def test_eager_install_fail(tmpdir, start_cluster):
    """Simulate failing to install a runtime_env in ray.init().

    By default eager_install is set to True.  We should make sure
    the driver fails to start if the eager_install fails.
    """
    _, address = start_cluster

    def init_ray():
        # Simulate failure using a nonexistent `pip` package.  This will pass
        # validation but fail during installation.
        ray.init(address, runtime_env={"pip": ["ray-nonexistent-pkg"]})

    if using_ray_client():
        # Fails at ray.init() because the `pip` package is downloaded for the
        # Ray Client server.
        with pytest.raises(ConnectionAbortedError) as e:
            init_ray()
        assert "No matching distribution found for ray-nonexistent-pkg" in str(e.value)
    else:
        init_ray()

        # TODO(architkulkarni): After #25972 is resolved, we should raise an
        # exception in ray.init().  Until then, we need to `ray.get` a task
        # to raise the exception.
        @ray.remote
        def f():
            pass

        with pytest.raises(RuntimeEnvSetupError) as e:
            ray.get(f.remote())
        assert "No matching distribution found for ray-nonexistent-pkg" in str(e.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
