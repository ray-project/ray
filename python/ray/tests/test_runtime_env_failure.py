import os
from unittest import mock

import pytest
from ray._private.runtime_env.packaging import (
    RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR,
    RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR,
)
import ray
from ray.exceptions import RuntimeEnvSetupError


def using_ray_client(address):
    return address.startswith("ray://")


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


@pytest.fixture
def client_connection_timeout_1s():
    """Lower Ray Client ray.init() timeout to 1 second (default 30s) to save time"""
    with mock.patch.dict(
        os.environ,
        {
            "RAY_CLIENT_RECONNECT_GRACE_PERIOD": "1",
        },
    ):
        yield


class TestRuntimeEnvFailure:
    @pytest.mark.parametrize("plugin", ["working_dir", "py_modules"])
    def test_fail_upload(
        self, tmpdir, monkeypatch, start_cluster, plugin, client_connection_timeout_1s
    ):
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
        self,
        tmpdir,
        monkeypatch,
        fail_download,
        start_cluster,
        plugin,
        client_connection_timeout_1s,
    ):
        """Simulate failing to download the working_dir from the GCS.

        Test that we raise an exception and don't hang.
        """
        _, address = start_cluster
        if plugin == "working_dir":
            runtime_env = {"working_dir": str(tmpdir)}
        else:
            runtime_env = {"py_modules": [str(tmpdir)]}

        def init_ray():
            ray.init(address, runtime_env=runtime_env)

        if using_ray_client(address):
            # Fails at ray.init() because the working_dir is downloaded for the
            # Ray Client server.
            with pytest.raises(ConnectionAbortedError) as e:
                init_ray()
            assert "Failed to download" in str(e.value)
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
            assert "Failed to download" in str(e.value)

    def test_eager_install_fail(
        self, tmpdir, monkeypatch, start_cluster, client_connection_timeout_1s
    ):
        """Simulate failing to install a runtime_env in ray.init().

        By default eager_install is set to True.  We should make sure
        the driver fails to start if the eager_install fails.
        """
        _, address = start_cluster

        def init_ray():
            # Simulate failure using a nonexistent `pip` package.  This will pass
            # validation but fail during installation.
            ray.init(address, runtime_env={"pip": ["ray-nonexistent-pkg"]})

        if using_ray_client(address):
            # Fails at ray.init() because the `pip` package is downloaded for the
            # Ray Client server.
            with pytest.raises(ConnectionAbortedError) as e:
                init_ray()
            assert "No matching distribution found for ray-nonexistent-pkg" in str(
                e.value
            )
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
            assert "No matching distribution found for ray-nonexistent-pkg" in str(
                e.value
            )


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
