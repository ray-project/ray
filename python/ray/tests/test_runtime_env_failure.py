import os
from unittest import mock

import pytest
from ray._private.runtime_env.packaging import (
    RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR,
    RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR,
)
import ray
from ray.exceptions import RuntimeEnvSetupError

from ray.runtime_env import RuntimeEnv


def using_ray_client(address):
    return address.startswith("ray://")


@pytest.fixture
def set_agent_failure_env_var():
    os.environ["_RAY_AGENT_FAILING"] = "1"
    yield
    del os.environ["_RAY_AGENT_FAILING"]


# TODO(SongGuyang): Fail the agent which is in different node from driver.
@pytest.mark.skip(
    reason="Agent failure will lead to raylet failure and driver failure."
)
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
def test_runtime_env_broken(
    set_agent_failure_env_var, runtime_env_class, ray_start_cluster_head
):
    @ray.remote
    class A:
        def ready(self):
            pass

    @ray.remote
    def f():
        pass

    runtime_env = runtime_env_class(env_vars={"TF_WARNINGS": "none"})
    """
    Test task raises an exception.
    """
    with pytest.raises(ray.exceptions.LocalRayletDiedError):
        ray.get(f.options(runtime_env=runtime_env).remote())
    """
    Test actor task raises an exception.
    """
    a = A.options(runtime_env=runtime_env).remote()
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.ready.remote())


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
def client_connection_timeout_5s():
    """Lower Ray Client ray.init() to 1 second (default 30) to save time"""
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
        self, tmpdir, monkeypatch, start_cluster, plugin, client_connection_timeout_5s
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
        client_connection_timeout_5s,
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
        self, tmpdir, monkeypatch, start_cluster, client_connection_timeout_5s
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
