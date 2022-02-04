import os
import pytest
import sys
import platform
import time
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.test_utils import (
    wait_for_condition,
    chdir,
    check_local_files_gced,
    generate_runtime_env_dict,
)
from ray._private.runtime_env.conda import _get_conda_dict_with_ray_inserted
from ray._private.runtime_env.validation import ParsedRuntimeEnv

import yaml
import tempfile
from pathlib import Path

import ray

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


def test_get_conda_dict_with_ray_inserted_m1_wheel(monkeypatch):
    # Disable dev mode to prevent Ray dependencies being automatically inserted
    # into the conda dict.
    if os.environ.get("RAY_RUNTIME_ENV_LOCAL_DEV_MODE") is not None:
        monkeypatch.delenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE")
    if os.environ.get("RAY_CI_POST_WHEEL_TESTS") is not None:
        monkeypatch.delenv("RAY_CI_POST_WHEEL_TESTS")
    monkeypatch.setattr(ray, "__version__", "1.9.0")
    monkeypatch.setattr(ray, "__commit__", "92599d9127e228fe8d0a2d94ca75754ec21c4ae4")
    monkeypatch.setattr(sys, "version_info", (3, 9, 7, "final", 0))
    # Simulate running on an M1 Mac.
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")

    input_conda = {"dependencies": ["blah", "pip", {"pip": ["pip_pkg"]}]}
    runtime_env = RuntimeEnv(ParsedRuntimeEnv({"conda": input_conda}).serialize())
    output_conda = _get_conda_dict_with_ray_inserted(runtime_env)
    # M1 wheels are not uploaded to AWS S3.  So rather than have an S3 URL
    # inserted as a dependency, we should just have the string "ray==1.9.0".
    assert output_conda == {
        "dependencies": [
            "blah",
            "pip",
            {"pip": ["ray==1.9.0", "ray[default]", "pip_pkg"]},
            "python=3.9.7",
        ]
    }


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.",
)
@pytest.mark.parametrize("field", ["conda", "pip"])
def test_files_remote_cluster(start_cluster, field):
    """Test that requirements files are parsed on the driver, not the cluster.

    This is the desired behavior because the file paths only make sense on the
    driver machine. The files do not exist on the remote cluster.
    """
    cluster, address = start_cluster

    # cd into a temporary directory and pass in pip/conda requirements file
    # using a relative path.  Since the cluster has already been started,
    # the cwd of the processes on the cluster nodes will not be this
    # temporary directory.  So if the nodes try to read the requirements file,
    # this test should fail because the relative path won't make sense.
    with tempfile.TemporaryDirectory() as tmpdir, chdir(tmpdir):
        if field == "conda":
            conda_dict = {"dependencies": ["pip", {"pip": ["pip-install-test==0.5"]}]}
            relative_filepath = "environment.yml"
            conda_file = Path(relative_filepath)
            conda_file.write_text(yaml.dump(conda_dict))
            runtime_env = {"conda": relative_filepath}
        elif field == "pip":
            pip_list = ["pip-install-test==0.5"]
            relative_filepath = "requirements.txt"
            pip_file = Path(relative_filepath)
            pip_file.write_text("\n".join(pip_list))
            runtime_env = {"pip": relative_filepath}

        ray.init(address, runtime_env=runtime_env)

        @ray.remote
        def f():
            import pip_install_test  # noqa: F401

            return True

        # Ensure that the runtime env has been installed.
        assert ray.get(f.remote())


class TestGC:
    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Needs PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.parametrize("field", ["conda", "pip"])
    @pytest.mark.parametrize("spec_format", ["file", "python_object"])
    def test_job_level_gc(
        self, runtime_env_disable_URI_cache, start_cluster, field, spec_format, tmp_path
    ):
        """Tests that job-level conda env is GC'd when the job exits."""
        # We must use a single-node cluster.  If we simulate a multi-node
        # cluster then the conda installs will proceed simultaneously, one on
        # each node, but since they're actually running on the same machine we
        # get errors.
        cluster, address = start_cluster

        ray.init(
            address, runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path)
        )

        @ray.remote
        def f():
            import pip_install_test  # noqa: F401

            return True

        # Ensure that the runtime env has been installed.
        assert ray.get(f.remote())
        # Sleep some seconds before checking that we didn't GC. Otherwise this
        # check may spuriously pass.
        time.sleep(2)
        assert not check_local_files_gced(cluster)

        ray.shutdown()

        wait_for_condition(lambda: check_local_files_gced(cluster), timeout=30)

        # Check that we can reconnect with the same env.  (In other words, ensure
        # the conda env was fully deleted and not left in some kind of corrupted
        # state that prevents reinstalling the same conda env.)

        ray.init(
            address, runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path)
        )

        assert ray.get(f.remote())

    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason=("Requires PR wheels built in CI, so only run on linux CI " "machines."),
    )
    @pytest.mark.parametrize("field", ["conda", "pip"])
    @pytest.mark.parametrize("spec_format", ["file", "python_object"])
    def test_detached_actor_gc(
        self, runtime_env_disable_URI_cache, start_cluster, field, spec_format, tmp_path
    ):
        """Tests that detached actor's conda env is GC'd only when it exits."""
        cluster, address = start_cluster

        ray.init(
            address,
            namespace="test",
            runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path),
        )

        @ray.remote
        class A:
            def test_import(self):
                import pip_install_test  # noqa: F401

                return True

        a = A.options(name="test", lifetime="detached").remote()
        ray.get(a.test_import.remote())

        assert not check_local_files_gced(cluster)

        ray.shutdown()
        ray.init(address, namespace="test")

        assert not check_local_files_gced(cluster)

        a = ray.get_actor("test")
        assert ray.get(a.test_import.remote())

        ray.kill(a)

        wait_for_condition(lambda: check_local_files_gced(cluster), timeout=30)

    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason=("Requires PR wheels built in CI, so only run on linux CI " "machines."),
    )
    @pytest.mark.parametrize("field", ["conda", "pip"])
    @pytest.mark.parametrize("spec_format", ["file", "python_object"])
    def test_actor_level_gc(
        self, runtime_env_disable_URI_cache, start_cluster, field, spec_format, tmp_path
    ):
        """Tests that actor-level working_dir is GC'd when the actor exits."""
        cluster, address = start_cluster

        ray.init(address)

        runtime_env = generate_runtime_env_dict(field, spec_format, tmp_path)

        @ray.remote
        class A:
            def test_import(self):
                import pip_install_test  # noqa: F401

                return True

        NUM_ACTORS = 5
        actors = [
            A.options(runtime_env=runtime_env).remote() for _ in range(NUM_ACTORS)
        ]
        ray.get([a.test_import.remote() for a in actors])
        for i in range(5):
            assert not check_local_files_gced(cluster)
            ray.kill(actors[i])
        wait_for_condition(lambda: check_local_files_gced(cluster))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
