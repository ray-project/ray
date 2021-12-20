import os
import pytest
import sys
from ray._private.test_utils import (wait_for_condition, chdir,
                                     check_local_files_gced,
                                     generate_runtime_env_dict)

import yaml
import tempfile
from pathlib import Path

import ray

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
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
            conda_dict = {
                "dependencies": ["pip", {
                    "pip": ["pip-install-test==0.5"]
                }]
            }
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


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_job_level_gc(start_cluster, field, spec_format, tmp_path):
    """Tests that job-level conda env is GC'd when the job exits."""
    # We must use a single-node cluster.  If we simulate a multi-node cluster
    # then the conda installs will proceed simultaneously, one on each node,
    # but since they're actually running on the same machine we get errors.
    cluster, address = start_cluster

    ray.init(
        address,
        runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path))

    @ray.remote
    def f():
        import pip_install_test  # noqa: F401
        return True

    # Ensure that the runtime env has been installed.
    assert ray.get(f.remote())

    assert not check_local_files_gced(cluster)

    ray.shutdown()

    wait_for_condition(lambda: check_local_files_gced(cluster), timeout=30)

    # Check that we can reconnect with the same env.  (In other words, ensure
    # the conda env was fully deleted and not left in some kind of corrupted
    # state that prevents reinstalling the same conda env.)

    ray.init(
        address,
        runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path))

    assert ray.get(f.remote())


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_detached_actor_gc(start_cluster, field, spec_format, tmp_path):
    """Tests that a detached actor's conda env is GC'd only when it exits."""
    cluster, address = start_cluster

    ray.init(
        address,
        namespace="test",
        runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path))

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
