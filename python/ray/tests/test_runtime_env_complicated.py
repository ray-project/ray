import os
import pytest
import sys
import unittest

import subprocess

from unittest import mock
import ray
from ray._private.utils import get_conda_env_dir, get_conda_bin_executable
from ray.job_config import JobConfig


@pytest.fixture(scope="session")
def conda_envs():
    """Creates two copies of current conda env with different tf versions."""
    ray.init()
    conda_path = get_conda_bin_executable("conda")
    init_cmd = (f". {os.path.dirname(conda_path)}"
                f"/../etc/profile.d/conda.sh")
    subprocess.run([f"{init_cmd} && conda activate"], shell=True)
    current_conda_env = os.environ.get("CONDA_DEFAULT_ENV")
    assert current_conda_env is not None

    # Cloning the env twice may take minutes, so parallelize with Ray.
    @ray.remote
    def create_tf_env(tf_version: str):

        subprocess.run([
            "conda", "create", "-n", f"tf-{tf_version}", f"--clone",
            current_conda_env, "-y"
        ])
        commands = [
            init_cmd, f"conda activate tf-{tf_version}",
            f"pip install tensorflow=={tf_version}", "conda deactivate"
        ]
        command_separator = " && "
        command_str = command_separator.join(commands)
        subprocess.run([command_str], shell=True)

    tf_versions = ["2.2.0", "2.3.0"]
    ray.get([create_tf_env.remote(version) for version in tf_versions])
    ray.shutdown()
    yield

    ray.init()

    @ray.remote
    def remove_tf_env(tf_version: str):
        subprocess.run(
            ["conda", "remove", "-n", f"tf-{tf_version}", "--all", "-y"])

    ray.get([remove_tf_env.remote(version) for version in tf_versions])
    subprocess.run([f"{init_cmd} && conda deactivate"], shell=True)
    ray.shutdown()


@pytest.mark.skipif(
    os.environ.get("CONDA_DEFAULT_ENV") is None,
    reason="must be run from within a conda environment")
def test_task_conda_env(conda_envs, shutdown_only):
    import tensorflow as tf
    ray.init()

    @ray.remote
    def get_tf_version():
        return tf.__version__

    tf_versions = ["2.2.0", "2.3.0"]
    for tf_version in tf_versions:
        runtime_env = {"conda": f"tf-{tf_version}"}
        task = get_tf_version.options(runtime_env=runtime_env)
        assert ray.get(task.remote()) == tf_version


@pytest.mark.skipif(
    os.environ.get("CONDA_DEFAULT_ENV") is None,
    reason="must be run from within a conda environment")
def test_actor_conda_env(conda_envs, shutdown_only):
    import tensorflow as tf
    ray.init()

    @ray.remote
    class TfVersionActor:
        def get_tf_version(self):
            return tf.__version__

    tf_versions = ["2.2.0", "2.3.0"]
    for tf_version in tf_versions:
        runtime_env = {"conda": f"tf-{tf_version}"}
        actor = TfVersionActor.options(runtime_env=runtime_env).remote()
        assert ray.get(actor.get_tf_version.remote()) == tf_version


@pytest.mark.skipif(
    os.environ.get("CONDA_DEFAULT_ENV") is None,
    reason="must be run from within a conda environment")
def test_inheritance_conda_env(conda_envs, shutdown_only):
    import tensorflow as tf
    ray.init()

    @ray.remote
    def get_tf_version():
        return tf.__version__

    @ray.remote
    def wrapped_tf_version():
        return ray.get(get_tf_version.remote())

    @ray.remote
    class TfVersionActor:
        def get_tf_version(self):
            return ray.get(wrapped_tf_version.remote())

    tf_versions = ["2.2.0", "2.3.0"]
    for tf_version in tf_versions:
        runtime_env = {"conda": f"tf-{tf_version}"}
        task = wrapped_tf_version.options(runtime_env=runtime_env)
        assert ray.get(task.remote()) == tf_version
        actor = TfVersionActor.options(runtime_env=runtime_env).remote()
        assert ray.get(actor.get_tf_version.remote()) == tf_version


@pytest.mark.skipif(
    os.environ.get("CONDA_DEFAULT_ENV") is None,
    reason="must be run from within a conda environment")
def test_job_config_conda_env(conda_envs, shutdown_only):
    import tensorflow as tf

    tf_version = "2.2.0"

    @ray.remote
    def get_conda_env():
        return tf.__version__

    for tf_version in ["2.2.0", "2.3.0"]:
        runtime_env = {"conda": f"tf-{tf_version}"}
        ray.init(job_config=JobConfig(runtime_env=runtime_env))
        assert ray.get(get_conda_env.remote()) == tf_version
        ray.shutdown()


def test_get_conda_env_dir(tmp_path):
    from pathlib import Path
    """
    Typical output of `conda env list`, for context:

    base                 /Users/scaly/anaconda3
    my_env_1             /Users/scaly/anaconda3/envs/my_env_1

    For this test, `tmp_path` is a stand-in for `Users/scaly/anaconda3`.
    """

    # Simulate starting in an env named tf1.
    d = tmp_path / "envs" / "tf1"
    Path.mkdir(d, parents=True)
    with mock.patch.dict(os.environ, {
            "CONDA_PREFIX": str(d),
            "CONDA_DEFAULT_ENV": "tf1"
    }):
        with pytest.raises(ValueError):
            # Env tf2 should not exist.
            env_dir = get_conda_env_dir("tf2")
        tf2_dir = tmp_path / "envs" / "tf2"
        Path.mkdir(tf2_dir, parents=True)
        env_dir = get_conda_env_dir("tf2")
        assert (env_dir == str(tmp_path / "envs" / "tf2"))

    # Simulate starting in (base) conda env.
    with mock.patch.dict(os.environ, {
            "CONDA_PREFIX": str(tmp_path),
            "CONDA_DEFAULT_ENV": "base"
    }):
        with pytest.raises(ValueError):
            # Env tf3 should not exist.
            env_dir = get_conda_env_dir("tf3")
        # Env tf2 still should exist.
        env_dir = get_conda_env_dir("tf2")
        assert (env_dir == str(tmp_path / "envs" / "tf2"))


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_experimental_package(shutdown_only):
    ray.init(num_cpus=2)
    pkg = ray.experimental.load_package(
        os.path.join(
            os.path.dirname(__file__),
            "../experimental/packaging/example_pkg/ray_pkg.yaml"))
    a = pkg.MyActor.remote()
    assert ray.get(a.f.remote()) == "hello world"
    assert ray.get(pkg.my_func.remote()) == "hello world"


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_experimental_package_lazy(shutdown_only):
    pkg = ray.experimental.load_package(
        os.path.join(
            os.path.dirname(__file__),
            "../experimental/packaging/example_pkg/ray_pkg.yaml"))
    ray.init(num_cpus=2)
    a = pkg.MyActor.remote()
    assert ray.get(a.f.remote()) == "hello world"
    assert ray.get(pkg.my_func.remote()) == "hello world"


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_experimental_package_github(shutdown_only):
    ray.init(num_cpus=2)
    pkg = ray.experimental.load_package(
        "http://raw.githubusercontent.com/ray-project/ray/master/"
        "python/ray/experimental/packaging/example_pkg/ray_pkg.yaml")
    a = pkg.MyActor.remote()
    assert ray.get(a.f.remote()) == "hello world"
    assert ray.get(pkg.my_func.remote()) == "hello world"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
