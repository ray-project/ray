import os
from ray.workers.setup_runtime_env import inject_dependencies
import pytest
import sys
import unittest
import yaml

import subprocess

from unittest import mock
import ray
from ray._private.utils import get_conda_env_dir, get_conda_bin_executable
from ray._private.runtime_env import RuntimeEnvDict
from ray.job_config import JobConfig
from ray.test_utils import run_string_as_driver


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

    def delete_env(env_name):
        subprocess.run(["conda", "remove", "--name", env_name, "--all", "-y"])

    # Cloning the env twice may take minutes, so parallelize with Ray.
    @ray.remote
    def create_tf_env(tf_version: str):
        env_name = f"tf-{tf_version}"
        delete_env(env_name)
        subprocess.run([
            "conda", "create", "-n", env_name, "--clone", current_conda_env,
            "-y"
        ])
        commands = [
            init_cmd, f"conda activate {env_name}",
            f"python -m pip install tensorflow=={tf_version}",
            "conda deactivate"
        ]
        command_separator = " && "
        command_str = command_separator.join(commands)
        subprocess.run([command_str], shell=True)

    tf_versions = ["2.2.0", "2.3.0"]
    ray.get([create_tf_env.remote(version) for version in tf_versions])
    ray.shutdown()
    yield

    ray.init()

    for tf_version in tf_versions:
        delete_env(env_name=f"tf-{tf_version}")

    subprocess.run([f"{init_cmd} && conda deactivate"], shell=True)
    ray.shutdown()


check_remote_client_conda = """
import ray
context = ray.client("localhost:24001").env({{"conda" : "tf-{tf_version}"}}).\\
connect()
@ray.remote
def get_tf_version():
    import tensorflow as tf
    return tf.__version__

assert ray.get(get_tf_version.remote()) == "{tf_version}"
context.disconnect()
"""


@pytest.mark.skipif(
    os.environ.get("CONDA_DEFAULT_ENV") is None,
    reason="must be run from within a conda environment")
@pytest.mark.skipif(sys.platform == "win32", reason="Unsupported on Windows.")
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on MacOS.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 24001 --port 0"],
    indirect=True)
def test_client_tasks_and_actors_inherit_from_driver(conda_envs,
                                                     call_ray_start):
    @ray.remote
    def get_tf_version():
        import tensorflow as tf
        return tf.__version__

    @ray.remote
    class TfVersionActor:
        def get_tf_version(self):
            import tensorflow as tf
            return tf.__version__

    tf_versions = ["2.2.0", "2.3.0"]
    for i, tf_version in enumerate(tf_versions):
        runtime_env = {"conda": f"tf-{tf_version}"}
        with ray.client("localhost:24001").env(runtime_env).connect():
            assert ray.get(get_tf_version.remote()) == tf_version
            actor_handle = TfVersionActor.remote()
            assert ray.get(actor_handle.get_tf_version.remote()) == tf_version

            # Ensure that we can have a second client connect using the other
            # conda environment.
            other_tf_version = tf_versions[(i + 1) % 2]
            run_string_as_driver(
                check_remote_client_conda.format(tf_version=other_tf_version))


@pytest.mark.skipif(
    os.environ.get("CONDA_DEFAULT_ENV") is None,
    reason="must be run from within a conda environment")
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on MacOS.")
@pytest.mark.skipif(sys.platform == "win32", reason="Unsupported on Windows.")
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
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on MacOS.")
@pytest.mark.skipif(sys.platform == "win32", reason="Unsupported on Windows.")
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
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on MacOS.")
@pytest.mark.skipif(sys.platform == "win32", reason="Unsupported on Windows.")
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
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on MacOS.")
@pytest.mark.skipif(sys.platform == "win32", reason="Unsupported on Windows.")
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


"""
Note(architkulkarni):
These tests only run on Buildkite in a special job that runs
after the wheel is built, because the tests pass in the wheel as a dependency
in the runtime env.  Buildkite only supports Linux for now.
"""


@pytest.mark.skipif(
    os.environ.get("CI") is None,
    reason="This test is only run on CI because it uses the built Ray wheel.")
@pytest.mark.skipif(
    sys.platform != "linux", reason="This test is only run on Buildkite.")
def test_conda_create_task(shutdown_only):
    """Tests dynamic creation of a conda env in a task's runtime env."""
    ray.init()
    runtime_env = {
        "conda": {
            "dependencies": ["pip", {
                "pip": ["pip-install-test==0.5"]
            }]
        }
    }

    @ray.remote
    def f():
        import pip_install_test  # noqa
        return True

    with pytest.raises(ModuleNotFoundError):
        # Ensure pip-install-test is not installed on the test machine
        import pip_install_test  # noqa
    with pytest.raises(ray.exceptions.RayTaskError) as excinfo:
        ray.get(f.remote())
    assert "ModuleNotFoundError" in str(excinfo.value)
    assert ray.get(f.options(runtime_env=runtime_env).remote())


@pytest.mark.skipif(
    os.environ.get("CI") is None,
    reason="This test is only run on CI because it uses the built Ray wheel.")
@pytest.mark.skipif(
    sys.platform != "linux", reason="This test is only run on Buildkite.")
def test_conda_create_job_config(shutdown_only):
    """Tests dynamic conda env creation in a runtime env in the JobConfig."""

    runtime_env = {
        "conda": {
            "dependencies": ["pip", {
                "pip": ["pip-install-test==0.5"]
            }]
        }
    }
    ray.init(job_config=JobConfig(runtime_env=runtime_env))

    @ray.remote
    def f():
        import pip_install_test  # noqa
        return True

    with pytest.raises(ModuleNotFoundError):
        # Ensure pip-install-test is not installed on the test machine
        import pip_install_test  # noqa
    assert ray.get(f.remote())


def test_inject_dependencies():
    num_tests = 4
    conda_dicts = [None] * num_tests
    outputs = [None] * num_tests

    conda_dicts[0] = {}
    outputs[0] = {
        "dependencies": ["python=7.8", "pip", {
            "pip": ["ray==1.2.3"]
        }]
    }

    conda_dicts[1] = {"dependencies": ["blah"]}
    outputs[1] = {
        "dependencies": ["blah", "python=7.8", "pip", {
            "pip": ["ray==1.2.3"]
        }]
    }

    conda_dicts[2] = {"dependencies": ["blah", "pip"]}
    outputs[2] = {
        "dependencies": ["blah", "pip", "python=7.8", {
            "pip": ["ray==1.2.3"]
        }]
    }

    conda_dicts[3] = {"dependencies": ["blah", "pip", {"pip": ["some_pkg"]}]}
    outputs[3] = {
        "dependencies": [
            "blah", "pip", {
                "pip": ["ray==1.2.3", "some_pkg"]
            }, "python=7.8"
        ]
    }

    for i in range(num_tests):
        output = inject_dependencies(conda_dicts[i], "7.8", ["ray==1.2.3"])
        error_msg = (f"failed on input {i}."
                     f"Output: {output} \n"
                     f"Expected output: {outputs[i]}")
        assert (output == outputs[i]), error_msg


@pytest.mark.skipif(
    os.environ.get("CI") is None, reason="This test is only run on CI.")
@pytest.mark.skipif(
    sys.platform != "linux", reason="This test is only run for Linux.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 24001 --port 0"],
    indirect=True)
def test_conda_create_ray_client(call_ray_start):
    """Tests dynamic conda env creation in RayClient."""

    runtime_env = {
        "conda": {
            "dependencies": ["pip", {
                "pip": ["pip-install-test==0.5"]
            }]
        }
    }

    @ray.remote
    def f():
        import pip_install_test  # noqa
        return True

    with ray.client("localhost:24001").env(runtime_env).connect():
        with pytest.raises(ModuleNotFoundError):
            # Ensure pip-install-test is not installed on the test machine
            import pip_install_test  # noqa
        assert ray.get(f.remote())

    with ray.client("localhost:24001").connect():
        with pytest.raises(ModuleNotFoundError):
            # Ensure pip-install-test is not installed in a client that doesn't
            # use the runtime_env
            ray.get(f.remote())


@pytest.mark.skipif(
    os.environ.get("CI") is None,
    reason="This test is only run on CI because it uses the built Ray wheel.")
@pytest.mark.skipif(
    sys.platform != "linux", reason="This test is only run on Buildkite.")
@pytest.mark.parametrize("pip_as_str", [True, False])
def test_pip_task(shutdown_only, pip_as_str, tmp_path):
    """Tests pip installs in the runtime env specified in f.options()."""

    ray.init()
    if pip_as_str:
        d = tmp_path / "pip_requirements"
        d.mkdir()
        p = d / "requirements.txt"
        requirements_txt = """
        pip-install-test==0.5
        """
        p.write_text(requirements_txt)
        runtime_env = {"pip": str(p)}
    else:
        runtime_env = {"pip": ["pip-install-test==0.5"]}

    @ray.remote
    def f():
        import pip_install_test  # noqa
        return True

    with pytest.raises(ModuleNotFoundError):
        # Ensure pip-install-test is not installed on the test machine
        import pip_install_test  # noqa
    with pytest.raises(ray.exceptions.RayTaskError) as excinfo:
        ray.get(f.remote())
    assert "ModuleNotFoundError" in str(excinfo.value)
    assert ray.get(f.options(runtime_env=runtime_env).remote())


@pytest.mark.skipif(
    os.environ.get("CI") is None,
    reason="This test is only run on CI because it uses the built Ray wheel.")
@pytest.mark.skipif(
    sys.platform != "linux", reason="This test is only run on Buildkite.")
def test_pip_ray_serve(shutdown_only):
    """Tests that ray[serve] can be included as a pip dependency."""
    ray.init()
    runtime_env = {"pip": ["pip-install-test==0.5", "ray[serve]"]}

    @ray.remote
    def f():
        import pip_install_test  # noqa
        return True

    with pytest.raises(ModuleNotFoundError):
        # Ensure pip-install-test is not installed on the test machine
        import pip_install_test  # noqa
    with pytest.raises(ray.exceptions.RayTaskError) as excinfo:
        ray.get(f.remote())
    assert "ModuleNotFoundError" in str(excinfo.value)
    assert ray.get(f.options(runtime_env=runtime_env).remote())


@pytest.mark.skipif(
    os.environ.get("CI") is None,
    reason="This test is only run on CI because it uses the built Ray wheel.")
@pytest.mark.skipif(
    sys.platform != "linux", reason="This test is only run on Buildkite.")
@pytest.mark.parametrize("pip_as_str", [True, False])
def test_pip_job_config(shutdown_only, pip_as_str, tmp_path):
    """Tests dynamic installation of pip packages in a task's runtime env."""

    if pip_as_str:
        d = tmp_path / "pip_requirements"
        d.mkdir()
        p = d / "requirements.txt"
        requirements_txt = """
        pip-install-test==0.5
        """
        p.write_text(requirements_txt)
        runtime_env = {"pip": str(p)}
    else:
        runtime_env = {"pip": ["pip-install-test==0.5"]}

    ray.init(job_config=JobConfig(runtime_env=runtime_env))

    @ray.remote
    def f():
        import pip_install_test  # noqa
        return True

    with pytest.raises(ModuleNotFoundError):
        # Ensure pip-install-test is not installed on the test machine
        import pip_install_test  # noqa
    assert ray.get(f.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Unsupported on Windows.")
@pytest.mark.parametrize("use_working_dir", [True, False])
def test_conda_input_filepath(use_working_dir, tmp_path):
    conda_dict = {"dependencies": ["pip", {"pip": ["pip-install-test==0.5"]}]}
    d = tmp_path / "pip_requirements"
    d.mkdir()
    p = d / "environment.yml"

    p.write_text(yaml.dump(conda_dict))

    if use_working_dir:
        runtime_env_dict = RuntimeEnvDict({
            "working_dir": str(d),
            "conda": "environment.yml"
        })
    else:
        runtime_env_dict = RuntimeEnvDict({"conda": str(p)})

    output_conda_dict = runtime_env_dict.get_parsed_dict().get("conda")
    assert output_conda_dict == conda_dict


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


@pytest.mark.skipif(
    os.environ.get("CI") is None,
    reason="This test is only run on CI because it uses the built Ray wheel.")
@pytest.mark.skipif(
    sys.platform != "linux", reason="This test is only run on Buildkite.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 24001 --port 0"],
    indirect=True)
def test_client_working_dir_filepath(call_ray_start, tmp_path):
    """Test that pip and conda relative filepaths work with working_dir."""

    working_dir = tmp_path / "requirements"
    working_dir.mkdir()

    pip_file = working_dir / "requirements.txt"
    requirements_txt = """
    pip-install-test==0.5
    """
    pip_file.write_text(requirements_txt)
    runtime_env_pip = {
        "working_dir": str(working_dir),
        "pip": "requirements.txt"
    }

    conda_file = working_dir / "environment.yml"
    conda_dict = {"dependencies": ["pip", {"pip": ["pip-install-test==0.5"]}]}
    conda_str = yaml.dump(conda_dict)
    conda_file.write_text(conda_str)
    runtime_env_conda = {
        "working_dir": str(working_dir),
        "conda": "environment.yml"
    }

    @ray.remote
    def f():
        import pip_install_test  # noqa
        return True

    with ray.client("localhost:24001").connect():
        with pytest.raises(ModuleNotFoundError):
            # Ensure pip-install-test is not installed in a client that doesn't
            # use the runtime_env
            ray.get(f.remote())

    for runtime_env in [runtime_env_pip, runtime_env_conda]:
        with ray.client("localhost:24001").env(runtime_env).connect():
            with pytest.raises(ModuleNotFoundError):
                # Ensure pip-install-test is not installed on the test machine
                import pip_install_test  # noqa
            assert ray.get(f.remote())


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
