import os
import pytest
import sys
import platform
import time
from ray._private.test_utils import (
    wait_for_condition,
    chdir,
    check_local_files_gced,
    generate_runtime_env_dict,
)
from ray._private.runtime_env.conda import _get_conda_dict_with_ray_inserted
from ray._private.runtime_env.pip import (
    INTERNAL_PIP_FILENAME,
    MAX_INTERNAL_PIP_FILENAME_TRIES,
    _PathHelper,
)
from ray.runtime_env import RuntimeEnv

import yaml
import tempfile
from pathlib import Path
import subprocess

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
    runtime_env = RuntimeEnv(conda=input_conda)
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
def test_requirements_files(start_cluster, field):
    """Test the use of requirements.txt and environment.yaml.

    Tests that requirements files are parsed on the driver, not the cluster.
    This is the desired behavior because the file paths only make sense on the
    driver machine. The files do not exist on the remote cluster.

    Also tests the common use case of specifying the option --extra-index-url
    in a pip requirements.txt file.
    """
    cluster, address = start_cluster

    # cd into a temporary directory and pass in pip/conda requirements file
    # using a relative path.  Since the cluster has already been started,
    # the cwd of the processes on the cluster nodes will not be this
    # temporary directory.  So if the nodes try to read the requirements file,
    # this test should fail because the relative path won't make sense.
    with tempfile.TemporaryDirectory() as tmpdir, chdir(tmpdir):
        pip_list = [
            "--extra-index-url https://pypi.org/simple",
            "pip-install-test==0.5",
        ]
        if field == "conda":
            conda_dict = {"dependencies": ["pip", {"pip": pip_list}]}
            relative_filepath = "environment.yml"
            conda_file = Path(relative_filepath)
            conda_file.write_text(yaml.dump(conda_dict))
            runtime_env = {"conda": relative_filepath}
        elif field == "pip":
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
        reason="Requires PR wheels built in CI, so only run on linux CI machines.",
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


def test_import_in_subprocess(shutdown_only):

    ray.init()

    @ray.remote(runtime_env={"pip": ["pip-install-test==0.5"]})
    def f():
        return subprocess.run(["python", "-c", "import pip_install_test"]).returncode

    assert ray.get(f.remote()) == 0


def test_runtime_env_conda_not_exists_not_hang(shutdown_only):
    """Verify when the conda env doesn't exist, it doesn't hang Ray."""
    ray.init(runtime_env={"conda": "env_which_does_not_exist"})

    @ray.remote
    def f():
        return 1

    refs = [f.remote() for _ in range(5)]

    for ref in refs:
        with pytest.raises(ray.exceptions.RuntimeEnvSetupError) as exc_info:
            ray.get(ref)
        assert "doesn't exist from the output of `conda env list --json`" in str(
            exc_info.value
        )  # noqa


def test_get_requirements_file():
    """Unit test for _PathHelper.get_requirements_file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path_helper = _PathHelper()

        # If pip_list is None, we should return the internal pip filename.
        assert path_helper.get_requirements_file(tmpdir, pip_list=None) == os.path.join(
            tmpdir, INTERNAL_PIP_FILENAME
        )

        # If the internal pip filename is not in pip_list, we should return the internal
        # pip filename.
        assert path_helper.get_requirements_file(
            tmpdir, pip_list=["foo", "bar"]
        ) == os.path.join(tmpdir, INTERNAL_PIP_FILENAME)

        # If the internal pip filename is in pip_list, we should append numbers to the
        # end of the filename until we find one that doesn't conflict.
        assert path_helper.get_requirements_file(
            tmpdir, pip_list=["foo", "bar", f"-r {INTERNAL_PIP_FILENAME}"]
        ) == os.path.join(tmpdir, f"{INTERNAL_PIP_FILENAME}.1")
        assert path_helper.get_requirements_file(
            tmpdir,
            pip_list=[
                "foo",
                "bar",
                f"{INTERNAL_PIP_FILENAME}.1",
                f"{INTERNAL_PIP_FILENAME}.2",
            ],
        ) == os.path.join(tmpdir, f"{INTERNAL_PIP_FILENAME}.3")

        # If we can't find a valid filename, we should raise an error.
        with pytest.raises(RuntimeError) as excinfo:
            path_helper.get_requirements_file(
                tmpdir,
                pip_list=[
                    "foo",
                    "bar",
                    *[
                        f"{INTERNAL_PIP_FILENAME}.{i}"
                        for i in range(MAX_INTERNAL_PIP_FILENAME_TRIES)
                    ],
                ],
            )
        assert "Could not find a valid filename for the internal " in str(excinfo.value)


def test_working_dir_applies_for_pip_creation(start_cluster, tmp_working_dir):
    cluster, address = start_cluster

    with open(Path(tmp_working_dir) / "requirements.txt", "w") as f:
        f.write("-r more_requirements.txt")

    with open(Path(tmp_working_dir) / "more_requirements.txt", "w") as f:
        f.write("pip-install-test==0.5")

    ray.init(
        address,
        runtime_env={
            "working_dir": tmp_working_dir,
            "pip": ["-r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/requirements.txt"],
        },
    )

    @ray.remote
    def test_import():
        import pip_install_test

        return pip_install_test.__name__

    assert ray.get(test_import.remote()) == "pip_install_test"


def test_working_dir_applies_for_pip_creation_files(start_cluster, tmp_working_dir):
    """
    Different from test_working_dir_applies_for_pip_creation, this test uses a file
    in `pip`. This file is read by the driver and hence has no relative path to the
    more_requirements.txt file, so you need to add a
    ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR} in the referenced path.
    """
    cluster, address = start_cluster

    with open(Path(tmp_working_dir) / "requirements.txt", "w") as f:
        f.write("-r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/more_requirements.txt")

    with open(Path(tmp_working_dir) / "more_requirements.txt", "w") as f:
        f.write("pip-install-test==0.5")

    ray.init(
        address,
        runtime_env={
            "working_dir": tmp_working_dir,
            "pip": str(Path(tmp_working_dir) / "requirements.txt"),
        },
    )

    @ray.remote
    def test_import():
        import pip_install_test

        return pip_install_test.__name__

    assert ray.get(test_import.remote()) == "pip_install_test"


def test_working_dir_applies_for_conda_creation(start_cluster, tmp_working_dir):
    cluster, address = start_cluster

    with open(Path(tmp_working_dir) / "requirements.txt", "w") as f:
        f.write("-r more_requirements.txt")

    with open(Path(tmp_working_dir) / "more_requirements.txt", "w") as f:
        f.write("pip-install-test==0.5")

    # Note: if you want to refernce some file in the working dir, you need to use
    # the ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR} variable.
    with open(Path(tmp_working_dir) / "environment.yml", "w") as f:
        f.write("dependencies:\n")
        f.write(" - pip\n")
        f.write(" - pip:\n")
        f.write("   - -r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/more_requirements.txt\n")

    ray.init(
        address,
        runtime_env={
            "working_dir": tmp_working_dir,
            "conda": str(Path(tmp_working_dir) / "environment.yml"),
        },
    )

    @ray.remote
    def test_import():
        import pip_install_test

        return pip_install_test.__name__

    assert ray.get(test_import.remote()) == "pip_install_test"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
