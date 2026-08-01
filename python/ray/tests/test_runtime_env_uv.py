# TODO(hjiang): A few unit tests to add after full functionality implemented.
# 1. Install specialized version of `uv`.
# 2. Options for `uv install`.

import os
import sys
import tempfile
from pathlib import Path

import pytest

import ray
from ray._private.runtime_env import virtualenv_utils


@pytest.fixture(scope="function")
def tmp_working_dir():
    """A test fixture which writes a requirements file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        requirements_file = path / "requirements.txt"
        with requirements_file.open(mode="w") as f:
            f.write("requests==2.32.3")

        yield str(requirements_file)


def test_uv_install_in_virtualenv(shutdown_only):
    assert (
        virtualenv_utils.is_in_virtualenv() is False
        and "IN_VIRTUALENV" not in os.environ
    ) or (virtualenv_utils.is_in_virtualenv() is True and "IN_VIRTUALENV" in os.environ)
    runtime_env = {"pip": ["pip-install-test==0.5"]}
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def f():
        import pip_install_test  # noqa: F401

        return virtualenv_utils.is_in_virtualenv()

    # Ensure that the runtime env has been installed and virtualenv is activated.
    assert ray.get(f.remote())


# Package installation succeeds.
def test_package_install_with_uv(shutdown_only):
    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.32.3"]}})
    def f():
        import requests

        return requests.__version__

    assert ray.get(f.remote()) == "2.32.3"


# Package installation succeeds, with compatibility enabled.
def test_package_install_with_uv_and_validation(shutdown_only):
    @ray.remote(
        runtime_env={"uv": {"packages": ["requests==2.32.3"], "uv_check": True}}
    )
    def f():
        import requests

        return requests.__version__

    assert ray.get(f.remote()) == "2.32.3"


# Package installation fails due to conflict versions.
def test_package_install_has_conflict_with_uv(shutdown_only):
    # Make it simply impossible to resolve.
    conflict_packages = ["requests<2.32.2", "requests==2.32.2"]

    @ray.remote(runtime_env={"uv": {"packages": conflict_packages}})
    def f():
        import pip

        return pip.__version__

    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(f.remote())


# Specify uv version and check.
def test_uv_with_version_and_check(shutdown_only):
    @ray.remote(
        runtime_env={"uv": {"packages": ["requests==2.32.3"], "uv_version": "==0.4.0"}}
    )
    def f():
        # Not pkg_resources: virtualenv >= 21 no longer seeds setuptools into
        # the venvs Ray creates, and setuptools >= 83 removed pkg_resources.
        import importlib.metadata

        import requests

        assert importlib.metadata.version("uv") == "0.4.0"
        assert requests.__version__ == "2.32.3"

    ray.get(f.remote())


# Package installation via requirements file.
def test_package_install_with_requirements(shutdown_only, tmp_working_dir):
    requirements_file = tmp_working_dir

    @ray.remote(runtime_env={"uv": requirements_file})
    def f():
        import requests

        return requests.__version__

    assert ray.get(f.remote()) == "2.32.3"


# Install different versions of the same package across different tasks, used to check
# uv cache doesn't break runtime env requirement.
def test_package_install_with_different_versions(shutdown_only):
    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.32.3"]}})
    def f():
        import requests

        assert requests.__version__ == "2.32.3"

    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.32.2"]}})
    def g():
        import requests

        assert requests.__version__ == "2.32.2"

    ray.get(f.remote())
    ray.get(g.remote())


# Install packages with cache enabled.
def test_package_install_with_cache_enabled(shutdown_only):
    @ray.remote(
        runtime_env={
            "uv": {"packages": ["requests==2.32.3"], "uv_pip_install_options": []}
        }
    )
    def f():
        import requests

        assert requests.__version__ == "2.32.3"

    @ray.remote(
        runtime_env={
            "uv": {"packages": ["requests==2.32.2"], "uv_pip_install_options": []}
        }
    )
    def g():
        import requests

        assert requests.__version__ == "2.32.2"

    ray.get(f.remote())
    ray.get(g.remote())


# Testing senario: install packages with `uv` with multiple options.
def test_package_install_with_multiple_options(shutdown_only):
    @ray.remote(
        runtime_env={
            "uv": {
                "packages": ["requests==2.32.3"],
                "uv_pip_install_options": ["--no-cache", "--color=auto"],
            }
        }
    )
    def f():
        import requests

        assert requests.__version__ == "2.32.3"

    @ray.remote(
        runtime_env={
            "uv": {
                "packages": ["requests==2.32.2"],
                "uv_pip_install_options": ["--no-cache", "--color=auto"],
            }
        }
    )
    def g():
        import requests

        assert requests.__version__ == "2.32.2"

    ray.get(f.remote())
    ray.get(g.remote())


def test_working_dir_applies_for_uv_creation(shutdown_only, tmp_path):
    """uv packages should expand ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR} like pip.

    Regression for #59343.
    """
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("pip-install-test==0.5\n")

    ray.init(
        runtime_env={
            "working_dir": str(tmp_path),
            "uv": {
                "packages": [
                    "-r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/requirements.txt"
                ]
            },
        }
    )

    @ray.remote
    def test_import():
        import pip_install_test

        return pip_install_test.__name__

    assert ray.get(test_import.remote()) == "pip_install_test"


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            "-r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/requirements.txt",
            "-r /expected/wd/requirements.txt",
        ),
        (
            "--requirements=$RAY_RUNTIME_ENV_CREATE_WORKING_DIR/r.txt",
            "--requirements=/expected/wd/r.txt",
        ),
        ("${UNSET_VAR}", "${UNSET_VAR}"),
        ("$UNSET_VAR", "$UNSET_VAR"),
        ("requests==2.31.0", "requests==2.31.0"),
    ],
)
def test_expand_env_vars(monkeypatch, value, expected):
    """Expansion must use the per-setup environment, not the live process environment.

    `RAY_RUNTIME_ENV_CREATE_WORKING_DIR` is set process-wide by
    `WorkingDirPlugin.with_working_dir_env` for the duration of one setup, so a
    concurrent setup can change it while this coroutine is awaiting. Reading it from
    `os.environ` would resolve a package path against another job's working directory.
    """
    from ray._private.runtime_env.uv import _expand_env_vars

    # A different value in the live environment, as a concurrent setup would leave it.
    monkeypatch.setenv("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", "/other/job/wd")
    env = {"RAY_RUNTIME_ENV_CREATE_WORKING_DIR": "/expected/wd"}

    assert _expand_env_vars(value, env) == expected


def test_expand_env_vars_honors_user_env_vars():
    """`runtime_env["env_vars"]` overrides live only in the per-setup environment."""
    from ray._private.runtime_env.uv import _expand_env_vars

    assert (
        _expand_env_vars("${MY_DIR}/r.txt", {"MY_DIR": "/from/env_vars"})
        == "/from/env_vars/r.txt"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
