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
            f.write("requests==2.3.0")

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
    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.3.0"]}})
    def f():
        import requests

        return requests.__version__

    assert ray.get(f.remote()) == "2.3.0"


# Package installation succeeds, with compatibility enabled.
def test_package_install_with_uv_and_validation(shutdown_only):
    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.3.0"], "uv_check": True}})
    def f():
        import requests

        return requests.__version__

    assert ray.get(f.remote()) == "2.3.0"


# Package installation fails due to conflict versions.
def test_package_install_has_conflict_with_uv(shutdown_only):
    # moto require requests>=2.5
    conflict_packages = ["moto==3.0.5", "requests==2.4.0"]

    @ray.remote(runtime_env={"uv": {"packages": conflict_packages}})
    def f():
        import pip

        return pip.__version__

    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(f.remote())


# Specify uv version and check.
def test_uv_with_version_and_check(shutdown_only):
    @ray.remote(
        runtime_env={"uv": {"packages": ["requests==2.3.0"], "uv_version": "==0.4.0"}}
    )
    def f():
        import pkg_resources
        import requests

        assert pkg_resources.get_distribution("uv").version == "0.4.0"
        assert requests.__version__ == "2.3.0"

    ray.get(f.remote())


# Package installation via requirements file.
def test_package_install_with_requirements(shutdown_only, tmp_working_dir):
    requirements_file = tmp_working_dir

    @ray.remote(runtime_env={"uv": requirements_file})
    def f():
        import requests

        return requests.__version__

    assert ray.get(f.remote()) == "2.3.0"


# Install different versions of the same package across different tasks, used to check
# uv cache doesn't break runtime env requirement.
def test_package_install_with_different_versions(shutdown_only):
    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.3.0"]}})
    def f():
        import requests

        assert requests.__version__ == "2.3.0"

    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.2.0"]}})
    def g():
        import requests

        assert requests.__version__ == "2.2.0"

    ray.get(f.remote())
    ray.get(g.remote())


# Install packages with cache enabled.
def test_package_install_with_cache_enabled(shutdown_only):
    @ray.remote(
        runtime_env={
            "uv": {"packages": ["requests==2.3.0"], "uv_pip_install_options": []}
        }
    )
    def f():
        import requests

        assert requests.__version__ == "2.3.0"

    @ray.remote(
        runtime_env={
            "uv": {"packages": ["requests==2.2.0"], "uv_pip_install_options": []}
        }
    )
    def g():
        import requests

        assert requests.__version__ == "2.2.0"

    ray.get(f.remote())
    ray.get(g.remote())


# Testing senario: install packages with `uv` with multiple options.
def test_package_install_with_multiple_options(shutdown_only):
    @ray.remote(
        runtime_env={
            "uv": {
                "packages": ["requests==2.3.0"],
                "uv_pip_install_options": ["--no-cache", "--color=auto"],
            }
        }
    )
    def f():
        import requests

        assert requests.__version__ == "2.3.0"

    @ray.remote(
        runtime_env={
            "uv": {
                "packages": ["requests==2.2.0"],
                "uv_pip_install_options": ["--no-cache", "--color=auto"],
            }
        }
    )
    def g():
        import requests

        assert requests.__version__ == "2.2.0"

    ray.get(f.remote())
    ray.get(g.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
