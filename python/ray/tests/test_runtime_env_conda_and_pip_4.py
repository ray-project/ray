import os
import sys

import pytest

import ray
from ray._private.runtime_env import virtualenv_utils

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


def test_in_virtualenv(ray_start_regular_shared):
    assert (
        virtualenv_utils.is_in_virtualenv() is False
        and "IN_VIRTUALENV" not in os.environ
    ) or (virtualenv_utils.is_in_virtualenv() is True and "IN_VIRTUALENV" in os.environ)

    @ray.remote(runtime_env={"pip": ["pip-install-test==0.5"]})
    def f():
        import pip_install_test  # noqa: F401

        return virtualenv_utils.is_in_virtualenv()

    # Ensure that the runtime env has been installed
    # and virtualenv is activated.
    assert ray.get(f.remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="python.exe in use during deletion."
)
def test_multiple_pip_installs(ray_start_regular_shared):
    """Test that multiple pip installs don't interfere with each other."""

    @ray.remote
    def f():
        return os.environ["TEST_VAR"]

    assert ray.get(
        [
            f.options(
                runtime_env={
                    "pip": ["pip-install-test"],
                    "env_vars": {"TEST_VAR": "1"},
                }
            ).remote(),
            f.options(
                runtime_env={
                    "pip": ["pip-install-test"],
                    "env_vars": {"TEST_VAR": "2"},
                }
            ).remote(),
        ]
    ) == ["1", "2"]


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.",
)
def test_pip_ray_is_overwritten(ray_start_regular_shared):
    @ray.remote
    def f():
        import pip_install_test  # noqa: F401

    # Test an unconstrained "ray" dependency (should work).
    ray.get(f.options(runtime_env={"pip": ["pip-install-test==0.5", "ray"]}).remote())

    # Test a constrained "ray" dependency that matches the env (should work).
    ray.get(
        f.options(runtime_env={"pip": ["pip-install-test==0.5", "ray>=2.0"]}).remote()
    )

    # Test a constrained "ray" dependency that doesn't match the env (shouldn't work).
    with pytest.raises(Exception):
        ray.get(
            f.options(
                runtime_env={"pip": ["pip-install-test==0.5", "ray<2.0"]}
            ).remote()
        )


# pytest-virtualenv doesn't support Python 3.12 as of now, see more details here:
# https://github.com/man-group/pytest-plugins/issues/220
@pytest.mark.skipif(
    "IN_VIRTUALENV" in os.environ
    or sys.platform != "linux"
    or (sys.version_info.major == 3 and sys.version_info.minor >= 12),
    reason="Requires PR wheels built in CI, so only run on linux CI machines.",
)
def test_run_in_virtualenv(cloned_virtualenv):
    python_exe_path = cloned_virtualenv.python

    # make sure cloned_virtualenv.run will run in virtualenv.
    cloned_virtualenv.run(
        f"{python_exe_path} -c 'from ray._private.runtime_env import virtualenv_utils;"
        "assert virtualenv_utils.is_in_virtualenv()'",
        capture=True,
    )

    # Run current test case in virtualenv again.
    # If the command exit code is not zero, this will raise an `Exception`
    # which construct by this pytest-cmd's stderr
    cloned_virtualenv.run(f"IN_VIRTUALENV=1 python -m pytest {__file__}", capture=True)


@pytest.mark.skipif(
    "IN_VIRTUALENV" in os.environ, reason="Pip option not supported in virtual env."
)
def test_runtime_env_with_pip_config(ray_start_regular_shared):
    @ray.remote(
        runtime_env={
            "pip": {"packages": ["pip-install-test==0.5"], "pip_version": "==24.1.2"}
        }
    )
    def f():
        import pip

        return pip.__version__

    assert ray.get(f.remote()) == "24.1.2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
