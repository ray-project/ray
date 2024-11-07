# TODO(hjiang): A few unit tests to add after full functionality implemented.
# 1. Install specialized version of `uv`.
# 2. Options for `uv install`.
# 3. Use requirement files for packages.

import os
import pytest
import sys

from ray._private.runtime_env import virtualenv_utils
import ray


def test_uv_install_in_virtualenv(start_cluster):
    assert (
        virtualenv_utils.is_in_virtualenv() is False
        and "IN_VIRTUALENV" not in os.environ
    ) or (virtualenv_utils.is_in_virtualenv() is True and "IN_VIRTUALENV" in os.environ)
    cluster, address = start_cluster
    runtime_env = {"pip": ["pip-install-test==0.5"]}

    ray.init(address, runtime_env=runtime_env)

    @ray.remote
    def f():
        import pip_install_test  # noqa: F401

        return virtualenv_utils.is_in_virtualenv()

    # Ensure that the runtime env has been installed and virtualenv is activated.
    assert ray.get(f.remote())


# Package installation succeeds.
def test_package_install_with_uv():
    @ray.remote(runtime_env={"uv": {"packages": ["requests==2.3.0"]}})
    def f():
        import requests

        return requests.__version__

    assert ray.get(f.remote()) == "2.3.0"


# Package installation fails due to conflict versions.
def test_package_install_has_conflict_with_uv():
    # moto require requests>=2.5
    conflict_packages = ["moto==3.0.5", "requests==2.4.0"]

    @ray.remote(runtime_env={"uv": {"packages": conflict_packages}})
    def f():
        import pip

        return pip.__version__

    with pytest.raises(ray.exceptions.RuntimeEnvSetupError) as _:
        ray.get(f.remote())


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
