import os
import pytest
import sys

from ray._private.runtime_env.pip import PipProcessor
import ray


if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.",
)
def test_in_virtualenv(start_cluster):
    assert (
        PipProcessor._is_in_virtualenv() is False and "IN_VIRTUALENV" not in os.environ
    ) or (PipProcessor._is_in_virtualenv() is True and "IN_VIRTUALENV" in os.environ)
    cluster, address = start_cluster
    runtime_env = {"pip": ["pip-install-test==0.5"]}

    ray.init(address, runtime_env=runtime_env)

    @ray.remote
    def f():
        import pip_install_test  # noqa: F401

        return PipProcessor._is_in_virtualenv()

    # Ensure that the runtime env has been installed
    # and virtualenv is activated.
    assert ray.get(f.remote())


class TestGC:
    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Requires PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.parametrize("field", ["pip"])
    def test_pip_ray_is_overwritten(self, start_cluster, field):
        cluster, address = start_cluster

        # It should be OK to install packages with ray dependency.
        ray.init(address, runtime_env={"pip": ["pip-install-test==0.5", "ray"]})

        @ray.remote
        def f():
            import pip_install_test  # noqa: F401

            return True

        # Ensure that the runtime env has been installed.
        assert ray.get(f.remote())

        ray.shutdown()

        # It should be OK if cluster ray meets the installing ray version.
        ray.init(address, runtime_env={"pip": ["pip-install-test==0.5", "ray>1.6.0"]})

        @ray.remote
        def f():
            import pip_install_test  # noqa: F401

            return True

        # Ensure that the runtime env has been installed.
        assert ray.get(f.remote())

        ray.shutdown()

        # It will raise exceptions if ray is overwritten.
        with pytest.raises(Exception):
            ray.init(
                address, runtime_env={"pip": ["pip-install-test==0.5", "ray<=1.6.0"]}
            )

            @ray.remote
            def f():
                import pip_install_test  # noqa: F401

                return True

            # Ensure that the runtime env has been installed.
            assert ray.get(f.remote())

        ray.shutdown()


@pytest.mark.skipif(
    "IN_VIRTUALENV" in os.environ or sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.",
)
def test_run_in_virtualenv(cloned_virtualenv):
    python_exe_path = cloned_virtualenv.python
    print(python_exe_path)

    # make sure cloned_virtualenv.run will run in virtualenv.
    cloned_virtualenv.run(
        f"{python_exe_path} -c 'from ray._private.runtime_env.pip import PipProcessor;"
        "assert PipProcessor._is_in_virtualenv()'",
        capture=True,
    )

    # Run current test case in virtualenv again.
    # If the command exit code is not zero, this will raise an `Exception`
    # which construct by this pytest-cmd's stderr
    cloned_virtualenv.run(f"IN_VIRTUALENV=1 python -m pytest {__file__}", capture=True)


@pytest.mark.skipif(
    "IN_VIRTUALENV" in os.environ or sys.platform == "win32",
    reason="Pip option not supported on Windows.",
)
def test_runtime_env_with_pip_config(start_cluster):
    @ray.remote(
        runtime_env={
            "pip": {"packages": ["pip-install-test==0.5"], "pip_version": "==20.2.3"}
        }
    )
    def f():
        import pip

        return pip.__version__

    assert ray.get(f.remote()) == "20.2.3"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
