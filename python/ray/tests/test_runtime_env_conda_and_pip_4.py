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
    assert PipProcessor._is_in_virtualenv() is False
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
