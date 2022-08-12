import sys
import pytest
from pkg_resources import Requirement

import ray


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Pip option not supported on Windows.",
)
def test_runtime_env_with_pip_config(start_cluster):

    pip_versions = [
        "==20.2.3",
        "<20.3, >19",
    ]

    @ray.remote
    def f():
        import pip

        return pip.__version__

    for pip_version in pip_versions:
        requirement = Requirement.parse(f"pip{pip_version}")
        assert (
            ray.get(
                f.options(
                    runtime_env={
                        "pip": {
                            "packages": ["pip-install-test==0.5"],
                            "pip_version": pip_version,
                        }
                    }
                ).remote()
            )
            in requirement
        )


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Pip option not supported on Windows.",
)
def test_runtime_env_with_conflict_pip_version(start_cluster):
    pip_version = "<19,>19"

    @ray.remote(
        runtime_env={
            "pip": {"packages": ["pip-install-test==0.5"], "pip_version": "<19,>19"}
        }
    )
    def f():
        import pip

        return pip.__version__

    with pytest.raises(ray.exceptions.RuntimeEnvSetupError) as error:
        ray.get(f.remote())

    assert f"No matching distribution found for pip{pip_version}" in str(error.value)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Pip option not supported on Windows.",
)
def test_runtime_env_cache_with_pip_check(start_cluster):

    # moto require requests>=2.5
    conflict_packages = ["moto==3.0.5", "requests==2.4.0"]
    runtime_env = {
        "pip": {
            "packages": conflict_packages,
            "pip_version": "==20.2.3",
            "pip_check": False,
        }
    }

    @ray.remote
    def f():
        return True

    assert ray.get(f.options(runtime_env=runtime_env).remote())

    runtime_env["pip"]["pip_version"] = "==21.3.1"
    # Just modify filed pip_version, but this time,
    # not hit cache and raise an exception
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError) as error:
        ray.get(f.options(runtime_env=runtime_env).remote())

    assert "The conflict is caused by:" in str(error.value)
    assert "The user requested requests==2.4.0" in str(error.value)
    assert "moto 3.0.5 depends on requests>=2.5" in str(error.value)

    runtime_env["pip"]["pip_check"] = True
    runtime_env["pip"]["pip_version"] = "==20.2.3"
    # Just modify filed pip_check, but this time,
    # not hit cache and raise an exception
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError) as error:
        ray.get(f.options(runtime_env=runtime_env).remote())


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
