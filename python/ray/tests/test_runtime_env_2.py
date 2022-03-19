import pytest

import ray
from ray.runtime_env import RuntimeEnv, RuntimeEnvConfig


def test_runtime_env_config(start_cluster):
    _, address = start_cluster
    bad_configs = []
    bad_configs.append({"setup_timeout_seconds": 10.0})
    bad_configs.append({"setup_timeout_seconds": 0})
    bad_configs.append({"setup_timeout_seconds": "10"})

    good_configs = []
    good_configs.append({"setup_timeout_seconds": 10})
    good_configs.append({"setup_timeout_seconds": -1})

    @ray.remote
    def f():
        return True

    def raise_exception_run(fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception:
            pass
        else:
            assert False

    for bad_config in bad_configs:

        def run(runtime_env):
            raise_exception_run(ray.init, address, runtime_env=runtime_env)
            raise_exception_run(f.options, runtime_env=runtime_env)

        runtime_env = {"config": bad_config}
        run(runtime_env)

        raise_exception_run(RuntimeEnvConfig, **bad_config)
        raise_exception_run(RuntimeEnv, config=bad_config)

    for good_config in good_configs:

        def run(runtime_env):
            ray.shutdown()
            ray.init(address, runtime_env=runtime_env)
            assert ray.get(f.options(runtime_env=runtime_env).remote())

        runtime_env = {"config": good_config}
        run(runtime_env)
        runtime_env = {"config": RuntimeEnvConfig(**good_config)}
        run(runtime_env)
        runtime_env = RuntimeEnv(config=good_config)
        run(runtime_env)
        runtime_env = RuntimeEnv(config=RuntimeEnvConfig(**good_config))
        run(runtime_env)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
