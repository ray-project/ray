import os
import sys
import time
import timeit

import pytest

import ray
from ray.anyscale._private.prestart import prestart_workers_on_pg_bundles
from ray.tests.conftest import maybe_external_redis, shutdown_only  # noqa: F401
from ray.util.placement_group import PlacementGroupSchedulingStrategy, placement_group


# Fixture to create a temporary module named `slow_to_import`
@pytest.fixture
def slow_import_module(tmp_path):
    (tmp_path / "__init__.py").touch()
    (tmp_path / "slow_to_import.py").write_text(
        """
import time
time.sleep(5)

def hello():
    return "hello from slow_to_import"
"""
    )
    yield tmp_path


def test_module_is_slow(slow_import_module):
    sys.path.insert(0, str(slow_import_module))
    print(f"{sys.path=}")
    import_time = timeit.timeit("import slow_to_import", number=1)
    print(f"Import time: {import_time:.2f} seconds")
    assert import_time >= 5


# F811 redefinition of unused 'shutdown_only', flake8 can't understand pytest fixtures
def test_ray_prestart(slow_import_module, shutdown_only):  # noqa: F811
    def try_import():
        import slow_to_import  # noqa: F401

    ray.init(
        num_cpus=5,
        runtime_env={
            "working_dir": slow_import_module,
            "worker_process_setup_hook": try_import,
        },
    )

    print(ray.get_runtime_context().runtime_env)

    runtime_env = {"env_vars": {"KEY": "value"}}
    pg = placement_group([{"CPU": 1}] * 5)
    one_hour_s = 3600
    objs = prestart_workers_on_pg_bundles(
        pg, runtime_env, keep_alive_s=one_hour_s, num_workers_per_bundle=1
    )
    ray.get(objs)
    # prestart is requested. Note this obj is waiting only for the request, NOT waiting
    # for the worker creation finish.

    # Wait for the prestart workers to start and load the slow module.
    time.sleep(5)

    @ray.remote(runtime_env=runtime_env)
    def imports_slow():
        print(f"{sys.path=}, {ray.get_runtime_context().runtime_env=}, {os.environ=}")
        # Already loaded in prestart
        assert "slow_to_import" in sys.modules
        # should be fast
        import slow_to_import

        return slow_to_import.hello() + f", KEY={os.environ['KEY']}"

    def run_tasks():
        results = ray.get(
            [
                imports_slow.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                    )
                ).remote()
                for _ in range(5)
            ]
        )
        assert results == ["hello from slow_to_import, KEY=value"] * 5

    time_spent = timeit.timeit(run_tasks, number=1)
    assert time_spent < 5


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
