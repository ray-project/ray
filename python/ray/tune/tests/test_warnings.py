import pytest

import ray
from ray import tune
from ray.data.context import DatasetContext
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.tune.error import TuneError


def test_nowarn_zero_cpu():
    def f(*a):
        @ray.remote(num_cpus=0)
        def f():
            pass

        @ray.remote(num_cpus=0)
        class Actor:
            def f(self):
                pass

        ray.get(f.remote())
        a = Actor.remote()
        ray.get(a.f.remote())

    tune.run(f, verbose=0)


def test_warn_cpu():
    def f(*a):
        @ray.remote(num_cpus=1)
        def f():
            pass

        ray.get(f.remote())

    with pytest.raises(TuneError):
        tune.run(f, verbose=0)

    with pytest.raises(TuneError):
        tune.run(
            f, resources_per_trial=tune.PlacementGroupFactory([{"CPU": 1}]), verbose=0
        )

    def g(*a):
        @ray.remote(num_cpus=1)
        class Actor:
            def f(self):
                pass

        a = Actor.remote()
        ray.get(a.f.remote())

    with pytest.raises(TuneError):
        tune.run(g, verbose=0)

    with pytest.raises(TuneError):
        tune.run(
            g, resources_per_trial=tune.PlacementGroupFactory([{"CPU": 1}]), verbose=0
        )


def test_pg_slots_ok():
    def f(*a):
        @ray.remote(num_cpus=1)
        def f():
            pass

        @ray.remote(num_cpus=1)
        class Actor:
            def f(self):
                pass

        ray.get(f.remote())
        a = Actor.remote()
        ray.get(a.f.remote())

    tune.run(
        f, resources_per_trial=tune.PlacementGroupFactory([{"CPU": 1}] * 2), verbose=0
    )


def test_bad_pg_slots():
    def f(*a):
        @ray.remote(num_cpus=2)
        def f():
            pass

        ray.get(f.remote())

    with pytest.raises(TuneError):
        tune.run(
            f,
            resources_per_trial=tune.PlacementGroupFactory([{"CPU": 1}] * 2),
            verbose=0,
        )


def test_dataset_ok():
    def f(*a):
        ray.data.range(10).show()

    tune.run(f, verbose=0)

    def g(*a):
        ctx = DatasetContext.get_current()
        ctx.scheduling_strategy = PlacementGroupSchedulingStrategy(
            ray.util.get_current_placement_group()
        )
        ray.data.range(10).show()

    with pytest.raises(TuneError):
        tune.run(g, verbose=0)

    tune.run(
        g, resources_per_trial=tune.PlacementGroupFactory([{"CPU": 1}] * 2), verbose=0
    )


def test_scheduling_strategy_override():
    def f(*a):
        @ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
        def f():
            pass

        @ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
        class Actor:
            def f(self):
                pass

        # SPREAD tasks are not captured by placement groups, so don't warn.
        ray.get(f.remote())

        # SPREAD actors are not captured by placement groups, so don't warn.
        a = Actor.remote()
        ray.get(a.f.remote())

    tune.run(f, verbose=0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
