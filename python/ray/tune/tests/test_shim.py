import unittest
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.ax import AxSearch


class ShimCreationTest(unittest.TestCase):
    def testCreateScheduler(self):
        scheduler = "async_hyperband"
        kwargs = {"metric": "metric_foo", "mode": "min"}

        shim_scheduler = tune.create_scheduler(scheduler, **kwargs)
        real_scheduler = AsyncHyperBandScheduler(scheduler, **kwargs)
        assert shim_scheduler == real_scheduler

    def testCreateSearcher(self):
        searcher = "ax"
        kwargs = {"metric": "metric_foo", "mode": "min"}

        shim_searcher = tune.create_searcher(searcher, **kwargs)
        real_searcher = AxSearch(searcher, **kwargs)
        assert shim_searcher == real_searcher


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main([__file__]))
