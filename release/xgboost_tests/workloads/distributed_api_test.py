"""Distributed XGBoost API test

This test runs unit tests on a distributed cluster. This will confirm that
XGBoost API features like custom metrics/objectives work with remote
trainables.

Test owner: krfricke

Acceptance criteria: Unit tests should pass (requires pytest).
"""

import ray

from xgboost_ray.tests.test_xgboost_api import XGBoostAPITest
from xgboost_ray.tests.test_data_source import ModinDataSourceTest


class XGBoostDistributedAPITest(XGBoostAPITest):
    def _init_ray(self):
        if not ray.is_initialized():
            ray.init(address="auto")


class XGBoostDistributedModinDataSourceTest(ModinDataSourceTest):
    def _init_ray(self):
        if not ray.is_initialized():
            ray.init(address="auto")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", f"{__file__}::XGBoostDistributedAPITest"]))
