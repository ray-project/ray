"""Distributed LightGBM API test

This test runs unit tests on a distributed cluster. This will confirm that
LightGBM API features like custom metrics/objectives work with remote
trainables.

Test owner: Yard1 (primary), krfricke

Acceptance criteria: Unit tests should pass (requires pytest).
"""

import ray

from lightgbm_ray.tests.test_lightgbm_api import LightGBMAPITest


class LightGBMDistributedAPITest(LightGBMAPITest):
    def _init_ray(self):
        if not ray.is_initialized():
            ray.init(address="auto")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", f"{__file__}::LightGBMDistributedAPITest"]))
