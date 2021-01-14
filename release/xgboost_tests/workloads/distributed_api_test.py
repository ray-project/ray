import ray

from xgboost_ray.tests.test_xgboost_api import XGBoostAPITest


class XGBoostDistributedAPITest(XGBoostAPITest):
    def _init_ray(self):
        if not ray.is_initialized():
            ray.init(address="auto")


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", f"{__file__}::XGBoostDistributedAPITest"]))
