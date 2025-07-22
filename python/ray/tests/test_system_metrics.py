import os
import sys
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    raw_metrics,
)

METRIC_CONFIG = {
    "_system_config": {
        "metrics_report_interval_ms": 100,
    }
}


def test_unintentional_worker_failures_metric(shutdown_only):
    context = ray.init(num_cpus=2, **METRIC_CONFIG)

    @ray.remote
    class Actor:
        def exit(self):
            os._exit(1)

    actor1 = Actor.remote()
    actor2 = Actor.remote()
    # intentional
    ray.kill(actor1)
    # unintentional
    actor2.exit.remote()

    def verify():
        metrics = raw_metrics(context)
        for sample in metrics["ray_unintentional_worker_failures_total"]:
            assert sample.value == 1
        return True

    # Wait for metrics to be reported
    time.sleep(1)
    wait_for_condition(lambda: verify())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
