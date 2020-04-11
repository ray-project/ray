import numpy as np
import pytest

import ray
from ray.serve.metric import MetricMonitor


@pytest.fixture(scope="session")
def start_target_actor(ray_instance):
    @ray.remote
    class Target:
        def __init__(self):
            self.counter_value = 0

        def get_metrics(self):
            self.counter_value += 1
            return {
                "latency_list": {
                    "type": "list",
                    # Generate 0 to 100 inclusive.
                    # This means total of 101 items.
                    "value": np.arange(101).tolist()
                },
                "counter": {
                    "type": "counter",
                    "value": self.counter_value
                }
            }

        def get_counter_value(self):
            return self.counter_value

    yield Target.remote()


def test_metric_gc(ray_instance, start_target_actor):
    target_actor = start_target_actor
    # this means when new scrapes are invoked, the
    metric_monitor = MetricMonitor.remote(gc_window_seconds=0)
    ray.get(metric_monitor.add_target.remote(target_actor))

    ray.get(metric_monitor.scrape.remote())
    df = ray.get(metric_monitor._get_dataframe.remote())
    assert len(df) == 102

    # Old metric sould be cleared. So only 1 counter + 101 list values left.
    ray.get(metric_monitor.scrape.remote())
    df = ray.get(metric_monitor._get_dataframe.remote())
    assert len(df) == 102


def test_metric_system(ray_instance, start_target_actor):
    target_actor = start_target_actor

    metric_monitor = MetricMonitor.remote()

    ray.get(metric_monitor.add_target.remote(target_actor))

    # Scrape once
    ray.get(metric_monitor.scrape.remote())

    percentiles = [50, 90, 95]
    agg_windows_seconds = [60]
    result = ray.get(
        metric_monitor.collect.remote(percentiles, agg_windows_seconds))
    real_counter_value = ray.get(target_actor.get_counter_value.remote())

    expected_result = {
        "counter": real_counter_value,
        "latency_list_50th_perc_60_window": 50.0,
        "latency_list_90th_perc_60_window": 90.0,
        "latency_list_95th_perc_60_window": 95.0,
    }
    assert result == expected_result
