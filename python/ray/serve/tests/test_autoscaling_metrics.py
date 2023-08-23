import time

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.autoscaling_metrics import InMemoryMetricsStore
from ray.serve._private.common import ReplicaState


class TestInMemoryMetricsStore:
    def test_basics(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 2}, timestamp=2)
        assert s.window_average("m1", window_start_timestamp_s=0) == 1.5
        assert s.max("m1", window_start_timestamp_s=0) == 2

    def test_out_of_order_insert(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 5}, timestamp=5)
        s.add_metrics_point({"m1": 3}, timestamp=3)
        s.add_metrics_point({"m1": 2}, timestamp=2)
        s.add_metrics_point({"m1": 4}, timestamp=4)
        assert s.window_average("m1", window_start_timestamp_s=0) == 3
        assert s.max("m1", window_start_timestamp_s=0) == 5

    def test_window_start_timestamp(self):
        s = InMemoryMetricsStore()
        assert s.window_average("m1", window_start_timestamp_s=0) is None
        assert s.max("m1", window_start_timestamp_s=0) is None

        s.add_metrics_point({"m1": 1}, timestamp=2)
        assert s.window_average("m1", window_start_timestamp_s=0) == 1
        assert (
            s.window_average("m1", window_start_timestamp_s=10, do_compact=False)
            is None
        )

    def test_compaction_window(self):
        s = InMemoryMetricsStore()

        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 2}, timestamp=2)

        assert (
            s.window_average("m1", window_start_timestamp_s=0, do_compact=False) == 1.5
        )
        s.window_average("m1", window_start_timestamp_s=1.1, do_compact=True)
        # First record should be removed.
        assert s.window_average("m1", window_start_timestamp_s=0, do_compact=False) == 2

    def test_compaction_max(self):
        s = InMemoryMetricsStore()

        s.add_metrics_point({"m1": 1}, timestamp=2)
        s.add_metrics_point({"m1": 2}, timestamp=1)

        assert s.max("m1", window_start_timestamp_s=0, do_compact=False) == 2

        s.window_average("m1", window_start_timestamp_s=1.1, do_compact=True)

        assert s.window_average("m1", window_start_timestamp_s=0, do_compact=False) == 1

    def test_multiple_metrics(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1, "m2": -1}, timestamp=1)
        s.add_metrics_point({"m1": 2, "m2": -2}, timestamp=2)
        assert s.window_average("m1", window_start_timestamp_s=0) == 1.5
        assert s.max("m1", window_start_timestamp_s=0) == 2
        assert s.max("m2", window_start_timestamp_s=0) == -1


def test_e2e(serve_instance):
    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 2,
            "target_num_ongoing_requests_per_replica": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "look_back_period_s": 1,
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_concurrent_queries=25,
        version="v1",
    )
    class A:
        def __call__(self):
            time.sleep(0.1)

    handle = serve.run(A.bind())
    dep_id = "default_A"
    [handle.remote() for _ in range(50)]

    # Wait for metrics to propagate
    def get_data():
        data = ray.get(
            serve_instance._controller._dump_autoscaling_metrics_for_testing.remote()
        )[dep_id]
        print(data)
        return data

    wait_for_condition(lambda: len(get_data()) > 0)
    print("Autoscaling metrics started recording on controller.")

    # Many queries should be inflight.
    def last_timestamp_value_high():
        data = get_data()
        metrics = list(data.values())
        assert len(metrics) == 2
        assert metrics[0] > 0 and metrics[1] > 0
        assert sum(metrics) > 25
        return True

    wait_for_condition(last_timestamp_value_high)
    print("Confirmed there are metrics from 2 replicas, and many queries are inflight.")

    def check_running_replicas(expected):
        replicas = ray.get(
            serve_instance._controller._dump_replica_states_for_testing.remote(dep_id)
        )
        running_replicas = replicas.get([ReplicaState.RUNNING])
        assert len(running_replicas) == expected
        return True

    # After traffic stops, num replica should drop to 1
    wait_for_condition(check_running_replicas, expected=1, timeout=15)
    print("Num replicas dropped to 1.")

    # The metrics stored on controller should only have info on the remaining replica
    wait_for_condition(lambda: len(get_data()) == 1)
    print("Metrics stored on the controller reduced to 1 replica.")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
