from copy import deepcopy

import pytest

from ray.serve._private.common import TargetCapacityDirection
from ray.serve._private.controller import (
    applications_match,
    calculate_target_capacity_direction,
)
from ray.serve._private.controller_health_metrics_tracker import (
    _HEALTH_METRICS_HISTORY_SIZE,
    ControllerHealthMetrics,
    ControllerHealthMetricsTracker,
    DurationStats,
)
from ray.serve.schema import (
    HTTPOptionsSchema,
    ServeApplicationSchema,
    ServeDeploySchema,
)


def create_app_config(name: str) -> ServeApplicationSchema:
    return ServeApplicationSchema(
        name=name, import_path=f"fake.{name}", route_prefix=f"/{name}"
    )


class TestApplicationsMatch:
    def test_config_with_self(self):
        config = ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="app1",
                    import_path="fake.import",
                    route_prefix="/",
                ),
            ],
        )

        assert applications_match(config, config) is True

    def test_configs_with_matching_app_names(self):
        config1 = ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="app1",
                    import_path="fake.import",
                    route_prefix="/app1",
                ),
                ServeApplicationSchema(
                    name="app2",
                    import_path="fake.import2",
                    route_prefix="/app2",
                ),
                ServeApplicationSchema(
                    name="app3",
                    import_path="fake.import3",
                    route_prefix="/app3",
                ),
            ],
        )

        # Configs contain apps with same name but different import paths.
        config2 = deepcopy(config1)
        config2.applications[0].import_path = "different_fake.import"
        assert applications_match(config1, config2) is True

        config2.applications[0].import_path = "extended.fake.import"
        assert applications_match(config1, config2) is True

        config2.applications[0].import_path = "fake:import"
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different route_prefixes.
        config2 = deepcopy(config1)
        config2.applications[0].route_prefix += "_suffix"
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different runtime_envs.
        config2 = deepcopy(config1)
        config2.applications[1].runtime_env = {"working_dir": "https://fake/uri"}
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different target_capacities.
        config2 = deepcopy(config1)
        config2.target_capacity = 50
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different http options.
        config2 = deepcopy(config1)
        config2.http_options = HTTPOptionsSchema(host="62.79.45.100")
        assert applications_match(config1, config2) is True

    def test_configs_with_different_app_names(self):
        config1 = ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="app1",
                    import_path="fake.import",
                    route_prefix="/app1",
                ),
                ServeApplicationSchema(
                    name="app2",
                    import_path="fake.import2",
                    route_prefix="/app2",
                ),
                ServeApplicationSchema(
                    name="app3",
                    import_path="fake.import3",
                    route_prefix="/app3",
                ),
            ],
        )

        # Configs contain apps with different names but same import paths.
        config2 = deepcopy(config1)
        config2.applications[0].name = "different_app1"
        assert applications_match(config1, config2) is False

        # Configs contain different number of apps.
        config2 = deepcopy(config1)
        config2.applications.pop()
        assert applications_match(config1, config2) is False


class TestCalculateScaleDirection:
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN],
    )
    @pytest.mark.parametrize(
        "new_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN],
    )
    def test_change_target_capacity_numeric(self, curr_direction, new_direction):
        curr_target_capacity = 5

        curr_config = ServeDeploySchema(
            target_capacity=curr_target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
            ],
        )

        if new_direction == TargetCapacityDirection.UP:
            new_target_capacity = curr_target_capacity * 3.3
        elif new_direction == TargetCapacityDirection.DOWN:
            new_target_capacity = curr_target_capacity / 3.3

        new_config = deepcopy(curr_config)
        new_config.target_capacity = new_target_capacity

        # The new direction must be returned, regardless of the current direction.
        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            == new_direction
        )

    @pytest.mark.parametrize("target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN, None],
    )
    def test_no_change_target_capacity(self, target_capacity, curr_direction):
        """When target_capacity doesn't change, return the current direction."""

        if target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        curr_config = ServeDeploySchema(
            target_capacity=target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        assert (
            calculate_target_capacity_direction(
                curr_config,
                curr_config,
                curr_direction,
            )
            == curr_direction
        )

    @pytest.mark.parametrize("curr_target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN, None],
    )
    def test_enter_null_target_capacity(self, curr_target_capacity, curr_direction):
        """When target capacity becomes null, scale up/down behavior must stop."""

        if curr_target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        curr_config = ServeDeploySchema(
            target_capacity=curr_target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        new_config = deepcopy(curr_config)
        new_config.target_capacity = None

        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            is None
        )

    @pytest.mark.parametrize("new_target_capacity", [0, 50, 100])
    def test_exit_null_target_capacity(self, new_target_capacity):
        """When target capacity goes null -> non-null, scale down must start."""

        curr_config = ServeDeploySchema(
            target_capacity=None,
            applications=[create_app_config(name="app1")],
        )

        new_config = deepcopy(curr_config)
        new_config.target_capacity = new_target_capacity

        # When Serve is already running the applications at target_capacity
        # None, and then a target_capacity is applied, the direction must
        # become DOWN.
        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                None,
            )
            == TargetCapacityDirection.DOWN
        )

    def test_scale_up_first_config(self):
        """Check how Serve handles the first config that's applied."""

        # Case 1: target_capacity is set. Serve should transition to scaling up.

        new_config = ServeDeploySchema(
            target_capacity=20,
            applications=[create_app_config(name="app1")],
        )
        assert (
            calculate_target_capacity_direction(
                None,
                new_config,
                None,
            )
            == TargetCapacityDirection.UP
        )

        # Case 2: target_capacity is not set. Serve should not be scaling.

        new_config = ServeDeploySchema(
            target_capacity=None,
            applications=[create_app_config(name="app1")],
        )
        assert (
            calculate_target_capacity_direction(
                None,
                new_config,
                None,
            )
            is None
        )

    @pytest.mark.parametrize("curr_target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN, None],
    )
    def test_config_live_apps_mismatch(self, curr_target_capacity, curr_direction):
        """Apply a config with apps that don't match the live apps.

        Serve should treat this like applying the first config. Its scaling
        direction should not be based on the previous config's target_capacity.
        """

        if curr_target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        curr_config = ServeDeploySchema(
            target_capacity=curr_target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        # Case 1: target_capacity is set. Serve should transition to scaling up.
        new_config = ServeDeploySchema(
            target_capacity=30,
            applications=[
                create_app_config(name="new_app1"),
                create_app_config(name="app2"),
            ],
        )

        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            is TargetCapacityDirection.UP
        )

        # Case 2: target_capacity is not set. Serve should not be scaling.
        new_config = ServeDeploySchema(
            target_capacity=None,
            applications=[
                create_app_config(name="new_app1"),
                create_app_config(name="app2"),
            ],
        )

        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            is None
        )


class TestDurationStats:
    """Tests for DurationStats model."""

    def test_empty_values(self):
        """Test statistics from empty list."""
        stats = DurationStats.from_values([])
        assert stats.mean == 0.0
        assert stats.std == 0.0
        assert stats.min == 0.0
        assert stats.max == 0.0

    def test_single_value(self):
        """Test statistics from single value."""
        stats = DurationStats.from_values([5.0])
        assert stats.mean == 5.0
        assert stats.std == 0.0
        assert stats.min == 5.0
        assert stats.max == 5.0

    def test_multiple_values(self):
        """Test statistics from multiple values."""
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        stats = DurationStats.from_values(values)
        assert stats.mean == 3.0
        assert stats.min == 1.0
        assert stats.max == 5.0
        # std for [1,2,3,4,5] with population std = sqrt(2)
        assert abs(stats.std - 1.4142135623730951) < 0.0001

    def test_dict_serialization(self):
        """Test that DurationStats serializes to dict."""
        stats = DurationStats(mean=1.0, std=0.5, min=0.5, max=1.5)
        result = stats.dict()
        assert result == {"mean": 1.0, "std": 0.5, "min": 0.5, "max": 1.5}


class TestControllerHealthMetrics:
    """Tests for ControllerHealthMetrics dataclass."""

    def test_default_values(self):
        """Test that all default values are initialized correctly."""
        metrics = ControllerHealthMetrics()
        assert metrics.timestamp == 0.0
        assert metrics.controller_start_time == 0.0
        assert metrics.uptime_s == 0.0
        assert metrics.num_control_loops == 0
        assert metrics.loop_duration_s is None
        assert metrics.event_loop_delay_s == 0.0
        assert metrics.num_asyncio_tasks == 0
        assert metrics.process_memory_mb == 0.0

    def test_dict(self):
        """Test serialization to dictionary."""
        loop_stats = DurationStats(mean=0.3, std=0.1, min=0.1, max=0.5)
        metrics = ControllerHealthMetrics(
            timestamp=1000.0,
            controller_start_time=900.0,
            uptime_s=100.0,
            num_control_loops=50,
            loop_duration_s=loop_stats,
        )
        result = metrics.dict()

        assert isinstance(result, dict)
        assert result["timestamp"] == 1000.0
        assert result["controller_start_time"] == 900.0
        assert result["uptime_s"] == 100.0
        assert result["num_control_loops"] == 50
        assert result["loop_duration_s"]["mean"] == 0.3

    def test_all_fields_in_dict(self):
        """Ensure dict() includes all fields."""
        metrics = ControllerHealthMetrics()
        result = metrics.dict()

        expected_keys = [
            "timestamp",
            "controller_start_time",
            "uptime_s",
            "num_control_loops",
            "loop_duration_s",
            "loops_per_second",
            "last_sleep_duration_s",
            "expected_sleep_duration_s",
            "event_loop_delay_s",
            "num_asyncio_tasks",
            "deployment_state_update_duration_s",
            "application_state_update_duration_s",
            "proxy_state_update_duration_s",
            "node_update_duration_s",
            "handle_metrics_delay_ms",
            "replica_metrics_delay_ms",
            "process_memory_mb",
        ]

        for key in expected_keys:
            assert key in result, f"Missing key: {key}"


class TestCollectHealthMetrics:
    """Tests for the health metrics collection logic."""

    def test_loop_statistics_computation(self):
        """Test that loop statistics are computed correctly from tracker data."""
        tracker = ControllerHealthMetricsTracker()

        # Record some loop durations
        durations = [0.1, 0.2, 0.3, 0.4, 0.5]
        for d in durations:
            tracker.record_loop_duration(d)

        # Verify tracker state
        assert len(tracker.loop_durations) == 5
        assert tracker.loop_durations[-1] == 0.5

        # Collect metrics and verify DurationStats
        metrics = tracker.collect_metrics()
        assert metrics.loop_duration_s is not None
        assert metrics.loop_duration_s.mean == 0.3
        assert metrics.loop_duration_s.min == 0.1
        assert metrics.loop_duration_s.max == 0.5
        assert metrics.loop_duration_s.std > 0

    def test_metrics_delay_statistics(self):
        """Test that metrics delay statistics are computed correctly."""
        tracker = ControllerHealthMetricsTracker()

        # Record handle metrics delays
        handle_delays = [10.0, 20.0, 30.0, 40.0, 50.0]
        for d in handle_delays:
            tracker.record_handle_metrics_delay(d)

        # Record replica metrics delays
        replica_delays = [5.0, 15.0, 25.0, 35.0, 45.0]
        for d in replica_delays:
            tracker.record_replica_metrics_delay(d)

        # Collect metrics and verify DurationStats
        metrics = tracker.collect_metrics()

        assert metrics.handle_metrics_delay_ms is not None
        assert metrics.handle_metrics_delay_ms.mean == 30.0
        assert metrics.handle_metrics_delay_ms.min == 10.0
        assert metrics.handle_metrics_delay_ms.max == 50.0

        assert metrics.replica_metrics_delay_ms is not None
        assert metrics.replica_metrics_delay_ms.mean == 25.0
        assert metrics.replica_metrics_delay_ms.min == 5.0
        assert metrics.replica_metrics_delay_ms.max == 45.0

    def test_empty_metrics_delays(self):
        """Test handling of empty metrics delay lists."""
        tracker = ControllerHealthMetricsTracker()

        # When no delays recorded, DurationStats should have zero values
        metrics = tracker.collect_metrics()

        assert metrics.handle_metrics_delay_ms is not None
        assert metrics.handle_metrics_delay_ms.mean == 0.0
        assert metrics.handle_metrics_delay_ms.max == 0.0

        assert metrics.replica_metrics_delay_ms is not None
        assert metrics.replica_metrics_delay_ms.mean == 0.0
        assert metrics.replica_metrics_delay_ms.max == 0.0

    def test_event_loop_delay_calculation(self):
        """Test event loop delay is calculated correctly."""
        from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S

        tracker = ControllerHealthMetricsTracker()

        # Case 1: Sleep took longer than expected (overloaded)
        tracker.last_sleep_duration_s = CONTROL_LOOP_INTERVAL_S + 0.5
        delay = max(0.0, tracker.last_sleep_duration_s - CONTROL_LOOP_INTERVAL_S)
        assert delay == 0.5

        # Case 2: Sleep took expected time (healthy)
        tracker.last_sleep_duration_s = CONTROL_LOOP_INTERVAL_S
        delay = max(0.0, tracker.last_sleep_duration_s - CONTROL_LOOP_INTERVAL_S)
        assert delay == 0.0

        # Case 3: Sleep was shorter (shouldn't happen, but handle it)
        tracker.last_sleep_duration_s = CONTROL_LOOP_INTERVAL_S - 0.1
        delay = max(0.0, tracker.last_sleep_duration_s - CONTROL_LOOP_INTERVAL_S)
        assert delay == 0.0

    def test_loops_per_second_calculation(self):
        """Test loops per second calculation."""
        import time

        tracker = ControllerHealthMetricsTracker()
        tracker.controller_start_time = time.time() - 10.0  # Started 10 seconds ago
        tracker.num_control_loops = 5

        now = time.time()
        uptime = now - tracker.controller_start_time
        loops_per_second = tracker.num_control_loops / uptime if uptime > 0 else 0.0

        # Should be approximately 0.5 loops per second
        assert 0.4 < loops_per_second < 0.6

    def test_component_update_durations_tracked(self):
        """Test that component update durations are tracked with DurationStats."""
        tracker = ControllerHealthMetricsTracker()

        # Record some component update durations
        dsm_durations = [0.1, 0.2, 0.3, 0.4, 0.5]
        asm_durations = [0.2, 0.3, 0.4, 0.5, 0.6]
        proxy_durations = [0.3, 0.4, 0.5, 0.6, 0.7]
        node_durations = [0.05, 0.06, 0.07, 0.08, 0.09]

        for d in dsm_durations:
            tracker.record_dsm_update_duration(d)
        for d in asm_durations:
            tracker.record_asm_update_duration(d)
        for d in proxy_durations:
            tracker.record_proxy_update_duration(d)
        for d in node_durations:
            tracker.record_node_update_duration(d)

        # Collect metrics and verify DurationStats
        metrics = tracker.collect_metrics()

        assert metrics.deployment_state_update_duration_s is not None
        assert metrics.deployment_state_update_duration_s.mean == 0.3
        assert metrics.deployment_state_update_duration_s.min == 0.1
        assert metrics.deployment_state_update_duration_s.max == 0.5

        assert metrics.application_state_update_duration_s is not None
        assert metrics.application_state_update_duration_s.mean == 0.4
        assert metrics.application_state_update_duration_s.min == 0.2
        assert metrics.application_state_update_duration_s.max == 0.6

        assert metrics.proxy_state_update_duration_s is not None
        assert metrics.proxy_state_update_duration_s.mean == 0.5
        assert metrics.proxy_state_update_duration_s.min == 0.3
        assert metrics.proxy_state_update_duration_s.max == 0.7

        assert metrics.node_update_duration_s is not None
        assert abs(metrics.node_update_duration_s.mean - 0.07) < 0.0001
        assert metrics.node_update_duration_s.min == 0.05
        assert metrics.node_update_duration_s.max == 0.09


class TestControllerHealthMetricsTracker:
    """Tests for ControllerHealthMetricsTracker."""

    def test_record_loop_duration(self):
        """Test recording loop durations."""
        tracker = ControllerHealthMetricsTracker()

        tracker.record_loop_duration(0.5)
        assert len(tracker.loop_durations) == 1
        assert tracker.loop_durations[-1] == 0.5

        tracker.record_loop_duration(0.3)
        assert len(tracker.loop_durations) == 2
        assert tracker.loop_durations[-1] == 0.3

    def test_rolling_window_size(self):
        """Test that rolling window doesn't exceed max size."""
        tracker = ControllerHealthMetricsTracker()

        # Record more than the max history size
        for i in range(_HEALTH_METRICS_HISTORY_SIZE + 50):
            tracker.record_loop_duration(float(i))

        assert len(tracker.loop_durations) == _HEALTH_METRICS_HISTORY_SIZE
        # The oldest values should have been dropped
        assert tracker.loop_durations[0] == 50.0

    def test_record_handle_metrics_delay(self):
        """Test recording handle metrics delays."""
        tracker = ControllerHealthMetricsTracker()

        tracker.record_handle_metrics_delay(100.0)
        assert len(tracker.handle_metrics_delays) == 1
        assert tracker.handle_metrics_delays[-1] == 100.0

    def test_record_replica_metrics_delay(self):
        """Test recording replica metrics delays."""
        tracker = ControllerHealthMetricsTracker()

        tracker.record_replica_metrics_delay(50.0)
        assert len(tracker.replica_metrics_delays) == 1
        assert tracker.replica_metrics_delays[-1] == 50.0

    def test_multiple_delay_records(self):
        """Test recording multiple metrics delays."""
        tracker = ControllerHealthMetricsTracker()

        for i in range(10):
            tracker.record_handle_metrics_delay(float(i * 10))
            tracker.record_replica_metrics_delay(float(i * 5))

        assert len(tracker.handle_metrics_delays) == 10
        assert len(tracker.replica_metrics_delays) == 10
        assert tracker.handle_metrics_delays[-1] == 90.0
        assert tracker.replica_metrics_delays[-1] == 45.0

    def test_record_dsm_update_duration(self):
        """Test recording deployment state manager update durations."""
        tracker = ControllerHealthMetricsTracker()

        tracker.record_dsm_update_duration(0.1)
        assert len(tracker.dsm_update_durations) == 1
        assert tracker.dsm_update_durations[-1] == 0.1

        tracker.record_dsm_update_duration(0.2)
        assert len(tracker.dsm_update_durations) == 2
        assert tracker.dsm_update_durations[-1] == 0.2

    def test_record_asm_update_duration(self):
        """Test recording application state manager update durations."""
        tracker = ControllerHealthMetricsTracker()

        tracker.record_asm_update_duration(0.15)
        assert len(tracker.asm_update_durations) == 1
        assert tracker.asm_update_durations[-1] == 0.15

    def test_record_proxy_update_duration(self):
        """Test recording proxy state update durations."""
        tracker = ControllerHealthMetricsTracker()

        tracker.record_proxy_update_duration(0.25)
        assert len(tracker.proxy_update_durations) == 1
        assert tracker.proxy_update_durations[-1] == 0.25

    def test_record_node_update_duration(self):
        """Test recording node update durations."""
        tracker = ControllerHealthMetricsTracker()

        tracker.record_node_update_duration(0.05)
        assert len(tracker.node_update_durations) == 1
        assert tracker.node_update_durations[-1] == 0.05

    def test_component_duration_rolling_window(self):
        """Test that component duration rolling windows respect max size."""
        tracker = ControllerHealthMetricsTracker()

        # Record more than the max history size
        for i in range(_HEALTH_METRICS_HISTORY_SIZE + 50):
            tracker.record_dsm_update_duration(float(i))

        assert len(tracker.dsm_update_durations) == _HEALTH_METRICS_HISTORY_SIZE
        # The oldest values should have been dropped
        assert tracker.dsm_update_durations[0] == 50.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
