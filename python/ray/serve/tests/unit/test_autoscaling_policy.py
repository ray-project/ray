import sys

import pytest

from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S
from ray.serve.autoscaling_policy import (
    _calculate_desired_num_replicas,
    replica_queue_length_autoscaling_policy,
)
from ray.serve.config import AutoscalingConfig


class TestCalculateDesiredNumReplicas:
    def test_bounds_checking(self):
        num_replicas = 10
        max_replicas = 11
        min_replicas = 9
        config = AutoscalingConfig(
            max_replicas=max_replicas,
            min_replicas=min_replicas,
            target_ongoing_requests=100,
        )

        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=150 * num_replicas,
            num_running_replicas=num_replicas,
        )
        assert desired_num_replicas == max_replicas

        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=50 * num_replicas,
            num_running_replicas=num_replicas,
        )
        assert desired_num_replicas == min_replicas

        for i in range(50, 150):
            desired_num_replicas = _calculate_desired_num_replicas(
                autoscaling_config=config,
                total_num_requests=i * num_replicas,
                num_running_replicas=num_replicas,
            )
            assert min_replicas <= desired_num_replicas <= max_replicas

    @pytest.mark.parametrize("target_requests", [0.5, 1.0, 1.5])
    def test_scale_up(self, target_requests):
        config = AutoscalingConfig(
            min_replicas=0, max_replicas=100, target_ongoing_requests=target_requests
        )
        num_replicas = 10
        num_ongoing_requests = 2 * target_requests * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 19 <= desired_num_replicas <= 21  # 10 * 2 = 20

    @pytest.mark.parametrize("target_requests", [0.5, 1.0, 1.5])
    def test_scale_down(self, target_requests):
        config = AutoscalingConfig(
            min_replicas=0, max_replicas=100, target_ongoing_requests=target_requests
        )
        num_replicas = 10
        num_ongoing_requests = 0.5 * target_requests * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 4 <= desired_num_replicas <= 6  # 10 * 0.5 = 5

    @pytest.mark.parametrize("use_deprecated_smoothing_factor", [True, False])
    def test_scaling_factor(self, use_deprecated_smoothing_factor):
        config = {"min_replicas": 0, "max_replicas": 100, "target_ongoing_requests": 2}

        if use_deprecated_smoothing_factor:
            config["smoothing_factor"] = 0.5
        else:
            config["upscaling_factor"] = 0.5
            config["downscaling_factor"] = 0.5

        config = AutoscalingConfig(**config)
        num_replicas = 10

        num_ongoing_requests = 8.0 * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        num_ongoing_requests = 0.25 * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25

    @pytest.mark.parametrize("use_deprecated_smoothing_factor", [True, False])
    def test_upscaling_factor(self, use_deprecated_smoothing_factor):
        config = {"min_replicas": 0, "max_replicas": 100, "target_ongoing_requests": 2}

        if use_deprecated_smoothing_factor:
            config["upscale_smoothing_factor"] = 0.5
        else:
            config["upscaling_factor"] = 0.5

        config = AutoscalingConfig(**config)
        num_replicas = 10

        # Should use upscale smoothing factor of 0.5
        num_ongoing_requests = 8.0 * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        # Should use downscale smoothing factor of 1 (default)
        num_ongoing_requests = 0.25 * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 1 <= desired_num_replicas <= 4  # 10 + (2.5 - 10) = 2.5

    @pytest.mark.parametrize("use_deprecated_smoothing_factor", [True, False])
    def test_downscaling_factor(self, use_deprecated_smoothing_factor):
        config = {"min_replicas": 0, "max_replicas": 100, "target_ongoing_requests": 2}

        if use_deprecated_smoothing_factor:
            config["downscale_smoothing_factor"] = 0.5
        else:
            config["downscaling_factor"] = 0.5

        config = AutoscalingConfig(**config)
        num_replicas = 10

        # Should use upscale smoothing factor of 1 (default)
        num_ongoing_requests = 8.0 * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 39 <= desired_num_replicas <= 41  # 10 + (40 - 10) = 40

        # Should use downscale smoothing factor of 0.5
        num_ongoing_requests = 0.25 * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=num_ongoing_requests,
            num_running_replicas=num_replicas,
        )
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25

    @pytest.mark.parametrize(
        "num_replicas,ratio,scaling_factor",
        [
            # All of the parametrized scenarios should downscale by 1
            # replica. Compare the first theoretical calculation that's
            # with smoothing factor, and the second calculation without
            # smoothing factor. In these cases, downscaling should not
            # be blocked by fractional smoothing factor.
            (2, 0.3, 0.5),  # 2 - 0.5 (2 * 0.7) = 1.3 | 2 - (2 * 0.7) = 0.6
            (5, 0.4, 0.2),  # 5 - 0.2 (5 * 0.6) = 4.4 | 5 - (5 * 0.6) = 2
            (10, 0.4, 0.1),  # 10 - 0.1 (10 * 0.6) = 9.4 | 10 - (10 * 0.6) = 4
        ],
    )
    @pytest.mark.parametrize("use_deprecated_smoothing_factor", [True, False])
    def test_downscaling_with_fractional_scaling_factor(
        self,
        num_replicas: int,
        ratio: float,
        scaling_factor: float,
        use_deprecated_smoothing_factor,
    ):
        config = {"min_replicas": 0, "max_replicas": 100, "target_ongoing_requests": 1}

        if use_deprecated_smoothing_factor:
            config["downscale_smoothing_factor"] = scaling_factor
        else:
            config["downscaling_factor"] = scaling_factor

        config = AutoscalingConfig(**config)
        total_num_requests = ratio * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config,
            total_num_requests=total_num_requests,
            num_running_replicas=num_replicas,
        )
        assert desired_num_replicas == num_replicas - 1


class TestReplicaQueueLengthPolicy:
    @pytest.mark.parametrize(
        "use_upscale_smoothing_factor,use_upscaling_factor",
        [(True, True), (True, False), (False, True)],
    )
    def test_scaling_factor_scale_up_from_0_replicas(
        self, use_upscale_smoothing_factor, use_upscaling_factor
    ):
        """Test that the scaling factor is respected when scaling up
        from 0 replicas.
        """

        min_replicas = 0
        max_replicas = 2
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            upscale_smoothing_factor=10 if use_upscale_smoothing_factor else None,
            upscaling_factor=10 if use_upscaling_factor else None,
        )
        new_num_replicas = replica_queue_length_autoscaling_policy(
            curr_target_num_replicas=0,
            total_num_requests=1,
            num_running_replicas=0,
            config=config,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state={},
        )

        # 1 * 10
        assert new_num_replicas == 10

        if use_upscale_smoothing_factor:
            config.upscale_smoothing_factor = 0.5
        if use_upscaling_factor:
            config.upscaling_factor = 0.5

        new_num_replicas = replica_queue_length_autoscaling_policy(
            curr_target_num_replicas=0,
            total_num_requests=1,
            num_running_replicas=0,
            config=config,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state={},
        )

        # math.ceil(1 * 0.5)
        assert new_num_replicas == 1

    @pytest.mark.parametrize(
        "use_downscale_smoothing_factor,use_downscaling_factor",
        [(True, True), (True, False), (False, True)],
    )
    def test_scaling_factor_scale_down_to_0_replicas(
        self, use_downscale_smoothing_factor, use_downscaling_factor
    ):
        """Test that a deployment scales down to 0 for non-default smoothing factors."""

        # With smoothing factor > 1, the desired number of replicas should
        # immediately drop to 0 (while respecting upscale and downscale delay)
        min_replicas = 0
        max_replicas = 5
        policy_state = {}
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            downscale_smoothing_factor=10 if use_downscale_smoothing_factor else None,
            downscaling_factor=10 if use_downscaling_factor else None,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )
        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=0,
            num_running_replicas=5,
            curr_target_num_replicas=5,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )

        assert new_num_replicas == 0

        # With smoothing factor < 1, the desired number of replicas shouldn't
        # get stuck at a positive number, and instead should eventually drop
        # to zero
        if use_downscale_smoothing_factor:
            config.downscale_smoothing_factor = 0.2
        if use_downscaling_factor:
            config.downscaling_factor = 0.2

        # policy_manager = AutoscalingPolicyManager(config)
        num_replicas = 5
        for _ in range(5):
            num_replicas = replica_queue_length_autoscaling_policy(
                config=config,
                total_num_requests=0,
                num_running_replicas=num_replicas,
                curr_target_num_replicas=num_replicas,
                capacity_adjusted_min_replicas=min_replicas,
                capacity_adjusted_max_replicas=max_replicas,
                policy_state=policy_state,
            )

        assert num_replicas == 0

    def test_upscale_downscale_delay(self):
        """Unit test for upscale_delay_s and downscale_delay_s."""

        upscale_delay_s = 30.0
        downscale_delay_s = 600.0

        min_replicas = 0
        max_replicas = 2
        policy_state = {}
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_ongoing_requests=1,
            upscale_delay_s=30.0,
            downscale_delay_s=600.0,
        )

        upscale_wait_periods = int(upscale_delay_s / CONTROL_LOOP_INTERVAL_S)
        downscale_wait_periods = int(downscale_delay_s / CONTROL_LOOP_INTERVAL_S)

        overload_requests = 100

        # Scale up when there are 0 replicas and current_handle_queued_queries > 0
        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=1,
            num_running_replicas=0,
            curr_target_num_replicas=0,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 1

        # We should scale up only after enough consecutive scale-up decisions.
        for i in range(upscale_wait_periods):
            new_num_replicas = replica_queue_length_autoscaling_policy(
                config=config,
                total_num_requests=overload_requests,
                num_running_replicas=1,
                curr_target_num_replicas=1,
                capacity_adjusted_min_replicas=min_replicas,
                capacity_adjusted_max_replicas=max_replicas,
                policy_state=policy_state,
            )
            assert new_num_replicas == 1, i

        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=overload_requests,
            num_running_replicas=1,
            curr_target_num_replicas=1,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 2

        no_requests = 0

        # We should scale down only after enough consecutive scale-down decisions.
        for i in range(downscale_wait_periods):
            new_num_replicas = replica_queue_length_autoscaling_policy(
                config=config,
                total_num_requests=no_requests,
                num_running_replicas=2,
                curr_target_num_replicas=2,
                capacity_adjusted_min_replicas=min_replicas,
                capacity_adjusted_max_replicas=max_replicas,
                policy_state=policy_state,
            )
            assert new_num_replicas == 2, i

        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=no_requests,
            num_running_replicas=2,
            curr_target_num_replicas=2,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 0

        # Get some scale-up decisions, but not enough to trigger a scale up.
        for i in range(int(upscale_wait_periods / 2)):
            new_num_replicas = replica_queue_length_autoscaling_policy(
                config=config,
                total_num_requests=overload_requests,
                num_running_replicas=1,
                curr_target_num_replicas=1,
                capacity_adjusted_min_replicas=min_replicas,
                capacity_adjusted_max_replicas=max_replicas,
                policy_state=policy_state,
            )
            assert new_num_replicas == 1, i

        # Interrupt with a scale-down decision.
        replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=0,
            num_running_replicas=1,
            curr_target_num_replicas=1,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )

        # The counter should be reset, so it should require `upscale_wait_periods`
        # more periods before we actually scale up.
        for i in range(upscale_wait_periods):
            new_num_replicas = replica_queue_length_autoscaling_policy(
                config=config,
                total_num_requests=overload_requests,
                num_running_replicas=1,
                curr_target_num_replicas=1,
                capacity_adjusted_min_replicas=min_replicas,
                capacity_adjusted_max_replicas=max_replicas,
                policy_state=policy_state,
            )
            assert new_num_replicas == 1, i

        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=overload_requests,
            num_running_replicas=1,
            curr_target_num_replicas=1,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 2

        # Get some scale-down decisions, but not enough to trigger a scale down.
        for i in range(int(downscale_wait_periods / 2)):
            new_num_replicas = replica_queue_length_autoscaling_policy(
                config=config,
                total_num_requests=no_requests,
                num_running_replicas=2,
                curr_target_num_replicas=2,
                capacity_adjusted_min_replicas=min_replicas,
                capacity_adjusted_max_replicas=max_replicas,
                policy_state=policy_state,
            )
            assert new_num_replicas == 2, i

        # Interrupt with a scale-up decision.
        replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=200,
            num_running_replicas=2,
            curr_target_num_replicas=2,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )

        # The counter should be reset so it should require `downscale_wait_periods`
        # more periods before we actually scale down.
        for i in range(downscale_wait_periods):
            new_num_replicas = replica_queue_length_autoscaling_policy(
                config=config,
                total_num_requests=no_requests,
                num_running_replicas=2,
                curr_target_num_replicas=2,
                capacity_adjusted_min_replicas=min_replicas,
                capacity_adjusted_max_replicas=max_replicas,
                policy_state=policy_state,
            )
            assert new_num_replicas == 2, i

        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=no_requests,
            num_running_replicas=2,
            curr_target_num_replicas=2,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 0

    def test_replicas_delayed_startup(self):
        """Unit test simulating replicas taking time to start up."""
        min_replicas = 1
        max_replicas = 200
        policy_state = {}
        config = {
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "upscale_delay_s": 0,
            "downscale_delay_s": 100000,
            "target_ongoing_requests": 1,
        }
        config = AutoscalingConfig(**config)

        # new_num_replicas = policy_manager.get_decision_num_replicas(1, 100, 1)
        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            curr_target_num_replicas=1,
            total_num_requests=100,
            num_running_replicas=1,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 100

        # New target is 100, but no new replicas finished spinning up during this
        # timestep.
        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            curr_target_num_replicas=100,
            total_num_requests=100,
            num_running_replicas=1,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 100

        # Two new replicas spun up during this timestep.
        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            curr_target_num_replicas=100,
            total_num_requests=123,
            num_running_replicas=3,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 123

        # A lot of queries got drained and a lot of replicas started up, but
        # new_num_replicas should not decrease, because of the downscale delay.
        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            curr_target_num_replicas=123,
            total_num_requests=10,
            num_running_replicas=4,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == 123

    @pytest.mark.parametrize("delay_s", [30.0, 0.0])
    def test_fluctuating_ongoing_requests(self, delay_s):
        """
        Simulates a workload that switches between too many and too few
        ongoing requests.
        """

        min_replicas = 1
        max_replicas = 10
        policy_state = {}
        config = {
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "upscale_delay_s": delay_s,
            "downscale_delay_s": delay_s,
            "target_ongoing_requests": 50,
        }
        config = AutoscalingConfig(**config)

        if delay_s > 0:
            wait_periods = int(delay_s / CONTROL_LOOP_INTERVAL_S)
            assert wait_periods > 1

        underload_requests, overload_requests = 2 * 20, 100
        trials = 1000

        new_num_replicas = None
        for trial in range(trials):
            if trial % 2 == 0:
                new_num_replicas = replica_queue_length_autoscaling_policy(
                    config=config,
                    total_num_requests=overload_requests,
                    num_running_replicas=1,
                    curr_target_num_replicas=1,
                    capacity_adjusted_min_replicas=min_replicas,
                    capacity_adjusted_max_replicas=max_replicas,
                    policy_state=policy_state,
                )
                if delay_s > 0:
                    assert new_num_replicas == 1, trial
                else:
                    assert new_num_replicas == 2, trial
            else:
                new_num_replicas = replica_queue_length_autoscaling_policy(
                    config=config,
                    total_num_requests=underload_requests,
                    num_running_replicas=2,
                    curr_target_num_replicas=2,
                    capacity_adjusted_min_replicas=min_replicas,
                    capacity_adjusted_max_replicas=max_replicas,
                    policy_state=policy_state,
                )
                if delay_s > 0:
                    assert new_num_replicas == 2, trial
                else:
                    assert new_num_replicas == 1, trial

    @pytest.mark.parametrize("ongoing_requests", [20, 100, 10])
    def test_single_replica_receives_all_requests(self, ongoing_requests):
        target_requests = 5

        min_replicas = 1
        max_replicas = 50
        policy_state = {}
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_ongoing_requests=target_requests,
            upscale_delay_s=0.0,
            downscale_delay_s=0.0,
        )

        new_num_replicas = replica_queue_length_autoscaling_policy(
            config=config,
            total_num_requests=ongoing_requests,
            num_running_replicas=4,
            curr_target_num_replicas=4,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
        )
        assert new_num_replicas == ongoing_requests / target_requests


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
