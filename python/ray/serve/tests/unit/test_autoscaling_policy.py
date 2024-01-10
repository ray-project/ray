import sys

import numpy as np
import pytest

from ray.serve._private.autoscaling_policy import AutoscalingPolicyManager
from ray.serve._private.constants import CONTROL_LOOP_PERIOD_S
from ray.serve.autoscaling_policy import _calculate_desired_num_replicas
from ray.serve.config import AutoscalingConfig


class TestCalculateDesiredNumReplicas:
    def test_bounds_checking(self):
        num_replicas = 10
        max_replicas = 11
        min_replicas = 9
        config = AutoscalingConfig(
            max_replicas=max_replicas,
            min_replicas=min_replicas,
            target_num_ongoing_requests_per_replica=100,
        )

        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=[150] * num_replicas
        )
        assert desired_num_replicas == max_replicas

        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=[50] * num_replicas
        )
        assert desired_num_replicas == min_replicas

        for i in range(50, 150):
            desired_num_replicas = _calculate_desired_num_replicas(
                autoscaling_config=config,
                current_num_ongoing_requests=[i] * num_replicas,
            )
            assert min_replicas <= desired_num_replicas <= max_replicas

    @pytest.mark.parametrize("target_requests", [0.5, 1.0, 1.5])
    def test_scale_up(self, target_requests):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=target_requests,
        )
        num_replicas = 10
        num_ongoing_requests = [2 * target_requests] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 19 <= desired_num_replicas <= 21  # 10 * 2 = 20

    @pytest.mark.parametrize("target_requests", [0.5, 1.0, 1.5])
    def test_scale_down(self, target_requests):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=target_requests,
        )
        num_replicas = 10
        num_ongoing_requests = [0.5 * target_requests] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 4 <= desired_num_replicas <= 6  # 10 * 0.5 = 5

    def test_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            smoothing_factor=0.5,
        )
        num_replicas = 10

        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25

    def test_upscale_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            upscale_smoothing_factor=0.5,
        )
        num_replicas = 10

        # Should use upscale smoothing factor of 0.5
        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        # Should use downscale smoothing factor of 1 (default)
        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 1 <= desired_num_replicas <= 4  # 10 + (2.5 - 10) = 2.5

    def test_downscale_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            downscale_smoothing_factor=0.5,
        )
        num_replicas = 10

        # Should use upscale smoothing factor of 1 (default)
        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 39 <= desired_num_replicas <= 41  # 10 + (40 - 10) = 40

        # Should use downscale smoothing factor of 0.5
        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = _calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25


class TestGetDecisionNumReplicas:
    def test_smoothing_factor_scale_up_from_0_replicas(self):
        """Test that the smoothing factor is respected when scaling up
        from 0 replicas.
        """

        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=2,
            smoothing_factor=10,
        )
        policy_manager = AutoscalingPolicyManager(config)
        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=[],
            curr_target_num_replicas=0,
            current_handle_queued_queries=1,
            _skip_bound_check=True,
        )

        # 1 * 10
        assert new_num_replicas == 10

        config.smoothing_factor = 0.5
        policy_manager = AutoscalingPolicyManager(config)
        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=[],
            curr_target_num_replicas=0,
            current_handle_queued_queries=1,
            _skip_bound_check=True,
        )

        # math.ceil(1 * 0.5)
        assert new_num_replicas == 1

    def test_smoothing_factor_scale_down_to_0_replicas(self):
        """Test that a deployment scales down to 0 for non-default smoothing factors."""

        # With smoothing factor > 1, the desired number of replicas should
        # immediately drop to 0 (while respecting upscale and downscale delay)
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=5,
            smoothing_factor=10,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )
        policy_manager = AutoscalingPolicyManager(config)
        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=[0, 0, 0, 0, 0],
            curr_target_num_replicas=5,
            current_handle_queued_queries=0,
        )

        assert new_num_replicas == 0

        # With smoothing factor < 1, the desired number of replicas shouldn't
        # get stuck at a positive number, and instead should eventually drop
        # to zero
        config.smoothing_factor = 0.2
        policy_manager = AutoscalingPolicyManager(config)
        num_replicas = 5
        for _ in range(5):
            num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=[0] * num_replicas,
                curr_target_num_replicas=num_replicas,
                current_handle_queued_queries=0,
            )

        assert num_replicas == 0

    def test_upscale_downscale_delay(self):
        """Unit test for upscale_delay_s and downscale_delay_s."""

        upscale_delay_s = 30.0
        downscale_delay_s = 600.0

        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=2,
            target_num_ongoing_requests_per_replica=1,
            upscale_delay_s=30.0,
            downscale_delay_s=600.0,
        )

        policy_manager = AutoscalingPolicyManager(config)

        upscale_wait_periods = int(upscale_delay_s / CONTROL_LOOP_PERIOD_S)
        downscale_wait_periods = int(downscale_delay_s / CONTROL_LOOP_PERIOD_S)

        overload_requests = [100]

        # Scale up when there are 0 replicas and current_handle_queued_queries > 0
        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=[],
            curr_target_num_replicas=0,
            current_handle_queued_queries=1,
        )
        assert new_num_replicas == 1

        # We should scale up only after enough consecutive scale-up decisions.
        for i in range(upscale_wait_periods):
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=overload_requests,
                curr_target_num_replicas=1,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 1, i

        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=overload_requests,
            curr_target_num_replicas=1,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 2

        no_requests = [0, 0]

        # We should scale down only after enough consecutive scale-down decisions.
        for i in range(downscale_wait_periods):
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=no_requests,
                curr_target_num_replicas=2,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 2, i

        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=no_requests,
            curr_target_num_replicas=2,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 0

        # Get some scale-up decisions, but not enough to trigger a scale up.
        for i in range(int(upscale_wait_periods / 2)):
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=overload_requests,
                curr_target_num_replicas=1,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 1, i

        # Interrupt with a scale-down decision.
        policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=[0],
            curr_target_num_replicas=1,
            current_handle_queued_queries=0,
        )

        # The counter should be reset, so it should require `upscale_wait_periods`
        # more periods before we actually scale up.
        for i in range(upscale_wait_periods):
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=overload_requests,
                curr_target_num_replicas=1,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 1, i

        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=overload_requests,
            curr_target_num_replicas=1,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 2

        # Get some scale-down decisions, but not enough to trigger a scale down.
        for i in range(int(downscale_wait_periods / 2)):
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=no_requests,
                curr_target_num_replicas=2,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 2, i

        # Interrupt with a scale-up decision.
        policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=[100, 100],
            curr_target_num_replicas=2,
            current_handle_queued_queries=0,
        )

        # The counter should be reset so it should require `downscale_wait_periods`
        # more periods before we actually scale down.
        for i in range(downscale_wait_periods):
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=no_requests,
                curr_target_num_replicas=2,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 2, i

        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=no_requests,
            curr_target_num_replicas=2,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 0

    def test_replicas_delayed_startup(self):
        """Unit test simulating replicas taking time to start up."""
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=200,
            target_num_ongoing_requests_per_replica=1,
            upscale_delay_s=0,
            downscale_delay_s=100000,
        )

        policy_manager = AutoscalingPolicyManager(config)

        new_num_replicas = policy_manager.get_decision_num_replicas(1, [100], 0)
        assert new_num_replicas == 100

        # New target is 100, but no new replicas finished spinning up during this
        # timestep.
        new_num_replicas = policy_manager.get_decision_num_replicas(100, [100], 0)
        assert new_num_replicas == 100

        # Two new replicas spun up during this timestep.
        new_num_replicas = policy_manager.get_decision_num_replicas(
            100, [100, 20, 3], 0
        )
        assert new_num_replicas == 123

        # A lot of queries got drained and a lot of replicas started up, but
        # new_num_replicas should not decrease, because of the downscale delay.
        new_num_replicas = policy_manager.get_decision_num_replicas(
            123, [6, 2, 1, 1], 0
        )
        assert new_num_replicas == 123

    @pytest.mark.parametrize("delay_s", [30.0, 0.0])
    def test_fluctuating_ongoing_requests(self, delay_s):
        """
        Simulates a workload that switches between too many and too few
        ongoing requests.
        """

        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=10,
            target_num_ongoing_requests_per_replica=50,
            upscale_delay_s=delay_s,
            downscale_delay_s=delay_s,
        )

        policy_manager = AutoscalingPolicyManager(config)

        if delay_s > 0:
            wait_periods = int(delay_s / CONTROL_LOOP_PERIOD_S)
            assert wait_periods > 1

        underload_requests, overload_requests = [20, 20], [100]
        trials = 1000

        new_num_replicas = None
        for trial in range(trials):
            if trial % 2 == 0:
                new_num_replicas = policy_manager.get_decision_num_replicas(
                    current_num_ongoing_requests=overload_requests,
                    curr_target_num_replicas=1,
                    current_handle_queued_queries=0,
                )
                if delay_s > 0:
                    assert new_num_replicas == 1, trial
                else:
                    assert new_num_replicas == 2, trial
            else:
                new_num_replicas = policy_manager.get_decision_num_replicas(
                    current_num_ongoing_requests=underload_requests,
                    curr_target_num_replicas=2,
                    current_handle_queued_queries=0,
                )
                if delay_s > 0:
                    assert new_num_replicas == 2, trial
                else:
                    assert new_num_replicas == 1, trial

    @pytest.mark.parametrize(
        "ongoing_requests", [[7, 1, 8, 4], [8, 1, 8, 4], [6, 1, 8, 4], [0, 1, 8, 4]]
    )
    def test_imbalanced_replicas(self, ongoing_requests):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=10,
            target_num_ongoing_requests_per_replica=5,
            upscale_delay_s=0.0,
            downscale_delay_s=0.0,
        )

        policy_manager = AutoscalingPolicyManager(config)

        # Check that as long as the average number of ongoing requests equals
        # the target_num_ongoing_requests_per_replica, the number of replicas
        # stays the same
        if np.mean(ongoing_requests) == config.target_num_ongoing_requests_per_replica:
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=ongoing_requests,
                curr_target_num_replicas=4,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 4

        # Check downscaling behavior when average number of requests
        # is lower than target_num_ongoing_requests_per_replica
        elif np.mean(ongoing_requests) < config.target_num_ongoing_requests_per_replica:
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=ongoing_requests,
                curr_target_num_replicas=4,
                current_handle_queued_queries=0,
            )

            if (
                config.target_num_ongoing_requests_per_replica
                - np.mean(ongoing_requests)
                <= 1
            ):
                # Autoscaling uses a ceiling operator, which means a slightly low
                # current_num_ongoing_requests value is insufficient to downscale
                assert new_num_replicas == 4
            else:
                assert new_num_replicas == 3

        # Check upscaling behavior when average number of requests
        # is higher than target_num_ongoing_requests_per_replica
        else:
            new_num_replicas = policy_manager.get_decision_num_replicas(
                current_num_ongoing_requests=ongoing_requests,
                curr_target_num_replicas=4,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 5

    @pytest.mark.parametrize(
        "ongoing_requests", [[20, 0, 0, 0], [100, 0, 0, 0], [10, 0, 0, 0]]
    )
    def test_single_replica_receives_all_requests(self, ongoing_requests):
        target_requests = 5

        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=50,
            target_num_ongoing_requests_per_replica=target_requests,
            upscale_delay_s=0.0,
            downscale_delay_s=0.0,
        )

        policy_manager = AutoscalingPolicyManager(config)

        new_num_replicas = policy_manager.get_decision_num_replicas(
            current_num_ongoing_requests=ongoing_requests,
            curr_target_num_replicas=4,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == sum(ongoing_requests) / target_requests


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
