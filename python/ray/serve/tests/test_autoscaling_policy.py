import random

from ray.serve.config import AutoscalingConfig
from ray.serve.autoscaling_policy import calculate_desired_num_replicas


class TestCalculateDesiredNumReplicas:
    def test_bounds_checking(self):
        num_replicas = 10
        max_replicas = 11
        min_replicas = 9
        config = AutoscalingConfig(
            max_replicas=max_replicas,
            min_replicas=min_replicas,
            target_num_ongoing_requests_per_replica=100)

        for i in range(100):
            in_flight_requests = [
                random.uniform(0, 200) for i in range(num_replicas)
            ]
            desired_num_replicas = calculate_desired_num_replicas(
                config, in_flight_requests)
            print(
                int(sum(in_flight_requests) / len(in_flight_requests)),
                desired_num_replicas)
            assert min_replicas <= desired_num_replicas <= max_replicas

        # Check that our random testing hits the bounds with high probability.
        # (It falls between the bounds, and therefore spuriously passes, with
        # probability (0.9)^100 = 0.003%.)
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=[110] * num_replicas)
        assert desired_num_replicas == max_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=[90] * num_replicas)
        assert desired_num_replicas == min_replicas

    def test_scale_up(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1)
        num_replicas = 10
        num_ongoing_requests = [2.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests)
        assert 19 <= desired_num_replicas <= 21  # 10 * 2 = 20

    def test_scale_down(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1)
        num_replicas = 10
        num_ongoing_requests = [0.5] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests)
        assert 4 <= desired_num_replicas <= 6  # 10 * 0.5 = 5

    def test_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            smoothing_factor=0.5)
        num_replicas = 10

        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests,
            current_num_replicas=num_replicas)
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests,
            current_num_replicas=num_replicas)
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25
