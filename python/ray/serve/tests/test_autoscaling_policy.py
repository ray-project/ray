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

        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=[150] * num_replicas)
        assert desired_num_replicas == max_replicas

        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=[50] * num_replicas)
        assert desired_num_replicas == min_replicas

        for i in range(50, 150):
            desired_num_replicas = calculate_desired_num_replicas(
                autoscaling_config=config,
                current_num_ongoing_requests=[i] * num_replicas)
            assert min_replicas <= desired_num_replicas <= max_replicas

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
            current_num_ongoing_requests=num_ongoing_requests)
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests)
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25
