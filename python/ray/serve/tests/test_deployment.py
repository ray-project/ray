import sys
import pytest


class TestDeploymentOptions:

    deployment_options = [
        "func_or_class",
        "name",
        "version",
        "num_replicas",
        "init_args",
        "init_kwargs",
        "route_prefix",
        "ray_actor_options",
        "user_config",
        "max_concurrent_queries",
        "autoscaling_config",
        "graceful_shutdown_wait_loop_s",
        "graceful_shutdown_timeout_s",
        "health_check_period_s",
        "health_check_timeout_s",
    ]

    # Options that can be None
    nullable_options = [
        "num_replicas",
        "route_prefix",
        "autoscaling_config",
        "user_config",
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
