import pytest

import ray
from ray.serve._private.deployment_state import DeploymentVersion
from ray.serve.config import DeploymentConfig


def test_validation():
    # Code version must be a string.
    with pytest.raises(TypeError):
        DeploymentVersion(123, DeploymentConfig(), {})


def test_other_type_equality():
    v = DeploymentVersion("1", DeploymentConfig(), {})

    assert v is not None
    assert v != "1"
    assert v != None  # noqa: E711


def test_code_version():
    v1 = DeploymentVersion("1", DeploymentConfig(), {})
    v2 = DeploymentVersion("1", DeploymentConfig(), {})
    v3 = DeploymentVersion("2", DeploymentConfig(), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_deployment_config_basic():
    v1 = DeploymentVersion("1", DeploymentConfig(user_config="1"), {})
    v2 = DeploymentVersion("1", DeploymentConfig(user_config="1"), {})
    v3 = DeploymentVersion("1", DeploymentConfig(user_config="2"), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_user_config_hashable():
    v1 = DeploymentVersion("1", DeploymentConfig(user_config=("1", "2")), {})
    v2 = DeploymentVersion("1", DeploymentConfig(user_config=("1", "2")), {})
    v3 = DeploymentVersion("1", DeploymentConfig(user_config=("1", "3")), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_user_config_list():
    v1 = DeploymentVersion("1", DeploymentConfig(user_config=["1", "2"]), {})
    v2 = DeploymentVersion("1", DeploymentConfig(user_config=["1", "2"]), {})
    v3 = DeploymentVersion("1", DeploymentConfig(user_config=["1", "3"]), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_user_config_dict_keys():
    v1 = DeploymentVersion("1", DeploymentConfig(user_config={"1": "1"}), {})
    v2 = DeploymentVersion("1", DeploymentConfig(user_config={"1": "1"}), {})
    v3 = DeploymentVersion("1", DeploymentConfig(user_config={"2": "1"}), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_user_config_dict_vals():
    v1 = DeploymentVersion("1", DeploymentConfig(user_config={"1": "1"}), {})
    v2 = DeploymentVersion("1", DeploymentConfig(user_config={"1": "1"}), {})
    v3 = DeploymentVersion("1", DeploymentConfig(user_config={"1": "2"}), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_user_config_nested():
    v1 = DeploymentVersion(
        "1", DeploymentConfig(user_config=[{"1": "2"}, {"1": "2"}]), {}
    )
    v2 = DeploymentVersion(
        "1", DeploymentConfig(user_config=[{"1": "2"}, {"1": "2"}]), {}
    )
    v3 = DeploymentVersion(
        "1", DeploymentConfig(user_config=[{"1": "2"}, {"1": "3"}]), {}
    )

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_user_config_nested_in_hashable():
    v1 = DeploymentVersion(
        "1", DeploymentConfig(user_config=([{"1": "2"}, {"1": "2"}])), {}
    )
    v2 = DeploymentVersion(
        "1", DeploymentConfig(user_config=([{"1": "2"}, {"1": "2"}])), {}
    )
    v3 = DeploymentVersion(
        "1", DeploymentConfig(user_config=([{"1": "2"}, {"1": "3"}])), {}
    )

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_num_replicas():
    v1 = DeploymentVersion("1", DeploymentConfig(num_replicas=1), {})
    v2 = DeploymentVersion("1", DeploymentConfig(num_replicas=2), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)


def test_autoscaling_config():
    v1 = DeploymentVersion(
        "1", DeploymentConfig(autoscaling_config={"max_replicas": 2}), {}
    )
    v2 = DeploymentVersion(
        "1", DeploymentConfig(autoscaling_config={"max_replicas": 5}), {}
    )

    assert v1 == v2
    assert hash(v1) == hash(v2)


def test_max_concurrent_queries():
    v1 = DeploymentVersion("1", DeploymentConfig(max_concurrent_queries=5), {})
    v2 = DeploymentVersion("1", DeploymentConfig(max_concurrent_queries=5), {})
    v3 = DeploymentVersion("1", DeploymentConfig(max_concurrent_queries=10), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_health_check_period_s():
    v1 = DeploymentVersion("1", DeploymentConfig(health_check_period_s=5), {})
    v2 = DeploymentVersion("1", DeploymentConfig(health_check_period_s=5), {})
    v3 = DeploymentVersion("1", DeploymentConfig(health_check_period_s=10), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_health_check_timeout_s():
    v1 = DeploymentVersion("1", DeploymentConfig(health_check_timeout_s=5), {})
    v2 = DeploymentVersion("1", DeploymentConfig(health_check_timeout_s=5), {})
    v3 = DeploymentVersion("1", DeploymentConfig(health_check_timeout_s=10), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_graceful_shutdown_timeout_s():
    v1 = DeploymentVersion("1", DeploymentConfig(graceful_shutdown_timeout_s=5), {})
    v2 = DeploymentVersion("1", DeploymentConfig(graceful_shutdown_timeout_s=5), {})
    v3 = DeploymentVersion("1", DeploymentConfig(graceful_shutdown_timeout_s=10), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_graceful_shutdown_wait_loop_s():
    v1 = DeploymentVersion("1", DeploymentConfig(graceful_shutdown_wait_loop_s=5), {})
    v2 = DeploymentVersion("1", DeploymentConfig(graceful_shutdown_wait_loop_s=5), {})
    v3 = DeploymentVersion("1", DeploymentConfig(graceful_shutdown_wait_loop_s=10), {})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_ray_actor_options():
    v1 = DeploymentVersion("1", DeploymentConfig(), {"num_cpus": 0.1})
    v2 = DeploymentVersion("1", DeploymentConfig(), {"num_cpus": 0.1})
    v3 = DeploymentVersion("1", DeploymentConfig(), {"num_gpus": 0.1})

    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert v1 != v3
    assert hash(v1) != hash(v3)


def test_hash_consistent_across_processes(serve_instance):
    @ray.remote
    def get_version():
        return DeploymentVersion(
            "1",
            DeploymentConfig(user_config=([{"1": "2"}, {"1": "2"}],)),
            {},
        )

    assert len(set(ray.get([get_version.remote() for _ in range(100)]))) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
