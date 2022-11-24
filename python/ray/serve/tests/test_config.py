import pytest
from pydantic import ValidationError

from ray import cloudpickle

from ray.serve.config import (
    DeploymentConfig,
    DeploymentMode,
    HTTPOptions,
    ReplicaConfig,
)
from ray.serve.config import AutoscalingConfig
from ray.serve._private.utils import DEFAULT


def test_autoscaling_config_validation():
    # Check validation over publicly exposed options

    with pytest.raises(ValidationError):
        # min_replicas must be nonnegative
        AutoscalingConfig(min_replicas=-1)

    with pytest.raises(ValidationError):
        # max_replicas must be positive
        AutoscalingConfig(max_replicas=0)

    with pytest.raises(ValidationError):
        # max_replicas must be nonnegative
        AutoscalingConfig(target_num_ongoing_requests_per_replica=-1)

    with pytest.raises(ValueError):
        # max_replicas must be greater than or equal to min_replicas
        AutoscalingConfig(min_replicas=100, max_replicas=1)

    # Default values should not raise an error
    AutoscalingConfig()


class TestDeploymentConfig:
    def test_deployment_config_validation(self):
        # Test unknown key.
        with pytest.raises(ValidationError):
            DeploymentConfig(unknown_key=-1)

        # Test num_replicas validation.
        DeploymentConfig(num_replicas=1)
        with pytest.raises(ValidationError, match="type_error"):
            DeploymentConfig(num_replicas="hello")
        with pytest.raises(ValidationError, match="value_error"):
            DeploymentConfig(num_replicas=-1)

        # Test dynamic default for max_concurrent_queries.
        assert DeploymentConfig().max_concurrent_queries == 100

    def test_deployment_config_update(self):
        b = DeploymentConfig(num_replicas=1, max_concurrent_queries=1)

        # Test updating a key works.
        b.num_replicas = 2
        assert b.num_replicas == 2
        # Check that not specifying a key doesn't update it.
        assert b.max_concurrent_queries == 1

        # Check that input is validated.
        with pytest.raises(ValidationError):
            b.num_replicas = "Hello"
        with pytest.raises(ValidationError):
            b.num_replicas = -1

    def test_from_default(self):
        """Check from_default() method behavior."""

        # Valid parameters
        dc = DeploymentConfig.from_default(num_replicas=5, is_cross_language=True)
        assert dc.num_replicas == 5
        assert dc.is_cross_language is True

        # Invalid parameters should raise TypeError
        with pytest.raises(TypeError):
            DeploymentConfig.from_default(num_replicas=5, is_xlang=True)

        # Validation should still be performed
        with pytest.raises(ValidationError):
            DeploymentConfig.from_default(num_replicas="hello world")

    def test_from_default_ignore_default(self):
        """Check that from_default() ignores DEFAULT.VALUE kwargs."""

        default = DeploymentConfig()

        # Valid parameter with DEFAULT.VALUE passed in should be ignored
        dc = DeploymentConfig.from_default(num_replicas=DEFAULT.VALUE)

        # Validators should run no matter what
        dc = DeploymentConfig.from_default(max_concurrent_queries=None)
        assert dc.max_concurrent_queries is not None
        assert dc.max_concurrent_queries == default.max_concurrent_queries


class TestReplicaConfig:
    def test_replica_config_validation(self):
        class Class:
            pass

        def function(_):
            pass

        ReplicaConfig.create(Class)
        ReplicaConfig.create(function)
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class())

        # Check ray_actor_options validation.
        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            ray_actor_options={
                "num_cpus": 1.0,
                "num_gpus": 10,
                "resources": {"abc": 1.0},
                "memory": 1000000.0,
                "object_store_memory": 1000000,
            },
        )
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class, ray_actor_options=1.0)
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class, ray_actor_options=False)
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class, ray_actor_options={"num_cpus": "hello"})
        with pytest.raises(ValueError):
            ReplicaConfig.create(Class, ray_actor_options={"num_cpus": -1})
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class, ray_actor_options={"num_gpus": "hello"})
        with pytest.raises(ValueError):
            ReplicaConfig.create(Class, ray_actor_options={"num_gpus": -1})
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class, ray_actor_options={"memory": "hello"})
        with pytest.raises(ValueError):
            ReplicaConfig.create(Class, ray_actor_options={"memory": -1})
        with pytest.raises(TypeError):
            ReplicaConfig.create(
                Class, ray_actor_options={"object_store_memory": "hello"}
            )
        with pytest.raises(ValueError):
            ReplicaConfig.create(Class, ray_actor_options={"object_store_memory": -1})
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class, ray_actor_options={"resources": []})

        disallowed_ray_actor_options = {
            "max_concurrency",
            "max_restarts",
            "max_task_retries",
            "name",
            "namespace",
            "lifetime",
            "placement_group",
            "placement_group_bundle_index",
            "placement_group_capture_child_tasks",
            "max_pending_calls",
            "scheduling_strategy",
            "get_if_exists",
            "_metadata",
        }

        for option in disallowed_ray_actor_options:
            with pytest.raises(ValueError):
                ReplicaConfig.create(Class, ray_actor_options={option: None})

    def test_replica_config_lazy_deserialization(self):
        def f():
            return "Check this out!"

        f_serialized = cloudpickle.dumps(f)
        config = ReplicaConfig(
            "f", f_serialized, cloudpickle.dumps(()), cloudpickle.dumps({}), {}
        )

        assert config.serialized_deployment_def == f_serialized
        assert config._deployment_def is None

        assert config.serialized_init_args == cloudpickle.dumps(tuple())
        assert config._init_args is None

        assert config.serialized_init_kwargs == cloudpickle.dumps(dict())
        assert config._init_kwargs is None

        assert isinstance(config.ray_actor_options, dict)
        assert isinstance(config.resource_dict, dict)

        assert config.deployment_def() == "Check this out!"
        assert config.init_args == tuple()
        assert config.init_kwargs == dict()


def test_http_options():
    HTTPOptions()
    HTTPOptions(host="8.8.8.8", middlewares=[object()])
    assert HTTPOptions(host=None).location == "NoServer"
    assert HTTPOptions(location=None).location == "NoServer"
    assert HTTPOptions(location=DeploymentMode.EveryNode).location == "EveryNode"


def test_with_proto():
    # Test roundtrip
    config = DeploymentConfig(num_replicas=100, max_concurrent_queries=16)
    assert config == DeploymentConfig.from_proto_bytes(config.to_proto_bytes())

    # Test user_config object
    config = DeploymentConfig(user_config={"python": ("native", ["objects"])})
    assert config == DeploymentConfig.from_proto_bytes(config.to_proto_bytes())


def test_zero_default_proto():
    # Test that options set to zero (protobuf default value) still retain their
    # original value after being serialized and deserialized.
    config = DeploymentConfig(
        autoscaling_config={
            "min_replicas": 1,
            "max_replicas": 2,
            "smoothing_factor": 0.123,
            "downscale_delay_s": 0,
        }
    )
    serialized_config = config.to_proto_bytes()
    deserialized_config = DeploymentConfig.from_proto_bytes(serialized_config)
    new_delay_s = deserialized_config.autoscaling_config.downscale_delay_s
    assert new_delay_s == 0

    # Check that this test is not spuriously passing.
    default_downscale_delay_s = AutoscalingConfig().downscale_delay_s
    assert new_delay_s != default_downscale_delay_s


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
