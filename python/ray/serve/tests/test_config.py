import pytest
from pydantic import ValidationError

from ray.serve.config import (DeploymentConfig, DeploymentMode, HTTPOptions,
                              ReplicaConfig)


def test_deployment_config_validation():
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


def test_deployment_config_update():
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


def test_replica_config_validation():
    class Class:
        pass

    def function(_):
        pass

    ReplicaConfig(Class)
    ReplicaConfig(function)
    with pytest.raises(TypeError):
        ReplicaConfig(Class())

    # Check ray_actor_options validation.
    ReplicaConfig(
        Class,
        ray_actor_options={
            "num_cpus": 1.0,
            "num_gpus": 10,
            "resources": {
                "abc": 1.0
            },
            "memory": 1000000.0,
            "object_store_memory": 1000000,
        })
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options=1.0)
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options=False)
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"num_cpus": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"num_cpus": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"num_gpus": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"num_gpus": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"memory": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"memory": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(
            Class, ray_actor_options={"object_store_memory": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"object_store_memory": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"resources": None})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"name": None})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"lifetime": None})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"max_restarts": None})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"placement_group": None})


def test_http_options():
    HTTPOptions()
    HTTPOptions(host="8.8.8.8", middlewares=[object()])
    assert HTTPOptions(host=None).location == "NoServer"
    assert HTTPOptions(location=None).location == "NoServer"
    assert HTTPOptions(
        location=DeploymentMode.EveryNode).location == "EveryNode"


def test_with_proto():
    # Test roundtrip
    config = DeploymentConfig(num_replicas=100, max_concurrent_queries=16)
    assert config == DeploymentConfig.from_proto_bytes(config.to_proto_bytes())

    # Test user_config object
    config = DeploymentConfig(user_config={"python": ("native", ["objects"])})
    assert config == DeploymentConfig.from_proto_bytes(config.to_proto_bytes())


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
