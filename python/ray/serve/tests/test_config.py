import pytest

from ray.serve.config import (BackendConfig, DeploymentMode, HTTPOptions,
                              ReplicaConfig)
from pydantic import ValidationError


def test_backend_config_validation():
    # Test unknown key.
    with pytest.raises(ValidationError):
        BackendConfig(unknown_key=-1)

    # Test num_replicas validation.
    BackendConfig(num_replicas=1)
    with pytest.raises(ValidationError, match="type_error"):
        BackendConfig(num_replicas="hello")
    with pytest.raises(ValidationError, match="value_error"):
        BackendConfig(num_replicas=-1)

    # Test dynamic default for max_concurrent_queries.
    assert BackendConfig().max_concurrent_queries == 100


def test_backend_config_update():
    b = BackendConfig(num_replicas=1, max_concurrent_queries=1)

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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
