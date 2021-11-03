import pytest
from pydantic import ValidationError

from ray.serve.config import (AutoscalingConfig, DeploymentConfig,
                              DeploymentMode, HTTPOptions, ReplicaConfig,
                              SerializedFuncOrClass)


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


class TestClass:
    pass


def test_function():
    pass


serialized_class = SerializedFuncOrClass(TestClass)
serialized_function = SerializedFuncOrClass(test_function)


class TestReplicaConfig:
    def test_func_or_class_validation(self):
        r1 = ReplicaConfig(func_or_class=TestClass)
        assert not r1.serialized_func_or_class.is_function
        r1.set_func_or_class(test_function)
        assert r1.serialized_func_or_class.is_function
        r2 = ReplicaConfig(func_or_class=test_function)
        assert r2.serialized_func_or_class.is_function
        r2.set_func_or_class(TestClass)
        assert not r2.serialized_func_or_class.is_function
        r3 = ReplicaConfig(func_or_class=serialized_class)
        assert not r3.serialized_func_or_class.is_function
        r3.set_func_or_class(serialized_function)
        assert r3.serialized_func_or_class.is_function
        r4 = ReplicaConfig(func_or_class=serialized_function)
        assert r4.serialized_func_or_class.is_function
        r4.set_func_or_class(serialized_class)
        assert not r4.serialized_func_or_class.is_function

        with pytest.raises(TypeError):
            ReplicaConfig(func_or_class=TestClass())

    def test_ray_actor_options_conversion(self):
        r = ReplicaConfig(func_or_class=TestClass)
        r.set_ray_actor_options({
            "num_cpus": 1.0,
            "num_gpus": 10,
            "resources": {
                "abc": 1.0
            },
            "memory": 1000000.0,
            "object_store_memory": 1000000,
        })
        assert r.num_cpus == 1.0
        assert r.num_gpus == 10.0
        assert r.resources == {
            "abc": 1.0,
            "memory": 1000000.0,
            "object_store_memory": 1000000
        }

    def test_ray_actor_options_invalid_types(self):
        r = ReplicaConfig(func_or_class=TestClass)
        with pytest.raises(TypeError):
            r.set_ray_actor_options(1.0)
        with pytest.raises(TypeError):
            r.set_ray_actor_options({"num_cpus": "hello"})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"num_cpus": -1})
        with pytest.raises(TypeError):
            r.set_ray_actor_options({"num_gpus": "hello"})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"num_gpus": -1})
        with pytest.raises(TypeError):
            r.set_ray_actor_options({"memory": "hello"})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"memory": -1})
        with pytest.raises(TypeError):
            r.set_ray_actor_options({"object_store_memory": "hello"})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"object_store_memory": -1})
        with pytest.raises(TypeError):
            r.set_ray_actor_options({"resources": None})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"name": None})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"lifetime": None})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"max_restarts": None})
        with pytest.raises(ValueError):
            r.set_ray_actor_options({"placement_group": None})

    def test_init_args(self):
        r1 = ReplicaConfig(func_or_class=TestClass, init_args=("hi", 123))
        assert r1.init_args == ("hi", 123)
        r1.set_init_args(tuple())
        assert r1.init_args == tuple()

        with pytest.raises(TypeError, match="tuple"):
            ReplicaConfig(func_or_class=TestClass, init_args={})

        with pytest.raises(ValueError, match="function deployments"):
            ReplicaConfig(func_or_class=test_function, init_args=("hi", 123))

        with pytest.raises(ValueError, match="function deployments"):
            ReplicaConfig(
                func_or_class=serialized_function, init_args=("hi", 123))

    def test_init_kwargs(self):
        r1 = ReplicaConfig(func_or_class=TestClass, init_kwargs={"hi": 123})
        assert r1.init_kwargs == {"hi": 123}
        r1.set_init_kwargs(dict())
        assert r1.init_kwargs == dict()

        with pytest.raises(TypeError, match="dict"):
            ReplicaConfig(func_or_class=TestClass, init_kwargs=tuple())

        with pytest.raises(ValueError, match="function deployments"):
            ReplicaConfig(func_or_class=test_function, init_kwargs={"hi": 123})

        with pytest.raises(ValueError, match="function deployments"):
            ReplicaConfig(
                func_or_class=serialized_function, init_args={"hi": 123})

    def test_num_cpus(self):
        r = ReplicaConfig(func_or_class=TestClass)
        assert r.num_cpus == 1.0
        r.set_num_cpus(2)
        assert r.num_cpus == 2.0
        r.set_num_cpus(2.0)
        assert r.num_cpus == 2.0
        with pytest.raises(TypeError):
            r.set_num_cpus("1")
        with pytest.raises(ValueError):
            r.set_num_cpus(-0.1)

    def test_num_gpus(self):
        r = ReplicaConfig(func_or_class=TestClass)
        assert r.num_gpus == 0.0
        r.set_num_gpus(2)
        assert r.num_gpus == 2.0
        r.set_num_gpus(2.0)
        assert r.num_gpus == 2.0
        with pytest.raises(TypeError):
            r.set_num_gpus("1")
        with pytest.raises(ValueError):
            r.set_num_gpus(-0.1)

    def test_resources(self):
        r = ReplicaConfig(func_or_class=TestClass, resources={"test_key": 0})
        assert r.resources == {"test_key": 0}
        r.set_resources({"test_key": 1})
        assert r.resources == {"test_key": 1}
        r.set_resources({"test_key": 0.1})
        assert r.resources == {"test_key": 0.1}
        r.set_resources({"test_key": 1.0})
        assert r.resources == {"test_key": 1.0}

        with pytest.raises(TypeError, match="keys must be strings"):
            r.set_resources({123: 1})

        with pytest.raises(TypeError, match="values must be ints or floats"):
            r.set_resources({"test_key": "abc"})

        with pytest.raises(ValueError, match="must be >= 0"):
            r.set_resources({"test_key": -1})

        with pytest.raises(
                ValueError, match="use num_cpus_per_replica instead"):
            r.set_resources({"CPU": 1})

        with pytest.raises(
                ValueError, match="use num_gpus_per_replica instead"):
            r.set_resources({"GPU": 1})

    def test_accelerator_type(self):
        r = ReplicaConfig(func_or_class=TestClass, accelerator_type="test")
        assert r.accelerator_type == "test"
        r.set_accelerator_type("test2")
        assert r.accelerator_type == "test2"

        with pytest.raises(TypeError):
            r.set_accelerator_type(1)

    def test_runtime_env(self):
        r = ReplicaConfig(
            func_or_class=TestClass, runtime_env={"pip": ["requests"]})
        assert r.runtime_env == {"pip": ["requests"]}
        r.set_runtime_env(runtime_env={"uris": ["fake_uri"]})
        assert r.runtime_env == {"uris": ["fake_uri"]}

    def test_override_runtime_env(self):
        # Should pick up parent env if nothing was specified.
        r1 = ReplicaConfig(func_or_class=TestClass)
        r1.override_runtime_env({"pip": ["requests"], "uris": ["fake_uri"]})
        assert r1.runtime_env == {"pip": ["requests"], "uris": ["fake_uri"]}

        # Should pick up URIs from parent env if not specified, even if other
        # options were.
        r2 = ReplicaConfig(
            func_or_class=TestClass, runtime_env={"pip": ["requests"]})
        r2.override_runtime_env({
            "pip": ["requests2"],
            "conda": {},
            "uris": ["fake_uri"]
        })
        assert r2.runtime_env == {"pip": ["requests"], "uris": ["fake_uri"]}

        # working_dir should be stripped from parent env.
        r2 = ReplicaConfig(func_or_class=TestClass)
        r2.override_runtime_env({"working_dir": ".", "uris": ["fake_uri"]})
        assert r2.runtime_env == {"uris": ["fake_uri"]}

    def test_get_resource_dict(self):
        r = ReplicaConfig(func_or_class=TestClass)
        assert r.get_resource_dict() == {"CPU": 1.0}

        r.set_num_cpus(0.0)
        assert r.get_resource_dict() == {}

        r.set_num_cpus(2.0)
        assert r.get_resource_dict() == {"CPU": 2.0}

        r.set_num_gpus(1.0)
        assert r.get_resource_dict() == {"CPU": 2.0, "GPU": 1.0}

        r.set_resources({"FAKE1": 1.0, "FAKE2": 2.0})
        assert r.get_resource_dict() == {
            "CPU": 2.0,
            "GPU": 1.0,
            "FAKE1": 1.0,
            "FAKE2": 2.0
        }

        r.set_num_cpus(0.0)
        r.set_num_gpus(0.0)
        assert r.get_resource_dict() == {"FAKE1": 1.0, "FAKE2": 2.0}

        r.set_resources({})
        assert r.get_resource_dict() == {}


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


def test_zero_default_proto():
    # Test that options set to zero (protobuf default value) still retain their
    # original value after being serialized and deserialized.
    config = DeploymentConfig(
        autoscaling_config={
            "min_replicas": 1,
            "max_replicas": 2,
            "smoothing_factor": 0.123,
            "downscale_delay_s": 0
        })
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
