import pytest
from pydantic import ValidationError

from ray import cloudpickle

from ray.serve.config import (
    DeploymentConfig,
    DeploymentMode,
    HTTPOptions,
    ReplicaConfig,
    gRPCOptions,
)
from ray.serve.config import AutoscalingConfig
from ray.serve._private.utils import DEFAULT
from ray.serve.generated.serve_pb2_grpc import add_UserDefinedServiceServicer_to_server
from ray.serve._private.constants import DEFAULT_GRPC_PORT
from ray.serve.schema import (
    ServeDeploySchema,
    HTTPOptionsSchema,
    ServeApplicationSchema,
    DeploymentSchema,
)


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

    # max_replicas must be greater than or equal to min_replicas
    with pytest.raises(ValueError):
        AutoscalingConfig(min_replicas=100, max_replicas=1)
    AutoscalingConfig(min_replicas=1, max_replicas=100)
    AutoscalingConfig(min_replicas=10, max_replicas=10)

    # initial_replicas must be greater than or equal to min_replicas
    with pytest.raises(ValueError):
        AutoscalingConfig(min_replicas=10, initial_replicas=1)
    with pytest.raises(ValueError):
        AutoscalingConfig(min_replicas=10, initial_replicas=1, max_replicas=15)
    AutoscalingConfig(min_replicas=5, initial_replicas=10, max_replicas=15)
    AutoscalingConfig(min_replicas=5, initial_replicas=5, max_replicas=15)

    # initial_replicas must be less than or equal to max_replicas
    with pytest.raises(ValueError):
        AutoscalingConfig(initial_replicas=10, max_replicas=8)
    with pytest.raises(ValueError):
        AutoscalingConfig(min_replicas=1, initial_replicas=10, max_replicas=8)
    AutoscalingConfig(min_replicas=1, initial_replicas=4, max_replicas=5)
    AutoscalingConfig(min_replicas=1, initial_replicas=5, max_replicas=5)

    # Default values should not raise an error
    AutoscalingConfig()


class TestDeploymentConfig:
    def test_deployment_config_validation(self):
        # Test config ignoring unknown keys (required for forward-compatibility)
        DeploymentConfig(new_version_key=-1)

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
    def test_basic_validation(self):
        class Class:
            pass

        def function(_):
            pass

        ReplicaConfig.create(Class)
        ReplicaConfig.create(function)
        with pytest.raises(TypeError):
            ReplicaConfig.create(Class())

    def test_ray_actor_options_validation(self):
        class Class:
            pass

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

    def test_placement_group_options_validation(self):
        class Class:
            pass

        # Specify placement_group_bundles without num_cpus or placement_group_strategy.
        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            placement_group_bundles=[{"CPU": 1.0}],
        )

        # Specify placement_group_bundles with integer value.
        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            placement_group_bundles=[{"CPU": 1}],
        )

        # Specify placement_group_bundles and placement_group_strategy.
        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            placement_group_bundles=[{"CPU": 1.0}],
            placement_group_strategy="STRICT_PACK",
        )

        # Specify placement_group_bundles and placement_group_strategy and num_cpus.
        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            ray_actor_options={"num_cpus": 1},
            placement_group_bundles=[{"CPU": 1.0}],
            placement_group_strategy="STRICT_PACK",
        )

        # Invalid: placement_group_strategy without placement_group_bundles.
        with pytest.raises(
            ValueError, match="`placement_group_bundles` must also be provided"
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                placement_group_strategy="PACK",
            )

        # Invalid: unsupported placement_group_strategy.
        with pytest.raises(
            ValueError, match="Invalid placement group strategy 'FAKE_NEWS'"
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                placement_group_bundles=[{"CPU": 1.0}],
                placement_group_strategy="FAKE_NEWS",
            )

        # Invalid: malformed placement_group_bundles.
        with pytest.raises(
            ValueError,
            match=(
                "`placement_group_bundles` must be a non-empty list "
                "of resource dictionaries."
            ),
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                placement_group_bundles=[{"CPU": "1.0"}],
            )

        # Invalid: replica actor does not fit in the first bundle (CPU).
        with pytest.raises(
            ValueError,
            match=(
                "the resource requirements for the actor must be a "
                "subset of the first bundle."
            ),
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                ray_actor_options={"num_cpus": 1},
                placement_group_bundles=[{"CPU": 0.1}],
            )

        # Invalid: replica actor does not fit in the first bundle (CPU).
        with pytest.raises(
            ValueError,
            match=(
                "the resource requirements for the actor must be a "
                "subset of the first bundle."
            ),
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                ray_actor_options={"num_gpus": 1},
                placement_group_bundles=[{"CPU": 1.0}],
            )

        # Invalid: replica actor does not fit in the first bundle (custom resource).
        with pytest.raises(
            ValueError,
            match=(
                "the resource requirements for the actor must be a "
                "subset of the first bundle."
            ),
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                ray_actor_options={"resources": {"custom": 1}},
                placement_group_bundles=[{"CPU": 1}],
            )

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


def test_config_schemas_forward_compatible():
    # Test configs ignoring unknown keys (required for forward-compatibility)
    ServeDeploySchema(
        http_options=HTTPOptionsSchema(
            new_version_config_key="this config is from newer version of Ray"
        ),
        applications=[
            ServeApplicationSchema(
                import_path="module.app",
                deployments=[
                    DeploymentSchema(
                        name="deployment",
                        new_version_config_key="this config is from newer version of Ray",
                    )
                ],
                new_version_config_key="this config is from newer version of Ray",
            ),
        ],
        new_version_config_key="this config is from newer version of Ray",
    )


def test_http_options():
    HTTPOptions()
    HTTPOptions(host="8.8.8.8", middlewares=[object()])

    # Test configs ignoring unknown keys (required for forward-compatibility)
    HTTPOptions(new_version_config_key="this config is from newer version of Ray")

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


def test_grpc_options():
    """Test gRPCOptions.

    When the gRPCOptions object is created, the default values are set correctly. When
    the gRPCOptions object is created with user-specified values, the values are set
    correctly. Also if the user provided an invalid grpc_servicer_function, it does not
    raise an error.
    """
    default_grpc_options = gRPCOptions()
    assert default_grpc_options.port == DEFAULT_GRPC_PORT
    assert default_grpc_options.grpc_servicer_functions == []
    assert default_grpc_options.grpc_servicer_func_callable == []

    port = 9001
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "fake.service.that.does.not.exist",  # Import not found, ignore.
        "ray.serve._private.constants.DEFAULT_HTTP_PORT",  # Not callable, ignore.
    ]
    grpc_options = gRPCOptions(
        port=port,
        grpc_servicer_functions=grpc_servicer_functions,
    )
    assert grpc_options.port == port
    assert grpc_options.grpc_servicer_functions == grpc_servicer_functions
    assert grpc_options.grpc_servicer_func_callable == [
        add_UserDefinedServiceServicer_to_server
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
