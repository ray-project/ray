import pytest

from ray import cloudpickle
from ray._private.pydantic_compat import ValidationError
from ray._common.utils import import_attr
from ray.serve._private.config import DeploymentConfig, ReplicaConfig, _proto_to_dict
from ray.serve._private.constants import DEFAULT_AUTOSCALING_POLICY, DEFAULT_GRPC_PORT
from ray.serve._private.utils import DEFAULT
from ray.serve.autoscaling_policy import default_autoscaling_policy
from ray.serve.config import (
    AutoscalingConfig,
    DeploymentMode,
    HTTPOptions,
    ProxyLocation,
    gRPCOptions,
)
from ray.serve.generated.serve_pb2 import AutoscalingConfig as AutoscalingConfigProto
from ray.serve.generated.serve_pb2 import DeploymentConfig as DeploymentConfigProto
from ray.serve.generated.serve_pb2 import DeploymentLanguage
from ray.serve.generated.serve_pb2_grpc import add_UserDefinedServiceServicer_to_server
from ray.serve.schema import (
    DeploymentSchema,
    HTTPOptionsSchema,
    ServeApplicationSchema,
    ServeDeploySchema,
)

fake_policy_return_value = 123


def fake_policy():
    return fake_policy_return_value


def test_autoscaling_config_validation():
    # Check validation over publicly exposed options

    with pytest.raises(ValidationError):
        # min_replicas must be nonnegative
        AutoscalingConfig(min_replicas=-1)

    with pytest.raises(ValidationError):
        # max_replicas must be positive
        AutoscalingConfig(max_replicas=0)

    # target_ongoing_requests must be nonnegative
    with pytest.raises(ValidationError):
        AutoscalingConfig(target_ongoing_requests=-1)

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

        # Test dynamic default for max_ongoing_requests.
        assert DeploymentConfig().max_ongoing_requests == 5

    def test_deployment_config_update(self):
        b = DeploymentConfig(num_replicas=1, max_ongoing_requests=1)

        # Test updating a key works.
        b.num_replicas = 2
        assert b.num_replicas == 2
        # Check that not specifying a key doesn't update it.
        assert b.max_ongoing_requests == 1

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

        # Valid parameter with DEFAULT.VALUE passed in should be ignored
        DeploymentConfig.from_default(num_replicas=DEFAULT.VALUE)


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

    def test_max_replicas_per_node_validation(self):
        class Class:
            pass

        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            max_replicas_per_node=5,
        )

        # Invalid type
        with pytest.raises(TypeError, match="Get invalid type"):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                max_replicas_per_node="1",
            )

        # Invalid: not in the range of [1, 100]
        with pytest.raises(
            ValueError,
            match=r"Valid values are None or an integer in the range of \[1, 100\]",
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                max_replicas_per_node=0,
            )

        with pytest.raises(
            ValueError,
            match=r"Valid values are None or an integer in the range of \[1, 100\]",
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                max_replicas_per_node=110,
            )

        with pytest.raises(
            ValueError,
            match=r"Valid values are None or an integer in the range of \[1, 100\]",
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                max_replicas_per_node=-1,
            )

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
            ValueError, match="Invalid placement group strategy FAKE_NEWS"
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
            match=("Bundles must be a non-empty list " "of resource dictionaries."),
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                placement_group_bundles=[{"CPU": "1.0"}],
            )

        # Invalid: invalid placement_group_bundles.
        with pytest.raises(
            ValueError,
            match="cannot be an empty dictionary or resources with only 0",
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                ray_actor_options={"num_cpus": 0, "num_gpus": 0},
                placement_group_bundles=[{"CPU": 0, "GPU": 0}],
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

    def test_mutually_exclusive_max_replicas_per_node_and_placement_group_bundles(self):
        class Class:
            pass

        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            max_replicas_per_node=5,
        )

        ReplicaConfig.create(
            Class,
            tuple(),
            dict(),
            placement_group_bundles=[{"CPU": 1.0}],
        )

        with pytest.raises(
            ValueError,
            match=(
                "Setting max_replicas_per_node is not allowed when "
                "placement_group_bundles is provided."
            ),
        ):
            ReplicaConfig.create(
                Class,
                tuple(),
                dict(),
                max_replicas_per_node=5,
                placement_group_bundles=[{"CPU": 1.0}],
            )

        with pytest.raises(
            ValueError,
            match=(
                "Setting max_replicas_per_node is not allowed when "
                "placement_group_bundles is provided."
            ),
        ):
            config = ReplicaConfig.create(Class, tuple(), dict())
            config.update(
                ray_actor_options={},
                max_replicas_per_node=5,
                placement_group_bundles=[{"CPU": 1.0}],
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


class TestAutoscalingConfig:
    def test_target_ongoing_requests(self):
        autoscaling_config = AutoscalingConfig()
        assert autoscaling_config.get_target_ongoing_requests() == 2

        autoscaling_config = AutoscalingConfig(target_ongoing_requests=7)
        assert autoscaling_config.get_target_ongoing_requests() == 7

    def test_scaling_factor(self):
        autoscaling_config = AutoscalingConfig()
        assert autoscaling_config.get_upscaling_factor() == 1
        assert autoscaling_config.get_downscaling_factor() == 1

        autoscaling_config = AutoscalingConfig(smoothing_factor=0.4)
        assert autoscaling_config.get_upscaling_factor() == 0.4
        assert autoscaling_config.get_downscaling_factor() == 0.4

        autoscaling_config = AutoscalingConfig(upscale_smoothing_factor=0.4)
        assert autoscaling_config.get_upscaling_factor() == 0.4
        assert autoscaling_config.get_downscaling_factor() == 1

        autoscaling_config = AutoscalingConfig(downscale_smoothing_factor=0.4)
        assert autoscaling_config.get_upscaling_factor() == 1
        assert autoscaling_config.get_downscaling_factor() == 0.4

        autoscaling_config = AutoscalingConfig(
            smoothing_factor=0.4,
            upscale_smoothing_factor=0.1,
            downscale_smoothing_factor=0.01,
        )
        assert autoscaling_config.get_upscaling_factor() == 0.1
        assert autoscaling_config.get_downscaling_factor() == 0.01

        autoscaling_config = AutoscalingConfig(
            smoothing_factor=0.4,
            upscaling_factor=0.5,
            downscaling_factor=0.6,
        )
        assert autoscaling_config.get_upscaling_factor() == 0.5
        assert autoscaling_config.get_downscaling_factor() == 0.6

        autoscaling_config = AutoscalingConfig(
            smoothing_factor=0.4,
            upscale_smoothing_factor=0.1,
            downscale_smoothing_factor=0.01,
            upscaling_factor=0.5,
            downscaling_factor=0.6,
        )
        assert autoscaling_config.get_upscaling_factor() == 0.5
        assert autoscaling_config.get_downscaling_factor() == 0.6


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
                        new_version_config_key="this config is from newer version"
                        " of Ray",
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
    config = DeploymentConfig(num_replicas=100, max_ongoing_requests=16)
    assert config == DeploymentConfig.from_proto_bytes(config.to_proto_bytes())

    # Test user_config object
    config = DeploymentConfig(user_config={"python": ("native", ["objects"])})
    assert config == DeploymentConfig.from_proto_bytes(config.to_proto_bytes())


@pytest.mark.parametrize("use_deprecated_smoothing_factor", [True, False])
def test_zero_default_proto(use_deprecated_smoothing_factor):
    # Test that options set to zero (protobuf default value) still retain their
    # original value after being serialized and deserialized.
    autoscaling_config = {
        "min_replicas": 1,
        "max_replicas": 2,
        "downscale_delay_s": 0,
    }
    if use_deprecated_smoothing_factor:
        autoscaling_config["smoothing_factor"] = 0.123
    else:
        autoscaling_config["upscaling_factor"] = 0.123
        autoscaling_config["downscaling_factor"] = 0.123

    config = DeploymentConfig(autoscaling_config=autoscaling_config)
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
    correctly. Also, if the user provided an invalid grpc_servicer_function, it
    raises errors.
    """
    default_grpc_options = gRPCOptions()
    assert default_grpc_options.port == DEFAULT_GRPC_PORT
    assert default_grpc_options.grpc_servicer_functions == []
    assert default_grpc_options.grpc_servicer_func_callable == []

    port = 9001
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
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

    # Import not found should raise ModuleNotFoundError.
    grpc_servicer_functions = ["fake.service.that.does.not.exist"]
    with pytest.raises(ModuleNotFoundError) as exception:
        grpc_options = gRPCOptions(grpc_servicer_functions=grpc_servicer_functions)
        _ = grpc_options.grpc_servicer_func_callable
    assert "can't be imported!" in str(exception)

    # Not callable should raise ValueError.
    grpc_servicer_functions = ["ray.serve._private.constants.DEFAULT_HTTP_PORT"]
    with pytest.raises(ValueError) as exception:
        grpc_options = gRPCOptions(grpc_servicer_functions=grpc_servicer_functions)
        _ = grpc_options.grpc_servicer_func_callable
    assert "is not a callable function!" in str(exception)


def test_proxy_location_to_deployment_mode():
    assert (
        ProxyLocation._to_deployment_mode(ProxyLocation.Disabled)
        == DeploymentMode.NoServer
    )
    assert (
        ProxyLocation._to_deployment_mode(ProxyLocation.HeadOnly)
        == DeploymentMode.HeadOnly
    )
    assert (
        ProxyLocation._to_deployment_mode(ProxyLocation.EveryNode)
        == DeploymentMode.EveryNode
    )

    assert ProxyLocation._to_deployment_mode("Disabled") == DeploymentMode.NoServer
    assert ProxyLocation._to_deployment_mode("HeadOnly") == DeploymentMode.HeadOnly
    assert ProxyLocation._to_deployment_mode("EveryNode") == DeploymentMode.EveryNode

    with pytest.raises(ValueError):
        ProxyLocation._to_deployment_mode("Unknown")

    with pytest.raises(TypeError):
        ProxyLocation._to_deployment_mode({"some_other_obj"})


def test_deployment_mode_to_proxy_location():
    assert ProxyLocation._from_deployment_mode(None) is None

    assert (
        ProxyLocation._from_deployment_mode(DeploymentMode.NoServer)
        == ProxyLocation.Disabled
    )
    assert (
        ProxyLocation._from_deployment_mode(DeploymentMode.HeadOnly)
        == ProxyLocation.HeadOnly
    )
    assert (
        ProxyLocation._from_deployment_mode(DeploymentMode.EveryNode)
        == ProxyLocation.EveryNode
    )

    assert ProxyLocation._from_deployment_mode("NoServer") == ProxyLocation.Disabled
    assert ProxyLocation._from_deployment_mode("HeadOnly") == ProxyLocation.HeadOnly
    assert ProxyLocation._from_deployment_mode("EveryNode") == ProxyLocation.EveryNode

    with pytest.raises(ValueError):
        ProxyLocation._from_deployment_mode("Unknown")

    with pytest.raises(TypeError):
        ProxyLocation._from_deployment_mode({"some_other_obj"})


@pytest.mark.parametrize(
    "policy", [None, fake_policy, "ray.serve.tests.unit.test_config:fake_policy"]
)
def test_autoscaling_policy_serializations(policy):
    """Test that autoscaling policy can be serialized and deserialized.

    This test checks that the autoscaling policy can be serialized and deserialized for
    when the policy is a function, a string, or None (default).
    """
    autoscaling_config = AutoscalingConfig()
    if policy:
        autoscaling_config = AutoscalingConfig(_policy=policy)

    config = DeploymentConfig.from_default(autoscaling_config=autoscaling_config)
    deserialized_autoscaling_policy = DeploymentConfig.from_proto_bytes(
        config.to_proto_bytes()
    ).autoscaling_config.get_policy()

    # Right now we don't allow modifying the autoscaling policy, so this will always
    # be the default autoscaling policy
    assert deserialized_autoscaling_policy == default_autoscaling_policy


def test_autoscaling_policy_import_fails_for_non_existing_policy():
    """Test that autoscaling policy will error out for non-existing policy.

    This test will ensure non-existing policy will be caught. It can happen when we
    moved the default policy or when user pass in a non-existing policy.
    """
    # Right now we don't allow modifying the autoscaling policy, so this will not fail
    policy = "i.dont.exist:fake_policy"
    AutoscalingConfig(_policy=policy)


def test_default_autoscaling_policy_import_path():
    """Test that default autoscaling policy can be imported."""
    policy = import_attr(DEFAULT_AUTOSCALING_POLICY)

    assert policy == default_autoscaling_policy


class TestProtoToDict:
    def test_empty_fields(self):
        """Test _proto_to_dict() to deserialize protobuf with empty fields"""
        proto = DeploymentConfigProto()
        result = _proto_to_dict(proto)

        # Defaults are filled.
        assert result["num_replicas"] == 0
        assert result["max_ongoing_requests"] == 0
        assert result["user_config"] == b""
        assert result["user_configured_option_names"] == []

        # Nested profobufs don't exist.
        assert "autoscaling_config" not in result

    def test_non_empty_fields(self):
        """Test _proto_to_dict() to deserialize protobuf with non-empty fields"""
        num_replicas = 111
        max_ongoing_requests = 222
        proto = DeploymentConfigProto(
            num_replicas=num_replicas,
            max_ongoing_requests=max_ongoing_requests,
        )
        result = _proto_to_dict(proto)

        # Fields with non-empty values are filled correctly.
        assert result["num_replicas"] == num_replicas
        assert result["max_ongoing_requests"] == max_ongoing_requests

        # Empty fields are continue to be filled with default values.
        assert result["user_config"] == b""

    def test_nested_protobufs(self):
        """Test _proto_to_dict() to deserialize protobuf with nested protobufs"""
        num_replicas = 111
        max_ongoing_requests = 222
        min_replicas = 333
        proto = DeploymentConfigProto(
            num_replicas=num_replicas,
            max_ongoing_requests=max_ongoing_requests,
            autoscaling_config=AutoscalingConfigProto(
                min_replicas=min_replicas,
            ),
        )
        result = _proto_to_dict(proto)

        # Non-empty field is filled correctly.
        assert result["num_replicas"] == num_replicas
        assert result["max_ongoing_requests"] == max_ongoing_requests

        # Nested protobuf is filled correctly.
        assert result["autoscaling_config"]["min_replicas"] == min_replicas

    def test_repeated_field(self):
        """Test _proto_to_dict() to deserialize protobuf with repeated field"""
        user_configured_option_names = ["foo", "bar"]
        config = DeploymentConfig.from_default(
            user_configured_option_names=user_configured_option_names,
        )
        proto_bytes = config.to_proto_bytes()
        proto = DeploymentConfigProto.FromString(proto_bytes)
        result = _proto_to_dict(proto)
        # Repeated field is filled correctly as list.
        assert set(result["user_configured_option_names"]) == set(
            user_configured_option_names
        )
        assert isinstance(result["user_configured_option_names"], list)

    def test_enum_field(self):
        """Test _proto_to_dict() to deserialize protobuf with enum field"""
        proto = DeploymentConfigProto(
            deployment_language=DeploymentLanguage.JAVA,
        )
        result = _proto_to_dict(proto)

        # Enum field is filled correctly.
        assert result["deployment_language"] == DeploymentLanguage.JAVA

    def test_optional_field(self):
        """Test _proto_to_dict() to deserialize protobuf with optional field"""
        min_replicas = 1
        proto = AutoscalingConfigProto(
            min_replicas=min_replicas,
        )
        result = _proto_to_dict(proto)

        # Non-empty field is filled correctly.
        assert result["min_replicas"] == 1

        # Empty field is filled correctly.
        assert result["max_replicas"] == 0

        # Optional field should not be filled.
        assert "initial_replicas" not in result


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
