import copy
import json
import logging
import sys
from typing import Dict, List, Optional, Union

import pytest

from ray import serve
from ray._private.pydantic_compat import ValidationError
from ray.serve.config import AutoscalingConfig
from ray.serve.deployment import deployment_to_schema, schema_to_deployment
from ray.serve.schema import (
    DeploymentSchema,
    LoggingConfig,
    RayActorOptionsSchema,
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
    _skip_validating_runtime_env_uris,
)
from ray.serve.tests.common.remote_uris import (
    TEST_DEPLOY_GROUP_PINNED_URI,
    TEST_MODULE_PINNED_URI,
)
from ray.util.accelerators.accelerators import NVIDIA_TESLA_P4, NVIDIA_TESLA_V100


def get_valid_runtime_envs() -> List[Dict]:
    """Get list of runtime environments allowed in Serve config/REST API."""

    return [
        # Empty runtime_env
        {},
        # Runtime_env with remote_URIs
        {
            "working_dir": TEST_MODULE_PINNED_URI,
            "py_modules": [TEST_DEPLOY_GROUP_PINNED_URI],
        },
        # Runtime_env with extra options
        {
            "working_dir": TEST_MODULE_PINNED_URI,
            "py_modules": [TEST_DEPLOY_GROUP_PINNED_URI],
            "pip": ["pandas", "numpy"],
            "env_vars": {"OMP_NUM_THREADS": "32", "EXAMPLE_VAR": "hello"},
            "excludes": "imaginary_file.txt",
        },
    ]


def get_invalid_runtime_envs() -> List[Dict]:
    """Get list of runtime environments not allowed in Serve config/REST API."""

    return [
        # Local URIs in working_dir and py_modules
        {
            "working_dir": ".",
            "py_modules": [
                "/Desktop/my_project",
                TEST_DEPLOY_GROUP_PINNED_URI,
            ],
        }
    ]


def get_valid_import_paths() -> List[str]:
    """Get list of import paths allowed in Serve config/REST API."""

    return [
        "module.deployment_graph",
        "module.submodule1.deploymentGraph",
        "module.submodule1.submodule_2.DeploymentGraph",
        "module:deploymentgraph",
        "module.submodule1:deploymentGraph",
        "module.submodule1.submodule_2:DeploymentGraph",
    ]


def get_invalid_import_paths() -> List[str]:
    """Get list of import paths not allowed in Serve config/REST API."""

    return [
        # Empty import path
        "",
        # Only a dot
        ".",
        # Only a colon,
        ":",
        # Import path with no dot or colon
        "module_deployment_graph",
        # Import path with empty deployment graph
        "module.",
        "module.submodule.",
        # Import path with no deployment graph
        ".module",
        # Import paths with more than 1 colon
        "module:submodule1:deploymentGraph",
        "module.submodule1:deploymentGraph:",
        "module:submodule1:submodule_2:DeploymentGraph",
        "module.submodule_1:submodule2:deployment_Graph",
        "module.submodule_1:submodule2:sm3.dg",
    ]


class TestRayActorOptionsSchema:
    def get_valid_ray_actor_options_schema(self):
        return {
            "runtime_env": {
                "working_dir": TEST_MODULE_PINNED_URI,
            },
            "num_cpus": 0.2,
            "num_gpus": 50,
            "memory": 3,
            "object_store_memory": 64,
            "resources": {"custom_asic": 12},
            "accelerator_type": NVIDIA_TESLA_V100,
        }

    def test_valid_ray_actor_options_schema(self):
        # Ensure a valid RayActorOptionsSchema can be generated

        ray_actor_options_schema = self.get_valid_ray_actor_options_schema()
        RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

    def test_ge_zero_ray_actor_options_schema(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than zero is set to zero.

        ge_zero_fields = ["num_cpus", "num_gpus", "memory", "object_store_memory"]
        for field in ge_zero_fields:
            with pytest.raises(ValidationError):
                RayActorOptionsSchema.parse_obj({field: -1})

    @pytest.mark.parametrize("env", get_valid_runtime_envs())
    def test_ray_actor_options_valid_runtime_env(self, env):
        # Test valid runtime_env configurations

        ray_actor_options_schema = self.get_valid_ray_actor_options_schema()
        ray_actor_options_schema["runtime_env"] = env
        schema = RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

        original_runtime_env = copy.deepcopy(schema.runtime_env)
        # Make sure "working_dir" is only added once.
        for _ in range(5):
            schema = RayActorOptionsSchema.parse_obj(schema)
            assert schema.runtime_env == original_runtime_env

    @pytest.mark.parametrize("env", get_invalid_runtime_envs())
    def test_ray_actor_options_invalid_runtime_env(self, env):
        # Test invalid runtime_env configurations

        ray_actor_options_schema = self.get_valid_ray_actor_options_schema()
        ray_actor_options_schema["runtime_env"] = env

        # By default, runtime_envs with local URIs should be rejected.
        with pytest.raises(ValueError):
            RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

        # Inside the context, runtime_envs with local URIs should not be rejected.
        with _skip_validating_runtime_env_uris():
            schema = RayActorOptionsSchema.parse_obj(ray_actor_options_schema)
            assert schema.runtime_env == env

        # Check that the validation state is reset outside of the context manager.
        with pytest.raises(ValueError):
            RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

    def test_extra_fields_invalid_ray_actor_options(self):
        # Undefined fields should be forbidden in the schema

        ray_actor_options_schema = {
            "runtime_env": {},
            "num_cpus": None,
            "num_gpus": None,
            "memory": None,
            "object_store_memory": None,
            "resources": {},
            "accelerator_type": None,
        }

        # Schema should be createable with valid fields
        RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

        # Schema should NOT raise error when extra field is included
        ray_actor_options_schema["extra_field"] = None
        RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

    def test_dict_defaults_ray_actor_options(self):
        # Dictionary fields should have empty dictionaries as defaults, not None

        ray_actor_options_schema = {}
        schema = RayActorOptionsSchema.parse_obj(ray_actor_options_schema)
        d = schema.dict()
        assert d["runtime_env"] == {}
        assert d["resources"] == {}


class TestDeploymentSchema:
    def get_minimal_deployment_schema(self):
        # Generate a DeploymentSchema with the fewest possible attributes set

        return {"name": "deep"}

    def test_valid_deployment_schema(self):
        # Ensure a valid DeploymentSchema can be generated

        deployment_schema = {
            "name": "shallow",
            "num_replicas": 2,
            "route_prefix": "/shallow",
            "max_concurrent_queries": 32,
            "max_queued_requests": 12,
            "user_config": {"threshold": 0.2, "pattern": "rainbow"},
            "autoscaling_config": None,
            "graceful_shutdown_wait_loop_s": 17,
            "graceful_shutdown_timeout_s": 49,
            "health_check_period_s": 11,
            "health_check_timeout_s": 11,
            "ray_actor_options": {
                "runtime_env": {
                    "working_dir": TEST_MODULE_PINNED_URI,
                    "py_modules": [TEST_DEPLOY_GROUP_PINNED_URI],
                },
                "num_cpus": 3,
                "num_gpus": 4.2,
                "memory": 5,
                "object_store_memory": 3,
                "resources": {"custom_asic": 8},
                "accelerator_type": NVIDIA_TESLA_P4,
            },
        }

        DeploymentSchema.parse_obj(deployment_schema)

    def test_gt_zero_deployment_schema(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than zero is set to zero.

        deployment_schema = self.get_minimal_deployment_schema()

        gt_zero_fields = [
            "num_replicas",
            "max_concurrent_queries",
            "health_check_period_s",
            "health_check_timeout_s",
        ]
        for field in gt_zero_fields:
            deployment_schema[field] = 0
            with pytest.raises(ValidationError):
                DeploymentSchema.parse_obj(deployment_schema)
            deployment_schema[field] = None

    def test_ge_zero_deployment_schema(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than or equal to zero is set to -1.

        deployment_schema = self.get_minimal_deployment_schema()

        ge_zero_fields = [
            "graceful_shutdown_wait_loop_s",
            "graceful_shutdown_timeout_s",
        ]

        for field in ge_zero_fields:
            deployment_schema[field] = -1
            with pytest.raises(ValidationError):
                DeploymentSchema.parse_obj(deployment_schema)
            deployment_schema[field] = None

    def test_validate_max_queued_requests(self):
        # Ensure ValidationError is raised when max_queued_requests is not -1 or > 1.

        deployment_schema = self.get_minimal_deployment_schema()

        deployment_schema["max_queued_requests"] = -1
        DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["max_queued_requests"] = 1
        DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["max_queued_requests"] = 100
        DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["max_queued_requests"] = "hi"
        with pytest.raises(ValidationError):
            DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["max_queued_requests"] = 1.5
        with pytest.raises(ValidationError):
            DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["max_queued_requests"] = 0
        with pytest.raises(ValidationError):
            DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["max_queued_requests"] = -2
        with pytest.raises(ValidationError):
            DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["max_queued_requests"] = -100
        with pytest.raises(ValidationError):
            DeploymentSchema.parse_obj(deployment_schema)

    def test_route_prefix(self):
        # Ensure that route_prefix is validated

        deployment_schema = self.get_minimal_deployment_schema()

        # route_prefix must start with a "/"
        deployment_schema["route_prefix"] = "hello/world"
        with pytest.raises(ValueError):
            DeploymentSchema.parse_obj(deployment_schema)

        # route_prefix must end with a "/"
        deployment_schema["route_prefix"] = "/hello/world/"
        with pytest.raises(ValueError):
            DeploymentSchema.parse_obj(deployment_schema)

        # route_prefix cannot contain wildcards, meaning it can't have
        # "{" or "}"
        deployment_schema["route_prefix"] = "/hello/{adjective}/world/"
        with pytest.raises(ValueError):
            DeploymentSchema.parse_obj(deployment_schema)

        # Ensure a valid route_prefix works
        deployment_schema["route_prefix"] = "/hello/wonderful/world"
        DeploymentSchema.parse_obj(deployment_schema)

        # Ensure route_prefix of "/" works
        deployment_schema["route_prefix"] = "/"
        DeploymentSchema.parse_obj(deployment_schema)

        # Ensure route_prefix of None works
        deployment_schema["route_prefix"] = None
        DeploymentSchema.parse_obj(deployment_schema)

    def test_mutually_exclusive_num_replicas_and_autoscaling_config(self):
        # num_replicas and autoscaling_config cannot be set at the same time
        deployment_schema = self.get_minimal_deployment_schema()

        deployment_schema["num_replicas"] = 5
        deployment_schema["autoscaling_config"] = None
        DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["num_replicas"] = None
        deployment_schema["autoscaling_config"] = AutoscalingConfig().dict()
        DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["num_replicas"] = 5
        deployment_schema["autoscaling_config"] = AutoscalingConfig().dict()
        with pytest.raises(ValueError):
            DeploymentSchema.parse_obj(deployment_schema)

    def test_num_replicas_auto(self):
        deployment_schema = self.get_minimal_deployment_schema()

        deployment_schema["num_replicas"] = "auto"
        deployment_schema["autoscaling_config"] = None
        DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["num_replicas"] = "auto"
        deployment_schema["autoscaling_config"] = {"max_replicas": 99}
        DeploymentSchema.parse_obj(deployment_schema)

        deployment_schema["num_replicas"] = "random_str"
        deployment_schema["autoscaling_config"] = None
        with pytest.raises(ValueError):
            DeploymentSchema.parse_obj(deployment_schema)

    def test_extra_fields_invalid_deployment_schema(self):
        # Undefined fields should be forbidden in the schema

        deployment_schema = self.get_minimal_deployment_schema()

        # Schema should be createable with valid fields
        DeploymentSchema.parse_obj(deployment_schema)

        # Schema should NOT raise error when extra field is included
        deployment_schema["extra_field"] = None
        DeploymentSchema.parse_obj(deployment_schema)

    @pytest.mark.parametrize(
        "option",
        [
            "num_replicas",
            "route_prefix",
            "autoscaling_config",
            "user_config",
        ],
    )
    def test_nullable_options(self, option: str):
        """Check that nullable options can be set to None."""

        deployment_options = {"name": "test", option: None}

        # One of "num_replicas" or "autoscaling_config" must be provided.
        if option == "num_replicas":
            deployment_options["autoscaling_config"] = {
                "min_replicas": 1,
                "max_replicas": 5,
                "target_num_ongoing_requests_per_replica": 5,
            }
        elif option == "autoscaling_config":
            deployment_options["num_replicas"] = 5

        # Schema should be created without error.
        DeploymentSchema.parse_obj(deployment_options)


class TestServeApplicationSchema:
    def get_valid_serve_application_schema(self):
        return {
            "import_path": "module.graph",
            "runtime_env": {},
            "deployments": [
                {
                    "name": "shallow",
                    "num_replicas": 2,
                    "route_prefix": "/shallow",
                    "max_concurrent_queries": 32,
                    "user_config": None,
                    "autoscaling_config": None,
                    "graceful_shutdown_wait_loop_s": 17,
                    "graceful_shutdown_timeout_s": 49,
                    "health_check_period_s": 11,
                    "health_check_timeout_s": 11,
                    "ray_actor_options": {
                        "runtime_env": {
                            "working_dir": TEST_MODULE_PINNED_URI,
                            "py_modules": [TEST_DEPLOY_GROUP_PINNED_URI],
                        },
                        "num_cpus": 3,
                        "num_gpus": 4.2,
                        "memory": 5,
                        "object_store_memory": 3,
                        "resources": {"custom_asic": 8},
                        "accelerator_type": NVIDIA_TESLA_P4,
                    },
                },
                {
                    "name": "deep",
                },
            ],
        }

    def test_valid_serve_application_schema(self):
        # Ensure a valid ServeApplicationSchema can be generated

        serve_application_schema = self.get_valid_serve_application_schema()
        ServeApplicationSchema.parse_obj(serve_application_schema)

    def test_extra_fields_invalid_serve_application_schema(self):
        # Undefined fields should be forbidden in the schema

        serve_application_schema = self.get_valid_serve_application_schema()

        # Schema should be createable with valid fields
        ServeApplicationSchema.parse_obj(serve_application_schema)

        # Schema should NOT raise error when extra field is included
        serve_application_schema["extra_field"] = None
        ServeApplicationSchema.parse_obj(serve_application_schema)

    @pytest.mark.parametrize("env", get_valid_runtime_envs())
    def test_serve_application_valid_runtime_env(self, env):
        # Test valid runtime_env configurations

        serve_application_schema = self.get_valid_serve_application_schema()
        serve_application_schema["runtime_env"] = env
        schema = ServeApplicationSchema.parse_obj(serve_application_schema)

        original_runtime_env = copy.deepcopy(schema.runtime_env)
        # Make sure "working_dir" is only added once.
        for _ in range(5):
            schema = ServeApplicationSchema.parse_obj(schema)
            assert schema.runtime_env == original_runtime_env

    @pytest.mark.parametrize("env", get_invalid_runtime_envs())
    def test_serve_application_invalid_runtime_env(self, env):
        # Test invalid runtime_env configurations

        serve_application_schema = self.get_valid_serve_application_schema()
        serve_application_schema["runtime_env"] = env
        with pytest.raises(ValueError):
            ServeApplicationSchema.parse_obj(serve_application_schema)

        # By default, runtime_envs with local URIs should be rejected.
        with pytest.raises(ValueError):
            ServeApplicationSchema.parse_obj(serve_application_schema)

        # Inside the context, runtime_envs with local URIs should not be rejected.
        with _skip_validating_runtime_env_uris():
            schema = ServeApplicationSchema.parse_obj(serve_application_schema)
            assert schema.runtime_env == env

        # Check that the validation was reset after the above call.
        with pytest.raises(ValueError):
            ServeApplicationSchema.parse_obj(serve_application_schema)

    @pytest.mark.parametrize("path", get_valid_import_paths())
    def test_serve_application_valid_import_path(self, path):
        # Test valid import path formats

        serve_application_schema = self.get_valid_serve_application_schema()
        serve_application_schema["import_path"] = path
        ServeApplicationSchema.parse_obj(serve_application_schema)

    @pytest.mark.parametrize("path", get_invalid_import_paths())
    def test_serve_application_invalid_import_path(self, path):
        # Test invalid import path formats

        serve_application_schema = self.get_valid_serve_application_schema()
        serve_application_schema["import_path"] = path
        with pytest.raises(ValidationError):
            ServeApplicationSchema.parse_obj(serve_application_schema)

    def test_serve_application_import_path_required(self):
        # If no import path is specified, this should not parse successfully
        with pytest.raises(ValidationError):
            ServeApplicationSchema.parse_obj({"host": "127.0.0.1", "port": 8000})


class TestServeDeploySchema:
    def test_deploy_config_duplicate_apps(self):
        deploy_config_dict = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/alice",
                    "import_path": "module.graph",
                },
                {
                    "name": "app2",
                    "route_prefix": "/charlie",
                    "import_path": "module.graph",
                },
            ],
        }
        ServeDeploySchema.parse_obj(deploy_config_dict)

        # Duplicate app1
        deploy_config_dict["applications"].append(
            {"name": "app1", "route_prefix": "/bob", "import_path": "module.graph"},
        )
        with pytest.raises(ValidationError) as e:
            ServeDeploySchema.parse_obj(deploy_config_dict)
        assert "app1" in str(e.value) and "app2" not in str(e.value)

        # Duplicate app2
        deploy_config_dict["applications"].append(
            {"name": "app2", "route_prefix": "/david", "import_path": "module.graph"}
        )
        with pytest.raises(ValidationError) as e:
            ServeDeploySchema.parse_obj(deploy_config_dict)
        assert "app1" in str(e.value) and "app2" in str(e.value)

    def test_deploy_config_duplicate_routes1(self):
        """Test that apps with duplicate route prefixes raises validation error"""
        deploy_config_dict = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/alice",
                    "import_path": "module.graph",
                },
                {"name": "app2", "route_prefix": "/bob", "import_path": "module.graph"},
            ],
        }
        ServeDeploySchema.parse_obj(deploy_config_dict)

        # Duplicate route prefix /alice
        deploy_config_dict["applications"].append(
            {"name": "app3", "route_prefix": "/alice", "import_path": "module.graph"},
        )
        with pytest.raises(ValidationError) as e:
            ServeDeploySchema.parse_obj(deploy_config_dict)
        assert "alice" in str(e.value) and "bob" not in str(e.value)

        # Duplicate route prefix /bob
        deploy_config_dict["applications"].append(
            {"name": "app4", "route_prefix": "/bob", "import_path": "module.graph"},
        )
        with pytest.raises(ValidationError) as e:
            ServeDeploySchema.parse_obj(deploy_config_dict)
        assert "alice" in str(e.value) and "bob" in str(e.value)

    def test_deploy_config_duplicate_routes2(self):
        """Test that multiple apps with route_prefix set to None parses with no error"""
        deploy_config_dict = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": "module.graph",
                },
                {"name": "app2", "route_prefix": None, "import_path": "module.graph"},
                {"name": "app3", "route_prefix": None, "import_path": "module.graph"},
            ],
        }
        ServeDeploySchema.parse_obj(deploy_config_dict)

    @pytest.mark.parametrize("option,value", [("host", "127.0.0.1"), ("port", 8000)])
    def test_deploy_config_nested_http_options(self, option, value):
        """
        The application configs inside a deploy config should not have http options set.
        """
        deploy_config_dict = {
            "http_options": {
                "host": "127.0.0.1",
                "port": 8000,
            },
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": "module.graph",
                },
            ],
        }
        deploy_config_dict["applications"][0][option] = value
        with pytest.raises(ValidationError) as e:
            ServeDeploySchema.parse_obj(deploy_config_dict)
        assert option in str(e.value)

    def test_deploy_empty_name(self):
        """The application configs inside a deploy config should have nonempty names."""

        deploy_config_dict = {
            "applications": [
                {
                    "name": "",
                    "route_prefix": "/app1",
                    "import_path": "module.graph",
                },
            ],
        }
        with pytest.raises(ValidationError) as e:
            ServeDeploySchema.parse_obj(deploy_config_dict)
        # Error message should be descriptive, mention name must be nonempty
        assert "name" in str(e.value) and "empty" in str(e.value)

    def test_deploy_no_applications(self):
        """Applications must be specified."""

        deploy_config_dict = {
            "http_options": {
                "host": "127.0.0.1",
                "port": 8000,
            },
        }
        with pytest.raises(ValidationError):
            ServeDeploySchema.parse_obj(deploy_config_dict)

    def test_deploy_with_grpc_options(self):
        """gRPC options can be specified."""

        deploy_config_dict = {
            "grpc_options": {
                "port": 9000,
                "grpc_servicer_functions": ["foo.bar"],
            },
            "applications": [],
        }
        ServeDeploySchema.parse_obj(deploy_config_dict)

    @pytest.mark.parametrize(
        "input_val,error,output_val",
        [
            # Can be omitted and defaults to `None`.
            (None, False, None),
            # Can be an int or a float.
            (50, False, 50),
            (33.33, False, 33.33),  # "... repeating, of course."
            # Can be 0 or 100, inclusive.
            (0, False, 0.0),
            (0.0, False, 0.0),
            (100, False, 100.0),
            (100.0, False, 100.0),
            # Cannot be < 0 or > 100.
            (-0.1, True, None),
            (-1, True, None),
            (100.1, True, None),
            (101, True, None),
        ],
    )
    def test_target_capacity(
        self,
        input_val: Union[None, int, float],
        error: bool,
        output_val: Optional[float],
    ):
        """Test validation of `target_capacity` field."""

        deploy_config_dict = {
            "applications": [],
        }
        if input_val is not None:
            deploy_config_dict["target_capacity"] = input_val

        if error:
            with pytest.raises(ValidationError):
                ServeDeploySchema.parse_obj(deploy_config_dict)
        else:
            s = ServeDeploySchema.parse_obj(deploy_config_dict)
            assert s.target_capacity == output_val


class TestLoggingConfig:
    def test_parse_dict(self):
        schema = LoggingConfig.parse_obj(
            {
                "log_level": logging.DEBUG,
                "encoding": "JSON",
                "logs_dir": "/my_dir",
                "enable_access_log": True,
            }
        )
        assert schema.log_level == "DEBUG"
        assert schema.encoding == "JSON"
        assert schema.logs_dir == "/my_dir"
        assert schema.enable_access_log

        # Test string values for log_level.
        schema = LoggingConfig.parse_obj(
            {
                "log_level": "DEBUG",
            }
        )
        assert schema.log_level == "DEBUG"

    def test_wrong_encoding_type(self):
        with pytest.raises(ValidationError):
            LoggingConfig.parse_obj(
                {
                    "logging_level": logging.INFO,
                    "encoding": "NOT_EXIST",
                    "logs_dir": "/my_dir",
                    "enable_access_log": True,
                }
            )

    def test_default_values(self):
        schema = LoggingConfig.parse_obj({})
        assert schema.log_level == "INFO"
        assert schema.encoding == "TEXT"
        assert schema.logs_dir is None
        assert schema.enable_access_log


# This function is defined globally to be accessible via import path
def global_f():
    return "Hello world!"


def test_deployment_to_schema_to_deployment():
    @serve.deployment(
        num_replicas=3,
        route_prefix="/hello",
        ray_actor_options={
            "runtime_env": {
                "working_dir": TEST_MODULE_PINNED_URI,
                "py_modules": [TEST_DEPLOY_GROUP_PINNED_URI],
            }
        },
    )
    def f():
        # The body of this function doesn't matter. It gets replaced by
        # global_f() when the import path in f._func_or_class is overwritten.
        # This function is used as a convenience to apply the @serve.deployment
        # decorator without converting global_f() into a Deployment object.
        pass

    deployment = schema_to_deployment(deployment_to_schema(f))
    deployment.set_options(func_or_class="ray.serve.tests.test_schema.global_f")

    assert deployment.num_replicas == 3
    assert deployment.route_prefix == "/hello"
    assert (
        deployment.ray_actor_options["runtime_env"]["working_dir"]
        == TEST_MODULE_PINNED_URI
    )
    assert deployment.ray_actor_options["runtime_env"]["py_modules"] == [
        TEST_DEPLOY_GROUP_PINNED_URI,
        TEST_MODULE_PINNED_URI,
    ]


def test_unset_fields_schema_to_deployment_ray_actor_options():
    # Ensure unset fields are excluded from ray_actor_options

    @serve.deployment(
        num_replicas=3,
        route_prefix="/hello",
        ray_actor_options={},
    )
    def f():
        pass

    deployment = schema_to_deployment(deployment_to_schema(f))
    deployment.set_options(func_or_class="ray.serve.tests.test_schema.global_f")

    # Serve will set num_cpus to 1 if it's not set.
    assert len(deployment.ray_actor_options) == 1
    assert deployment.ray_actor_options["num_cpus"] == 1


def test_serve_instance_details_is_json_serializable():
    """Test that ServeInstanceDetails is json serializable."""
    serialized_policy_def = (
        b"\x80\x05\x95L\x00\x00\x00\x00\x00\x00\x00\x8c\x1cray."
        b"serve.autoscaling_policy\x94\x8c'replica_queue_length_"
        b"autoscaling_policy\x94\x93\x94."
    )
    details = ServeInstanceDetails(
        controller_info={"node_id": "fake_node_id"},
        proxy_location="EveryNode",
        proxies={"node1": {"status": "HEALTHY"}},
        applications={
            "app1": {
                "name": "app1",
                "route_prefix": "/app1",
                "docs_path": "/docs/app1",
                "status": "RUNNING",
                "message": "fake_message",
                "last_deployed_time_s": 123,
                "deployments": {
                    "deployment1": {
                        "name": "deployment1",
                        "status": "HEALTHY",
                        "status_trigger": "AUTOSCALING",
                        "message": "fake_message",
                        "deployment_config": {
                            "name": "deployment1",
                            "autoscaling_config": {
                                # Byte object will cause json serializable error
                                "_serialized_policy_def": serialized_policy_def
                            },
                        },
                        "replicas": [],
                    }
                },
            }
        },
    )._get_user_facing_json_serializable_dict(exclude_unset=True)
    details_json = json.dumps(details)

    expected_json = json.dumps(
        {
            "controller_info": {"node_id": "fake_node_id"},
            "proxy_location": "EveryNode",
            "proxies": {"node1": {"status": "HEALTHY"}},
            "applications": {
                "app1": {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "docs_path": "/docs/app1",
                    "status": "RUNNING",
                    "message": "fake_message",
                    "last_deployed_time_s": 123.0,
                    "deployments": {
                        "deployment1": {
                            "name": "deployment1",
                            "status": "HEALTHY",
                            "status_trigger": "AUTOSCALING",
                            "message": "fake_message",
                            "deployment_config": {
                                "name": "deployment1",
                                "autoscaling_config": {},
                            },
                            "replicas": [],
                        }
                    },
                }
            },
        }
    )
    assert details_json == expected_json

    # ensure internal field, serialized_policy_def, is not exposed
    application = details["applications"]["app1"]
    deployment = application["deployments"]["deployment1"]
    autoscaling_config = deployment["deployment_config"]["autoscaling_config"]
    assert "_serialized_policy_def" not in autoscaling_config


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
