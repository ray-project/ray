from pydantic import ValidationError
import pytest

from ray.dashboard.modules.serve.schema import (
    RayActorOptionsSchema,
    DeploymentSchema,
    ServeInstanceSchema,
)
from ray.util.accelerators.accelerators import NVIDIA_TESLA_V100, NVIDIA_TESLA_P4
from ray.serve.config import AutoscalingConfig


class TestRayActorOptionsSchema:
    def test_valid_ray_actor_options_schema(self):
        # Ensure a valid RayActorOptionsSchema can be generated

        ray_actor_options_schema = {
            "runtime_env": {
                "working_dir": (
                    "https://github.com/shrekris-anyscale/"
                    "test_module/archive/HEAD.zip"
                )
            },
            "num_cpus": 0.2,
            "num_gpus": 50,
            "memory": 3,
            "object_store_memory": 64,
            "resources": {"custom_asic": 12},
            "accelerator_type": NVIDIA_TESLA_V100,
        }

        RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

    def test_gt_zero_ray_actor_options_schema(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than zero is set to zero.

        gt_zero_fields = ["num_cpus", "num_gpus", "memory", "object_store_memory"]
        for field in gt_zero_fields:
            with pytest.raises(ValidationError):
                RayActorOptionsSchema.parse_obj({field: 0})

    def test_runtime_env(self):
        # Test different runtime_env configurations

        ray_actor_options_schema = {
            "runtime_env": None,
            "num_cpus": 0.2,
            "num_gpus": 50,
            "memory": 3,
            "object_store_memory": 64,
            "resources": {"custom_asic": 12},
            "accelerator_type": NVIDIA_TESLA_V100,
        }

        # ray_actor_options_schema should work as is
        RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

        # working_dir and py_modules cannot contain local uris
        ray_actor_options_schema["runtime_env"] = {
            "working_dir": ".",
            "py_modules": [
                "/Desktop/my_project",
                (
                    "https://github.com/shrekris-anyscale/"
                    "test_deploy_group/archive/HEAD.zip"
                ),
            ],
        }

        with pytest.raises(ValueError):
            RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

        # remote uris should work
        ray_actor_options_schema["runtime_env"] = {
            "working_dir": (
                "https://github.com/shrekris-anyscale/" "test_module/archive/HEAD.zip"
            ),
            "py_modules": [
                (
                    "https://github.com/shrekris-anyscale/"
                    "test_deploy_group/archive/HEAD.zip"
                ),
            ],
        }

        RayActorOptionsSchema.parse_obj(ray_actor_options_schema)


class TestDeploymentSchema:
    def get_minimal_deployment_schema(self):
        # Generate a DeploymentSchema with the fewest possible attributes set

        return {
            "name": "deep",
            "init_args": None,
            "init_kwargs": None,
            "import_path": "my_module.MyClass",
            "num_replicas": None,
            "route_prefix": None,
            "max_concurrent_queries": None,
            "user_config": None,
            "autoscaling_config": None,
            "graceful_shutdown_wait_loop_s": None,
            "graceful_shutdown_timeout_s": None,
            "health_check_period_s": None,
            "health_check_timeout_s": None,
            "ray_actor_options": {
                "runtime_env": None,
                "num_cpus": None,
                "num_gpus": None,
                "memory": None,
                "object_store_memory": None,
                "resources": None,
                "accelerator_type": None,
            },
        }

    def test_valid_deployment_schema(self):
        # Ensure a valid DeploymentSchema can be generated

        deployment_schema = {
            "name": "shallow",
            "init_args": [4, "glue"],
            "init_kwargs": {"fuel": "diesel"},
            "import_path": "test_env.shallow_import.ShallowClass",
            "num_replicas": 2,
            "route_prefix": "/shallow",
            "max_concurrent_queries": 32,
            "user_config": {"threshold": 0.2, "pattern": "rainbow"},
            "autoscaling_config": None,
            "graceful_shutdown_wait_loop_s": 17,
            "graceful_shutdown_timeout_s": 49,
            "health_check_period_s": 11,
            "health_check_timeout_s": 11,
            "ray_actor_options": {
                "runtime_env": {
                    "working_dir": (
                        "https://github.com/shrekris-anyscale/"
                        "test_module/archive/HEAD.zip"
                    ),
                    "py_modules": [
                        (
                            "https://github.com/shrekris-anyscale/"
                            "test_deploy_group/archive/HEAD.zip"
                        ),
                    ],
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

    def test_invalid_python_attributes(self):
        # Test setting invalid attributes for Python to ensure a validation or
        # value error is raised.

        # Python requires an import path
        deployment_schema = self.get_minimal_deployment_schema()
        deployment_schema["init_args"] = [1, 2]
        deployment_schema["init_kwargs"] = {"threshold": 0.5}
        del deployment_schema["import_path"]

        with pytest.raises(ValueError, match="must be specified"):
            DeploymentSchema.parse_obj(deployment_schema)

        # DeploymentSchema should be generated once import_path is set
        deployment_schema["import_path"] = "my_module.MyClass"
        DeploymentSchema.parse_obj(deployment_schema)

        # Invalid import_path syntax should raise a ValidationError
        invalid_paths = ["", "MyClass", ".", "hello,world"]
        for path in invalid_paths:
            deployment_schema["import_path"] = path
            with pytest.raises(ValidationError):
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


class TestServeInstanceSchema:
    def test_valid_serve_instance_schema(self):
        # Ensure a valid ServeInstanceSchema can be generated

        serve_instance_schema = {
            "deployments": [
                {
                    "name": "shallow",
                    "init_args": [4, "glue"],
                    "init_kwargs": {"fuel": "diesel"},
                    "import_path": "test_env.shallow_import.ShallowClass",
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
                            "working_dir": (
                                "https://github.com/shrekris-anyscale/"
                                "test_module/archive/HEAD.zip"
                            ),
                            "py_modules": [
                                (
                                    "https://github.com/shrekris-anyscale/"
                                    "test_deploy_group/archive/HEAD.zip"
                                ),
                            ],
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
                    "init_args": None,
                    "init_kwargs": None,
                    "import_path": ("test_env.subdir1.subdir2.deep_import.DeepClass"),
                    "num_replicas": None,
                    "route_prefix": None,
                    "max_concurrent_queries": None,
                    "user_config": None,
                    "autoscaling_config": None,
                    "graceful_shutdown_wait_loop_s": None,
                    "graceful_shutdown_timeout_s": None,
                    "health_check_period_s": None,
                    "health_check_timeout_s": None,
                    "ray_actor_options": {
                        "runtime_env": None,
                        "num_cpus": None,
                        "num_gpus": None,
                        "memory": None,
                        "object_store_memory": None,
                        "resources": None,
                        "accelerator_type": None,
                    },
                },
            ]
        }

        ServeInstanceSchema.parse_obj(serve_instance_schema)
