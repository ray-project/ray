from pydantic import ValidationError
import pytest

from ray.dashboard.modules.serve.schema import (
    AppConfig,
    DeploymentConfig,
    RayActorOptions,
    FullDeploymentConfig,
    ServeInstanceConfig,
)
from ray.util.accelerators.accelerators import NVIDIA_TESLA_V100, NVIDIA_TESLA_P4


class TestAppConfig:
    def test_invalid_python_attributes(self):
        # Test setting invalid attributes for Python to ensure a validation or
        # value error is raised.

        # Python requires an import path
        app_config = {
            "init_args": [1, 2],
            "init_kwargs": {"threshold": 0.5, "version": "abcd"},
        }

        with pytest.raises(ValueError, match="must be specified"):
            AppConfig.parse_obj(app_config)

        # AppConfig should be generated once import_path is set
        app_config["import_path"] = "my_module.MyClass"
        AppConfig.parse_obj(app_config)

        # Invalid import_path syntax should raise a ValidationError
        invalid_paths = ["", "MyClass", ".", "hello,world"]
        for path in invalid_paths:
            with pytest.raises(ValidationError):
                AppConfig(import_path=path)


class TestDeploymentConfig:
    def test_valid_deployment_config(self):
        # Ensure a valid DeploymentConfig can be generated

        deployment_config = {
            "num_replicas": 5,
            "route_prefix": "/hello",
            "max_concurrent_queries": 20,
            "user_config": {"threshold": 0.2, "pattern": "rainbow"},
            "graceful_shutdown_wait_loop_s": 50,
            "graceful_shutdown_timeout_s": 20,
            "health_check_period_s": 10,
            "health_check_timeout_s": 30,
        }

        # Should not raise an error
        DeploymentConfig.parse_obj(deployment_config)

    def test_gt_zero_deployment_config(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than zero is set to zero.

        gt_zero_fields = [
            "num_replicas",
            "max_concurrent_queries",
            "health_check_period_s",
            "health_check_timeout_s",
        ]
        for field in gt_zero_fields:
            with pytest.raises(ValidationError):
                DeploymentConfig.parse_obj({field: 0})

    def test_ge_zero_deployment_config(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than or equal to zero is set to -1.

        ge_zero_fields = [
            "graceful_shutdown_wait_loop_s",
            "graceful_shutdown_timeout_s",
        ]
        for field in ge_zero_fields:
            with pytest.raises(ValidationError):
                DeploymentConfig.parse_obj({field: -1})

    def test_route_prefix(self):
        # route_prefix must start with a "/"
        with pytest.raises(ValueError):
            DeploymentConfig(route_prefix="hello/world")

        # route_prefix must end with a "/"
        with pytest.raises(ValueError):
            DeploymentConfig(route_prefix="/hello/world/")

        # route_prefix cannot contain wildcards, meaning it can't have
        # "{" or "}"
        with pytest.raises(ValueError):
            DeploymentConfig(route_prefix="/hello/{adjective}/world/")

        # Ensure a valid route_prefix works
        DeploymentConfig(route_prefix="/hello/wonderful/world")

        # Ensure route_prefix of "/" works
        DeploymentConfig(route_prefix="/")

        # Ensure route_prefix of None works
        DeploymentConfig(route_prefix=None)


class TestRayActorOptions:
    def test_valid_ray_actor_options(self):
        # Ensure a valid RayActorOptions can be generated

        ray_actor_options = {
            "num_cpus": 0.2,
            "num_gpus": 50,
            "memory": 3,
            "object_store_memory": 64,
            "resources": {"custom_asic": 12},
            "accelerator_type": NVIDIA_TESLA_V100,
        }

        RayActorOptions.parse_obj(ray_actor_options)

    def test_gt_zero_ray_actor_options(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than zero is set to zero.

        gt_zero_fields = ["num_cpus", "num_gpus", "memory", "object_store_memory"]
        for field in gt_zero_fields:
            with pytest.raises(ValidationError):
                RayActorOptions.parse_obj({field: 0})


class TestFullDeploymentConfig:
    def test_valid_full_deployment_config(self):
        # Ensure a valid FullDeploymentConfig can be generated

        full_deployment_config = {
            "name": "shallow",
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
            "app_config": {
                "init_args": [4, "glue"],
                "init_kwargs": {"fuel": "diesel"},
                "import_path": "test_env.shallow_import.ShallowClass",
            },
            "deployment_config": {
                "num_replicas": 2,
                "route_prefix": "/shallow",
                "max_concurrent_queries": 32,
                "user_config": None,
                "_autoscaling_config": None,
                "_graceful_shutdown_wait_loop_s": 17,
                "_graceful_shutdown_timeout_s": 49,
                "_health_check_period_s": 11,
                "_health_check_timeout_s": 11,
            },
            "ray_actor_options": {
                "num_cpus": 3,
                "num_gpus": 4.2,
                "memory": 5,
                "object_store_memory": 3,
                "resources": {"custom_asic": 8},
                "accelerator_type": NVIDIA_TESLA_P4,
            },
        }

        FullDeploymentConfig.parse_obj(full_deployment_config)

    def test_runtime_env(self):
        # Test different runtime_env configurations

        full_deployment_config = {
            "name": "shallow",
            "runtime_env": None,
            "app_config": {
                "init_args": None,
                "init_kwargs": None,
                "import_path": "test_env.shallow_import.ShallowClass",
            },
            "deployment_config": {
                "num_replicas": None,
                "route_prefix": None,
                "max_concurrent_queries": None,
                "user_config": None,
                "_autoscaling_config": None,
                "_graceful_shutdown_wait_loop_s": None,
                "_graceful_shutdown_timeout_s": None,
                "_health_check_period_s": None,
                "_health_check_timeout_s": None,
            },
            "ray_actor_options": {
                "num_cpus": None,
                "num_gpus": None,
                "memory": None,
                "object_store_memory": None,
                "resources": None,
                "accelerator_type": None,
            },
        }

        # full_deployment_config should work as is
        FullDeploymentConfig.parse_obj(full_deployment_config)

        # working_dir and py_modules cannot contain local uris
        full_deployment_config["runtime_env"] = {
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
            FullDeploymentConfig.parse_obj(full_deployment_config)


class TestServeInstanceConfig:
    def test_valid_serve_instance_config(self):
        # Ensure a valid ServeInstanceConfig can be generated

        serve_instance_config = {
            "deployments": [
                {
                    "name": "shallow",
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
                    "app_config": {
                        "init_args": [4, "glue"],
                        "init_kwargs": {"fuel": "diesel"},
                        "import_path": "test_env.shallow_import.ShallowClass",
                    },
                    "deployment_config": {
                        "num_replicas": 2,
                        "route_prefix": "/shallow",
                        "max_concurrent_queries": 32,
                        "user_config": None,
                        "_autoscaling_config": None,
                        "_graceful_shutdown_wait_loop_s": 17,
                        "_graceful_shutdown_timeout_s": 49,
                        "_health_check_period_s": 11,
                        "_health_check_timeout_s": 11,
                    },
                    "ray_actor_options": {
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
                    "runtime_env": None,
                    "app_config": {
                        "init_args": None,
                        "init_kwargs": None,
                        "import_path": (
                            "test_env.subdir1.subdir2.deep_import.DeepClass"
                        ),
                    },
                    "deployment_config": {
                        "num_replicas": None,
                        "route_prefix": None,
                        "max_concurrent_queries": None,
                        "user_config": None,
                        "_autoscaling_config": None,
                        "_graceful_shutdown_wait_loop_s": None,
                        "_graceful_shutdown_timeout_s": None,
                        "_health_check_period_s": None,
                        "_health_check_timeout_s": None,
                    },
                    "ray_actor_options": {
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

        ServeInstanceConfig.parse_obj(serve_instance_config)
