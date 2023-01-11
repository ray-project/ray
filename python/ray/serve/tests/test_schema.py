import sys
import time
import json
import pytest
import requests
from pydantic import ValidationError
from typing import List, Dict

import ray
from ray import serve
from ray.serve._private.common import (
    StatusOverview,
    DeploymentStatusInfo,
    ApplicationStatusInfo,
)
from ray.serve.schema import (
    RayActorOptionsSchema,
    DeploymentSchema,
    ServeApplicationSchema,
    ServeStatusSchema,
    serve_status_to_schema,
)
from ray.util.accelerators.accelerators import NVIDIA_TESLA_V100, NVIDIA_TESLA_P4
from ray.serve.config import AutoscalingConfig
from ray.serve.deployment import (
    deployment_to_schema,
    schema_to_deployment,
)


def get_valid_runtime_envs() -> List[Dict]:
    """Get list of runtime environments allowed in Serve config/REST API."""

    return [
        # Empty runtime_env
        {},
        # Runtime_env with remote_URIs
        {
            "working_dir": (
                "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
            ),
            "py_modules": [
                (
                    "https://github.com/shrekris-anyscale/"
                    "test_deploy_group/archive/HEAD.zip"
                ),
            ],
        },
        # Runtime_env with extra options
        {
            "working_dir": (
                "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
            ),
            "py_modules": [
                (
                    "https://github.com/shrekris-anyscale/"
                    "test_deploy_group/archive/HEAD.zip"
                ),
            ],
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
                (
                    "https://github.com/shrekris-anyscale/"
                    "test_deploy_group/archive/HEAD.zip"
                ),
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
        RayActorOptionsSchema.parse_obj(ray_actor_options_schema)

    @pytest.mark.parametrize("env", get_invalid_runtime_envs())
    def test_ray_actor_options_invalid_runtime_env(self, env):
        # Test invalid runtime_env configurations

        ray_actor_options_schema = self.get_valid_ray_actor_options_schema()
        ray_actor_options_schema["runtime_env"] = env
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

        # Schema should raise error when a nonspecified field is included
        ray_actor_options_schema["fake_field"] = None
        with pytest.raises(ValidationError):
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

    def test_extra_fields_invalid_deployment_schema(self):
        # Undefined fields should be forbidden in the schema

        deployment_schema = self.get_minimal_deployment_schema()

        # Schema should be createable with valid fields
        DeploymentSchema.parse_obj(deployment_schema)

        # Schema should raise error when a nonspecified field is included
        deployment_schema["fake_field"] = None
        with pytest.raises(ValidationError):
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

        # Schema should raise error when a nonspecified field is included
        serve_application_schema["fake_field"] = None
        with pytest.raises(ValidationError):
            ServeApplicationSchema.parse_obj(serve_application_schema)

    @pytest.mark.parametrize("env", get_valid_runtime_envs())
    def test_serve_application_valid_runtime_env(self, env):
        # Test valid runtime_env configurations

        serve_application_schema = self.get_valid_serve_application_schema()
        serve_application_schema["runtime_env"] = env
        ServeApplicationSchema.parse_obj(serve_application_schema)

    @pytest.mark.parametrize("env", get_invalid_runtime_envs())
    def test_serve_application_invalid_runtime_env(self, env):
        # Test invalid runtime_env configurations

        serve_application_schema = self.get_valid_serve_application_schema()
        serve_application_schema["runtime_env"] = env
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

    def test_serve_application_kubernetes_config(self):
        # Test kubernetes_dict() behavior

        config = {
            "import_path": "module.graph",
            "runtime_env": {"working_dir": "s3://path/file.zip"},
            "host": "1.1.1.1",
            "port": 7470,
            "deployments": [
                {
                    "name": "shallow",
                    "num_replicas": 2,
                    "route_prefix": "/shallow",
                    "user_config": {"a": 1, "b": "c", 2: 3},
                    "ray_actor_options": {
                        "runtime_env": {
                            "py_modules": ["gs://fake2/file2.zip"],
                        },
                        "num_cpus": 3,
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

        kubernetes_config = ServeApplicationSchema.parse_obj(config).kubernetes_dict(
            exclude_unset=True
        )

        assert kubernetes_config == {
            "importPath": "module.graph",
            "runtimeEnv": json.dumps({"working_dir": "s3://path/file.zip"}),
            "host": "1.1.1.1",
            "port": 7470,
            "deployments": [
                {
                    "name": "shallow",
                    "numReplicas": 2,
                    "routePrefix": "/shallow",
                    "userConfig": json.dumps({"a": 1, "b": "c", 2: 3}),
                    "rayActorOptions": {
                        "runtimeEnv": json.dumps(
                            {
                                "py_modules": ["gs://fake2/file2.zip"],
                            }
                        ),
                        "numCpus": 3.0,
                        "memory": 5.0,
                        "objectStoreMemory": 3.0,
                        "resources": json.dumps({"custom_asic": 8}),
                        "acceleratorType": NVIDIA_TESLA_P4,
                    },
                },
                {
                    "name": "deep",
                },
            ],
        }


class TestServeStatusSchema:
    def get_valid_serve_status_schema(self):
        return StatusOverview(
            app_status=ApplicationStatusInfo(
                status="DEPLOYING",
                message="",
                deployment_timestamp=time.time(),
            ),
            deployment_statuses=[
                DeploymentStatusInfo(
                    name="deployment_1",
                    status="HEALTHY",
                    message="",
                ),
                DeploymentStatusInfo(
                    name="deployment_2",
                    status="UNHEALTHY",
                    message="this deployment is deeply unhealthy",
                ),
            ],
        )

    def test_valid_serve_status_schema(self):
        # Ensure a valid ServeStatusSchema can be generated

        serve_status_schema = self.get_valid_serve_status_schema()
        serve_status_to_schema(serve_status_schema)

    def test_extra_fields_invalid_serve_status_schema(self):
        # Undefined fields should be forbidden in the schema

        serve_status_schema = self.get_valid_serve_status_schema()

        # Schema should be createable with valid fields
        serve_status_to_schema(serve_status_schema)

        # Schema should raise error when a nonspecified field is included
        with pytest.raises(ValidationError):
            ServeStatusSchema(
                app_status=serve_status_schema.app_status,
                deployment_statuses=[],
                fake_field=None,
            )


# This function is defined globally to be accessible via import path
def global_f():
    return "Hello world!"


def test_deployment_to_schema_to_deployment():
    @serve.deployment(
        num_replicas=3,
        route_prefix="/hello",
        ray_actor_options={
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
    assert deployment.ray_actor_options["runtime_env"]["working_dir"] == (
        "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
    )
    assert deployment.ray_actor_options["runtime_env"]["py_modules"] == [
        "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip",
        "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip",
    ]

    serve.start()
    deployment.deploy()
    assert ray.get(deployment.get_handle().remote()) == "Hello world!"
    assert requests.get("http://localhost:8000/hello").text == "Hello world!"
    serve.shutdown()


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

    assert len(deployment.ray_actor_options) == 0


def test_status_schema_helpers():
    @serve.deployment(
        num_replicas=1,
        route_prefix="/hello",
    )
    def f1():
        # The body of this function doesn't matter. See the comment in
        # test_deployment_to_schema_to_deployment.
        pass

    @serve.deployment(
        num_replicas=2,
        route_prefix="/hi",
    )
    def f2():
        pass

    client = serve.start()
    serve.run(f1.bind(), name="f1")
    serve.run(f2.bind(), name="f2")

    # Check statuses
    f1_statuses = serve_status_to_schema(
        client.get_serve_status("f1")
    ).deployment_statuses
    f2_statuses = serve_status_to_schema(
        client.get_serve_status("f2")
    ).deployment_statuses
    assert len(f1_statuses) == 1
    assert f1_statuses[0].status in {"UPDATING", "HEALTHY"}
    assert f1_statuses[0].name == "f1"
    assert len(f2_statuses) == 1
    assert f2_statuses[0].status in {"UPDATING", "HEALTHY"}
    assert f2_statuses[0].name == "f2"

    serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
