import sys
import requests
import pytest
from pydantic import ValidationError
from typing import List, Dict

import ray
from ray import serve
from ray.serve.schema import (
    RayActorOptionsSchema,
    DeploymentSchema,
    DeploymentStatusSchema,
    ServeApplicationSchema,
    ServeApplicationStatusSchema,
    status_info_to_schema,
    serve_application_status_to_schema,
)
from ray.util.accelerators.accelerators import NVIDIA_TESLA_V100, NVIDIA_TESLA_P4
from ray.serve.config import AutoscalingConfig
from ray.serve.common import DeploymentStatus, DeploymentStatusInfo
from ray.serve.api import get_deployment_statuses
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

        return {
            "name": "deep",
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
                "runtime_env": {},
                "num_cpus": None,
                "num_gpus": None,
                "memory": None,
                "object_store_memory": None,
                "resources": {},
                "accelerator_type": None,
            },
        }

    def test_valid_deployment_schema(self):
        # Ensure a valid DeploymentSchema can be generated

        deployment_schema = {
            "name": "shallow",
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
        del deployment_schema["import_path"]

        with pytest.raises(ValueError, match="must be specified"):
            DeploymentSchema.parse_obj(deployment_schema)

        # DeploymentSchema should be generated once import_path is set
        for path in get_valid_import_paths():
            deployment_schema["import_path"] = path
            DeploymentSchema.parse_obj(deployment_schema)

        # Invalid import_path syntax should raise a ValidationError
        for path in get_invalid_import_paths():
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

    def test_extra_fields_invalid_deployment_schema(self):
        # Undefined fields should be forbidden in the schema

        deployment_schema = self.get_minimal_deployment_schema()

        # Schema should be createable with valid fields
        DeploymentSchema.parse_obj(deployment_schema)

        # Schema should raise error when a nonspecified field is included
        deployment_schema["fake_field"] = None
        with pytest.raises(ValidationError):
            DeploymentSchema.parse_obj(deployment_schema)


class TestServeApplicationSchema:
    def get_valid_serve_application_schema(self):
        return {
            "import_path": "module.graph",
            "runtime_env": {},
            "deployments": [
                {
                    "name": "shallow",
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
                        "runtime_env": {},
                        "num_cpus": None,
                        "num_gpus": None,
                        "memory": None,
                        "object_store_memory": None,
                        "resources": {},
                        "accelerator_type": None,
                    },
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


class TestDeploymentStatusSchema:
    def get_valid_deployment_status_schema(self):
        return {
            "deployment_1": DeploymentStatusInfo(DeploymentStatus.HEALTHY),
            "deployment_2": DeploymentStatusInfo(
                DeploymentStatus.UNHEALTHY, "This is an unhealthy deployment."
            ),
            "deployment_3": DeploymentStatusInfo(DeploymentStatus.UPDATING),
        }

    def test_valid_deployment_status_schema(self):
        # Ensure valid DeploymentStatusSchemas can be generated

        deployment_status_schemas = self.get_valid_deployment_status_schema()

        for name, status_info in deployment_status_schemas.items():
            status_info_to_schema(name, status_info)

    def test_invalid_status(self):
        # Ensure a DeploymentStatusSchema cannot be initialized with an invalid status

        status_info = {
            "status": "nonexistent status",
            "message": "welcome to nonexistence",
        }
        with pytest.raises(ValidationError):
            status_info_to_schema("deployment name", status_info)

    def test_extra_fields_invalid_deployment_status_schema(self):
        # Undefined fields should be forbidden in the schema

        deployment_status_schemas = self.get_valid_deployment_status_schema()

        # Schema should be createable with valid fields
        for name, status_info in deployment_status_schemas.items():
            DeploymentStatusSchema(
                name=name, status=status_info.status, message=status_info.message
            )

        # Schema should raise error when a nonspecified field is included
        for name, status_info in deployment_status_schemas.items():
            with pytest.raises(ValidationError):
                DeploymentStatusSchema(
                    name=name,
                    status=status_info.status,
                    message=status_info.message,
                    fake_field=None,
                )


class TestServeApplicationStatusSchema:
    def get_valid_serve_application_status_schema(self):
        return {
            "deployment_1": {"status": "HEALTHY", "message": ""},
            "deployment_2": {
                "status": "UNHEALTHY",
                "message": "this deployment is deeply unhealthy",
            },
        }

    def test_valid_serve_application_status_schema(self):
        # Ensure a valid ServeApplicationStatusSchema can be generated

        serve_application_status_schema = (
            self.get_valid_serve_application_status_schema()
        )
        serve_application_status_to_schema(serve_application_status_schema)

    def test_extra_fields_invalid_serve_application_status_schema(self):
        # Undefined fields should be forbidden in the schema

        serve_application_status_schema = (
            self.get_valid_serve_application_status_schema()
        )

        # Schema should be createable with valid fields
        serve_application_status_to_schema(serve_application_status_schema)

        # Schema should raise error when a nonspecified field is included
        with pytest.raises(ValidationError):
            statuses = [
                status_info_to_schema(name, status_info)
                for name, status_info in serve_application_status_schema.items()
            ]
            ServeApplicationStatusSchema(statuses=statuses, fake_field=None)


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

    f._func_or_class = "ray.serve.tests.test_schema.global_f"

    deployment = schema_to_deployment(deployment_to_schema(f))

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

    f._func_or_class = "ray.serve.tests.test_schema.global_f"

    deployment = schema_to_deployment(deployment_to_schema(f))

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

    f1._func_or_class = "ray.serve.tests.test_schema.global_f"
    f2._func_or_class = "ray.serve.tests.test_schema.global_f"

    serve.start()

    f1.deploy()
    f2.deploy()

    # Check statuses
    statuses = serve_application_status_to_schema(get_deployment_statuses()).statuses
    deployment_names = {"f1", "f2"}
    for deployment_status in statuses:
        assert deployment_status.status in {"UPDATING", "HEALTHY"}
        assert deployment_status.name in deployment_names
        deployment_names.remove(deployment_status.name)
    assert len(deployment_names) == 0

    serve.shutdown()


@serve.deployment
def decorated_f(*args):
    return "reached decorated_f"


def test_use_deployment_import_path():
    """Ensure deployment func_or_class becomes import path when schematized."""

    d = schema_to_deployment(deployment_to_schema(decorated_f))

    assert isinstance(d.func_or_class, str)

    # CI may change the parent path, so check only that the suffix matches.
    assert d.func_or_class.endswith("ray.serve.tests.test_schema.decorated_f")

    serve.start()
    d.deploy()
    assert (
        requests.get("http://localhost:8000/decorated_f").text == "reached decorated_f"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
