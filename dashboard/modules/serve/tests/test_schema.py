from pydantic import ValidationError
import pytest

from ray.dashboard.modules.serve.schema import (
    SupportedLanguage,
    AppConfig,
    DeploymentConfig,
    ReplicaResources,
    FullDeploymentConfig,
    ServeInstanceConfig,
)


class TestAppConfig:

    def test_invalid_language(self):
        # Unsupported languages should raise a ValidationError
        with pytest.raises(ValidationError):
            AppConfig(language="ocaml")
    
    def test_invalid_python_attributes(self):
        # Test setting invalid attributes for Python to ensure a validation or
        # value error is raised.

        # Python versions 3.6 through Python 3.10 require an import path
        for minor_version in range(6, 11):
            app_config = {
                "language": f"python_3.{minor_version}",
                "init_args": [1, 2],
                "init_kwargs": {
                    "threshold": 0.5,
                    "version": "abcd"
                },
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
                AppConfig(language="python_3.8", import_path=path)
    

class TestDeploymentConfig:

    def test_gt_zero(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than zero is set to zero.

        gt_zero_fields = ["num_replicas", "max_concurrent_queries", 
                          "health_check_period_s", "health_check_timeout_s"]
        for field in gt_zero_fields:
            print("check1", field)
            with pytest.raises(ValidationError):
                DeploymentConfig.parse_obj({field: 0})
    
    def test_ge_zero(self):
        # Ensure ValidationError is raised when any fields that must be greater
        # than or equal to zero is set to -1.

        ge_zero_fields = ["graceful_shutdown_wait_loop_s",
                          "graceful_shutdown_timeout_s"]
        for field in ge_zero_fields:
            print("check2", field)
            with pytest.raises(ValidationError):
                DeploymentConfig.parse_obj({field: -1})
    
    def test_valid_deployment_config(self):
        # Ensure a valid DeploymentConfig can be generated

        deployment_config = {
            "num_replicas": 5,
            "route_prefix": "/hello",
            "max_concurrent_queries": 20,
            "user_config": {
                "threshold": 0.2,
                "pattern": "rainbow"
            },
            "graceful_shutdown_wait_loop_s": 50,
            "graceful_shutdown_timeout_s": 20,
            "health_check_period_s": 10,
            "health_check_timeout_s": 30
        }

        # Should not raise an error
        DeploymentConfig.parse_obj(deployment_config)
