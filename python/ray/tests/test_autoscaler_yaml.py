import jsonschema
import os
import sys
import tempfile
import unittest
import urllib
import yaml
import copy
from unittest.mock import MagicMock, Mock, patch
import pytest

from ray.autoscaler._private.util import prepare_config, validate_config
from ray.autoscaler._private.providers import _NODE_PROVIDERS
from ray.autoscaler._private.kubernetes.node_provider import\
    KubernetesNodeProvider

from ray.test_utils import load_test_config, recursive_fnmatch

RAY_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
CONFIG_PATHS = recursive_fnmatch(
    os.path.join(RAY_PATH, "autoscaler"), "*.yaml")

CONFIG_PATHS += recursive_fnmatch(
    os.path.join(RAY_PATH, "tune", "examples"), "*.yaml")


def ignore_k8s_operator_configs(paths):
    return [
        path for path in paths if "kubernetes/operator_configs" not in path
        and "kubernetes/job-example.yaml" not in path
    ]


CONFIG_PATHS = ignore_k8s_operator_configs(CONFIG_PATHS)


class AutoscalingConfigTest(unittest.TestCase):
    def testValidateDefaultConfig(self):
        for config_path in CONFIG_PATHS:
            if "aws/example-multi-node-type.yaml" in config_path:
                # aws is tested in testValidateDefaultConfigAWSMultiNodeTypes.
                continue
            with open(config_path) as f:
                config = yaml.safe_load(f)
            config = prepare_config(config)
            if config["provider"]["type"] == "kubernetes":
                KubernetesNodeProvider.fillout_available_node_types_resources(
                    config)
            try:
                validate_config(config)
            except Exception:
                self.fail(
                    f"Config {config_path} did not pass validation test!")

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Fails on Windows.")
    def testValidateDefaultConfigMinMaxWorkers(self):
        aws_config_path = os.path.join(
            RAY_PATH, "autoscaler/aws/example-multi-node-type.yaml")
        with open(aws_config_path) as f:
            config = yaml.safe_load(f)
        config = prepare_config(config)
        for node_type in config["available_node_types"]:
            config["available_node_types"][node_type]["resources"] = config[
                "available_node_types"][node_type].get("resources", {})
        try:
            validate_config(config)
        except Exception:
            self.fail("Config did not pass validation test!")

        config["max_workers"] = 0  # the sum of min_workers is 1.
        with pytest.raises(ValueError):
            validate_config(config)

        # make sure edge case of exactly 1 passes too.
        config["max_workers"] = 1
        try:
            validate_config(config)
        except Exception:
            self.fail("Config did not pass validation test!")

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Fails on Windows.")
    def testValidateDefaultConfigAWSMultiNodeTypes(self):
        aws_config_path = os.path.join(
            RAY_PATH, "autoscaler/aws/example-multi-node-type.yaml")
        with open(aws_config_path) as f:
            config = yaml.safe_load(f)
        new_config = copy.deepcopy(config)
        # modify it here
        new_config["available_node_types"] = {
            "cpu_4_ondemand": new_config["available_node_types"][
                "cpu_4_ondemand"],
            "cpu_16_spot": new_config["available_node_types"]["cpu_16_spot"],
            "gpu_8_ondemand": new_config["available_node_types"][
                "gpu_8_ondemand"]
        }
        orig_new_config = copy.deepcopy(new_config)
        expected_available_node_types = orig_new_config["available_node_types"]
        expected_available_node_types["cpu_4_ondemand"]["resources"] = {
            "CPU": 4
        }
        expected_available_node_types["cpu_16_spot"]["resources"] = {
            "CPU": 16,
            "Custom1": 1,
            "is_spot": 1
        }
        expected_available_node_types["gpu_8_ondemand"]["resources"] = {
            "CPU": 32,
            "GPU": 4,
            "accelerator_type:V100": 1
        }

        boto3_dict = {
            "InstanceTypes": [{
                "InstanceType": "m4.xlarge",
                "VCpuInfo": {
                    "DefaultVCpus": 4
                }
            }, {
                "InstanceType": "m4.4xlarge",
                "VCpuInfo": {
                    "DefaultVCpus": 16
                }
            }, {
                "InstanceType": "p3.8xlarge",
                "VCpuInfo": {
                    "DefaultVCpus": 32
                },
                "GpuInfo": {
                    "Gpus": [{
                        "Name": "V100",
                        "Count": 4
                    }]
                }
            }]
        }
        boto3_mock = Mock()
        describe_instance_types_mock = Mock()
        describe_instance_types_mock.describe_instance_types = MagicMock(
            return_value=boto3_dict)
        boto3_mock.client = MagicMock(
            return_value=describe_instance_types_mock)
        with patch.multiple(
                "ray.autoscaler._private.aws.node_provider",
                boto3=boto3_mock,
        ):
            new_config = prepare_config(new_config)
            importer = _NODE_PROVIDERS.get(new_config["provider"]["type"])
            provider_cls = importer(new_config["provider"])

            try:
                new_config = \
                    provider_cls.fillout_available_node_types_resources(
                        new_config)
                validate_config(new_config)
                expected_available_node_types == new_config[
                    "available_node_types"]
            except Exception:
                self.fail(
                    "Config did not pass multi node types auto fill test!")

    def testValidateNetworkConfig(self):
        web_yaml = "https://raw.githubusercontent.com/ray-project/ray/" \
            "master/python/ray/autoscaler/aws/example-full.yaml"
        response = urllib.request.urlopen(web_yaml, timeout=5)
        content = response.read()
        with tempfile.TemporaryFile() as f:
            f.write(content)
            f.seek(0)
            config = yaml.safe_load(f)
        config = prepare_config(config)
        try:
            validate_config(config)
        except Exception:
            self.fail("Config did not pass validation test!")

    def _test_invalid_config(self, config_path):
        with open(os.path.join(RAY_PATH, config_path)) as f:
            config = yaml.safe_load(f)
        try:
            validate_config(config)
            self.fail("Expected validation to fail for {}".format(config_path))
        except jsonschema.ValidationError:
            pass

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testInvalidConfig(self):
        self._test_invalid_config(
            os.path.join("tests", "additional_property.yaml"))

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testValidateCustomSecurityGroupConfig(self):
        aws_config_path = os.path.join(RAY_PATH,
                                       "autoscaler/aws/example-minimal.yaml")
        with open(aws_config_path) as f:
            config = yaml.safe_load(f)

        # Test validate security group with custom permissions
        ip_permissions = [{
            "FromPort": port,
            "ToPort": port,
            "IpProtocol": "TCP",
            "IpRanges": [{
                "CidrIp": "0.0.0.0/0"
            }],
        } for port in [80, 443, 8265]]
        config["provider"].update({
            "security_group": {
                "IpPermissions": ip_permissions
            }
        })
        config = prepare_config(copy.deepcopy(config))
        try:
            validate_config(config)
            assert config["provider"]["security_group"][
                "IpPermissions"] == ip_permissions
        except Exception:
            self.fail(
                "Failed to validate config with security group in bound rules!"
            )

        # Test validate security group with custom name
        group_name = "test_security_group_name"
        config["provider"]["security_group"].update({"GroupName": group_name})

        try:
            validate_config(config)
            assert config["provider"]["security_group"][
                "GroupName"] == group_name
        except Exception:
            self.fail("Failed to validate config with security group name!")

    def testMaxWorkerDefault(self):

        # Load config, call prepare config, check that default max_workers
        # is filled correctly for node types that don't specify it.
        # Check that max_workers is untouched for node types
        # that do specify it.
        config = load_test_config("test_multi_node.yaml")
        node_types = config["available_node_types"]

        # Max workers initially absent for this node type.
        assert "max_workers" not in node_types["worker_node_max_unspecified"]
        # Max workers specified for this node type.
        assert "max_workers" in node_types["worker_node_max_specified"]

        prepared_config = prepare_config(config)
        prepared_node_types = prepared_config["available_node_types"]
        # Max workers unchanged.
        assert node_types["worker_node_max_specified"]["max_workers"] ==\
            prepared_node_types["worker_node_max_specified"][
                "max_workers"] == 3
        # Max workers auto-filled with specified cluster-wide value of 5.
        assert config["max_workers"] ==\
            prepared_node_types["worker_node_max_unspecified"]["max_workers"]\
            == config["max_workers"] == 5

        # Repeat with a config that doesn't specify global max workers.
        # Default value of 2 should be pulled in for global max workers.
        config = load_test_config("test_multi_node.yaml")
        # Delete global max_workers so it can be autofilled with default of 2.
        del config["max_workers"]
        node_types = config["available_node_types"]

        # Max workers initially absent for this node type.
        assert "max_workers" not in node_types["worker_node_max_unspecified"]
        # Max workers specified for this node type.
        assert "max_workers" in node_types["worker_node_max_specified"]

        prepared_config = prepare_config(config)
        prepared_node_types = prepared_config["available_node_types"]
        # Max workers unchanged.
        assert node_types["worker_node_max_specified"]["max_workers"] ==\
            prepared_node_types["worker_node_max_specified"][
                "max_workers"] == 3
        # Max workers auto-filled with default cluster-wide value of 2.
        assert prepared_node_types["worker_node_max_unspecified"][
            "max_workers"] == 2


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
