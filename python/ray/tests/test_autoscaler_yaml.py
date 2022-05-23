import jsonschema
import logging
import mock
import os
import sys
import tempfile
import unittest
import urllib
import yaml
import copy
from unittest.mock import MagicMock, Mock, patch
import pytest
from click.exceptions import ClickException

from ray.autoscaler._private._azure.config import (
    _configure_key_pair as _azure_configure_key_pair,
)
from ray.autoscaler._private.gcp import config as gcp_config
from ray.autoscaler._private.util import (
    prepare_config,
    validate_config,
    _get_default_config,
    merge_setup_commands,
    fill_node_type_min_max_workers,
)
from ray.autoscaler._private.providers import _NODE_PROVIDERS
from ray.autoscaler._private._kubernetes.node_provider import KubernetesNodeProvider
from ray.autoscaler.tags import NODE_TYPE_LEGACY_HEAD, NODE_TYPE_LEGACY_WORKER

from ray._private.test_utils import load_test_config, recursive_fnmatch

RAY_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
CONFIG_PATHS = recursive_fnmatch(os.path.join(RAY_PATH, "autoscaler"), "*.yaml")

CONFIG_PATHS += recursive_fnmatch(os.path.join(RAY_PATH, "tune", "examples"), "*.yaml")


def ignore_k8s_operator_configs(paths):
    return [
        path
        for path in paths
        if "kubernetes/operator_configs" not in path
        and "kubernetes/job-example.yaml" not in path
    ]


CONFIG_PATHS = ignore_k8s_operator_configs(CONFIG_PATHS)

EXPECTED_LOCAL_CONFIG_STR = """
cluster_name: minimal-manual
provider:
  head_ip: xxx.yyy
  type: local
  worker_ips:
  - aaa.bbb
  - ccc.ddd
  - eee.fff
auth:
  ssh_private_key: ~/.ssh/id_rsa
  ssh_user: user
docker: {}
max_workers: 3
available_node_types:
  local.cluster.node:
    max_workers: 3
    min_workers: 3
    node_config: {}
    resources: {}
head_node_type: local.cluster.node
head_start_ray_commands:
- ray stop
- ulimit -c unlimited; ray start --head --port=6379 --autoscaling-config=~/ray_bootstrap_config.yaml
worker_start_ray_commands:
- ray stop
- ray start --address=$RAY_HEAD_IP:6379
cluster_synced_files: []
idle_timeout_minutes: 5
upscaling_speed: 1.0
file_mounts: {}
file_mounts_sync_continuously: false
head_setup_commands: []
initialization_commands: []
rsync_exclude: []
rsync_filter: []
setup_commands: []
worker_setup_commands: []
"""  # noqa E501


class AutoscalingConfigTest(unittest.TestCase):
    def testValidateDefaultConfig(self):
        for config_path in CONFIG_PATHS:
            try:
                if "aws/example-multi-node-type.yaml" in config_path:
                    # aws tested in testValidateDefaultConfigAWSMultiNodeTypes.
                    continue
                if "local" in config_path:
                    # local tested in testValidateLocal
                    continue
                if "fake_multi_node" in config_path:
                    # not supported with ray up
                    continue
                if "kuberay" in config_path:
                    # not supported with ray up
                    continue
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                config = prepare_config(config)
                if config["provider"]["type"] == "kubernetes":
                    KubernetesNodeProvider.fillout_available_node_types_resources(
                        config
                    )
                validate_config(config)
            except Exception:
                logging.exception("")
                self.fail(f"Config {config_path} did not pass validation test!")

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testValidateDefaultConfigMinMaxWorkers(self):
        aws_config_path = os.path.join(
            RAY_PATH, "autoscaler/aws/example-multi-node-type.yaml"
        )
        with open(aws_config_path) as f:
            config = yaml.safe_load(f)
        config = prepare_config(config)
        for node_type in config["available_node_types"]:
            config["available_node_types"][node_type]["resources"] = config[
                "available_node_types"
            ][node_type].get("resources", {})
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

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testValidateDefaultConfigAWSMultiNodeTypes(self):
        aws_config_path = os.path.join(
            RAY_PATH, "autoscaler/aws/example-multi-node-type.yaml"
        )
        with open(aws_config_path) as f:
            config = yaml.safe_load(f)
        new_config = copy.deepcopy(config)
        # modify it here
        new_config["available_node_types"] = {
            "cpu_4_ondemand": new_config["available_node_types"]["cpu_4_ondemand"],
            "cpu_16_spot": new_config["available_node_types"]["cpu_16_spot"],
            "gpu_8_ondemand": new_config["available_node_types"]["gpu_8_ondemand"],
        }
        orig_new_config = copy.deepcopy(new_config)
        expected_available_node_types = orig_new_config["available_node_types"]
        expected_available_node_types["cpu_4_ondemand"]["resources"] = {"CPU": 4}
        expected_available_node_types["cpu_16_spot"]["resources"] = {
            "CPU": 16,
            "memory": 41231686041,
            "Custom1": 1,
            "is_spot": 1,
        }
        expected_available_node_types["gpu_8_ondemand"]["resources"] = {
            "CPU": 32,
            "memory": 157195803033,
            "GPU": 4,
            "accelerator_type:V100": 1,
        }

        boto3_dict = {
            "InstanceTypes": [
                {
                    "InstanceType": "m4.xlarge",
                    "VCpuInfo": {"DefaultVCpus": 4},
                    "MemoryInfo": {"SizeInMiB": 16384},
                },
                {
                    "InstanceType": "m4.4xlarge",
                    "VCpuInfo": {"DefaultVCpus": 16},
                    "MemoryInfo": {"SizeInMiB": 65536},
                },
                {
                    "InstanceType": "p3.8xlarge",
                    "VCpuInfo": {"DefaultVCpus": 32},
                    "MemoryInfo": {"SizeInMiB": 249856},
                    "GpuInfo": {"Gpus": [{"Name": "V100", "Count": 4}]},
                },
            ]
        }
        describe_instance_types_mock = Mock()
        describe_instance_types_mock.describe_instance_types = MagicMock(
            return_value=boto3_dict
        )
        client_cache_mock = MagicMock(return_value=describe_instance_types_mock)
        with patch.multiple(
            "ray.autoscaler._private.aws.node_provider",
            client_cache=client_cache_mock,
        ):
            new_config = prepare_config(new_config)
            importer = _NODE_PROVIDERS.get(new_config["provider"]["type"])
            provider_cls = importer(new_config["provider"])

            try:
                new_config = provider_cls.fillout_available_node_types_resources(
                    new_config
                )
                validate_config(new_config)
                expected_available_node_types == new_config["available_node_types"]
            except Exception:
                self.fail("Config did not pass multi node types auto fill test!")

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testValidateLocal(self):
        """
        Tests local node provider config validation for the most common use
        case of bootstrapping a cluster at a static set of ips.
        """
        local_config_path = os.path.join(
            RAY_PATH, "autoscaler/local/example-minimal-manual.yaml"
        )
        base_config = yaml.safe_load(open(local_config_path).read())
        base_config["provider"]["head_ip"] = "xxx.yyy"
        base_config["provider"]["worker_ips"] = ["aaa.bbb", "ccc.ddd", "eee.fff"]
        base_config["auth"]["ssh_user"] = "user"
        base_config["auth"]["ssh_private_key"] = "~/.ssh/id_rsa"

        test_prepare_config = copy.deepcopy(base_config)
        prepared_config = prepare_config(test_prepare_config)
        try:
            validate_config(prepared_config)
        except Exception:
            self.fail("Failed to validate local/example-minimal-manual.yaml")
        expected_prepared = yaml.safe_load(EXPECTED_LOCAL_CONFIG_STR)
        assert prepared_config == expected_prepared

        no_worker_config = copy.deepcopy(base_config)
        del no_worker_config["provider"]["worker_ips"]
        with pytest.raises(ClickException):
            prepare_config(no_worker_config)
        no_head_config = copy.deepcopy(base_config)
        del no_head_config["provider"]["head_ip"]
        with pytest.raises(ClickException):
            prepare_config(no_head_config)
        for field in "head_node", "worker_nodes", "available_node_types":
            faulty_config = copy.deepcopy(base_config)
            faulty_config[field] = "This field shouldn't be in here."
            with pytest.raises(ClickException):
                prepare_config(faulty_config)

        too_many_workers_config = copy.deepcopy(base_config)

        # More workers requested than the three available ips.
        too_many_workers_config["max_workers"] = 10
        too_many_workers_config["min_workers"] = 10
        prepared_config = prepare_config(too_many_workers_config)

        # Check that worker config numbers were clipped to 3.
        assert prepared_config == expected_prepared

        not_enough_workers_config = copy.deepcopy(base_config)

        # Max workers is less than than the three available ips.
        # The user is probably has probably made an error. Make sure we log a warning.
        not_enough_workers_config["max_workers"] = 0
        not_enough_workers_config["min_workers"] = 0
        with mock.patch(
            "ray.autoscaler._private.local.config.cli_logger.warning"
        ) as warning:
            prepared_config = prepare_config(not_enough_workers_config)
            warning.assert_called_with(
                "The value of `max_workers` supplied (0) is less"
                " than the number of available worker ips (3)."
                " At most 0 Ray worker nodes will connect to the cluster."
            )
        expected_prepared = yaml.safe_load(EXPECTED_LOCAL_CONFIG_STR)
        # We logged a warning.
        # However, prepare_config does not repair the strange config setting:
        expected_prepared["max_workers"] = 0
        expected_prepared["available_node_types"]["local.cluster.node"][
            "max_workers"
        ] = 0
        expected_prepared["available_node_types"]["local.cluster.node"][
            "min_workers"
        ] = 0
        assert prepared_config == expected_prepared

    def testValidateNetworkConfig(self):
        web_yaml = (
            "https://raw.githubusercontent.com/ray-project/ray/"
            "master/python/ray/autoscaler/aws/example-full.yaml"
        )
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
        self._test_invalid_config(os.path.join("tests", "additional_property.yaml"))

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testValidateCustomSecurityGroupConfig(self):
        aws_config_path = os.path.join(RAY_PATH, "autoscaler/aws/example-minimal.yaml")
        with open(aws_config_path) as f:
            config = yaml.safe_load(f)

        # Test validate security group with custom permissions
        ip_permissions = [
            {
                "FromPort": port,
                "ToPort": port,
                "IpProtocol": "TCP",
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
            for port in [80, 443, 8265]
        ]
        config["provider"].update({"security_group": {"IpPermissions": ip_permissions}})
        config = prepare_config(copy.deepcopy(config))
        try:
            validate_config(config)
            assert (
                config["provider"]["security_group"]["IpPermissions"] == ip_permissions
            )
        except Exception:
            self.fail("Failed to validate config with security group in bound rules!")

        # Test validate security group with custom name
        group_name = "test_security_group_name"
        config["provider"]["security_group"].update({"GroupName": group_name})

        try:
            validate_config(config)
            assert config["provider"]["security_group"]["GroupName"] == group_name
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
        assert (
            node_types["worker_node_max_specified"]["max_workers"]
            == prepared_node_types["worker_node_max_specified"]["max_workers"]
            == 3
        )
        # Max workers auto-filled with specified cluster-wide value of 5.
        assert (
            config["max_workers"]
            == prepared_node_types["worker_node_max_unspecified"]["max_workers"]
            == 5
        )

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
        assert (
            node_types["worker_node_max_specified"]["max_workers"]
            == prepared_node_types["worker_node_max_specified"]["max_workers"]
            == 3
        )
        # Max workers auto-filled with default cluster-wide value of 2.
        assert (
            prepared_config["max_workers"]
            == prepared_node_types["worker_node_max_unspecified"]["max_workers"]
            == 2
        )

    def testFillEdgeLegacyConfigs(self):
        # Test edge cases: legacy configs which specify workers but not head
        # or vice-versa.
        no_head = load_test_config("test_no_head.yaml")
        aws_defaults = _get_default_config(no_head["provider"])
        head_prepared = prepare_config(no_head)
        assert (
            head_prepared["available_node_types"]["ray-legacy-head-node-type"][
                "node_config"
            ]
            == aws_defaults["available_node_types"]["ray.head.default"]["node_config"]
        )
        assert head_prepared["head_node"] == {}
        # Custom worker config preserved
        node_types = head_prepared["available_node_types"]
        worker_type = node_types["ray-legacy-worker-node-type"]
        assert (
            worker_type["node_config"]
            == head_prepared["worker_nodes"]
            == {"foo": "bar"}
        )

        no_workers = load_test_config("test_no_workers.yaml")
        workers_prepared = prepare_config(no_workers)
        assert (
            workers_prepared["available_node_types"]["ray-legacy-worker-node-type"][
                "node_config"
            ]
            == aws_defaults["available_node_types"]["ray.worker.default"]["node_config"]
        )
        assert workers_prepared["worker_nodes"] == {}
        # Custom head config preserved
        node_types = workers_prepared["available_node_types"]
        head_type = node_types["ray-legacy-head-node-type"]
        assert (
            head_type["node_config"] == workers_prepared["head_node"] == {"baz": "qux"}
        )

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testExampleFull(self):
        """
        Test that example-full yamls are unmodified by prepared_config,
        except possibly by having setup_commands merged and
        default per-node max/min workers set.
        """
        providers = ["aws", "gcp", "azure"]
        for provider in providers:
            path = os.path.join(RAY_PATH, "autoscaler", provider, "example-full.yaml")
            config = yaml.safe_load(open(path).read())
            config_copy = copy.deepcopy(config)
            merge_setup_commands(config_copy)
            fill_node_type_min_max_workers(config_copy)
            assert config_copy == prepare_config(config)

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testLegacyYaml(self):
        # Test correct default-merging behavior for legacy yamls.
        providers = ["aws", "azure"]
        for provider in providers:
            path = os.path.join(
                RAY_PATH, "autoscaler", provider, "example-full-legacy.yaml"
            )
            legacy_config = yaml.safe_load(open(path).read())
            # custom head and workers
            legacy_config["head_node"] = {"blahblah": 0}
            legacy_config["worker_nodes"] = {"halbhalhb": 0}
            legacy_config_copy = copy.deepcopy(legacy_config)
            prepared_legacy = prepare_config(legacy_config_copy)
            assert (
                prepared_legacy["available_node_types"][NODE_TYPE_LEGACY_HEAD][
                    "max_workers"
                ]
                == 0
            )
            assert (
                prepared_legacy["available_node_types"][NODE_TYPE_LEGACY_HEAD][
                    "min_workers"
                ]
                == 0
            )
            assert (
                prepared_legacy["available_node_types"][NODE_TYPE_LEGACY_HEAD][
                    "node_config"
                ]
                == legacy_config["head_node"]
            )

            assert (
                prepared_legacy["available_node_types"][NODE_TYPE_LEGACY_WORKER][
                    "max_workers"
                ]
                == 2
            )
            assert (
                prepared_legacy["available_node_types"][NODE_TYPE_LEGACY_WORKER][
                    "min_workers"
                ]
                == 0
            )
            assert (
                prepared_legacy["available_node_types"][NODE_TYPE_LEGACY_WORKER][
                    "node_config"
                ]
                == legacy_config["worker_nodes"]
            )

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testAzureKeyPair(self):
        azure_config_path = os.path.join(RAY_PATH, "autoscaler/azure/example-full.yaml")
        azure_config = yaml.safe_load(open(azure_config_path))
        azure_config["auth"]["ssh_user"] = "default_user"
        with tempfile.NamedTemporaryFile() as pub_key, tempfile.NamedTemporaryFile() as priv_key:  # noqa: E501
            pub_key.write(b"PUBLICKEY")
            pub_key.flush()
            priv_key.write(b"PRIVATEKEY")
            priv_key.flush()
            azure_config["auth"]["ssh_private_key"] = priv_key.name
            azure_config["auth"]["ssh_public_key"] = pub_key.name
            modified_config = _azure_configure_key_pair(azure_config)
        for node_type in modified_config["available_node_types"].values():
            assert (
                node_type["node_config"]["azure_arm_parameters"]["adminUsername"]
                == "default_user"
            )

            assert (
                node_type["node_config"]["azure_arm_parameters"]["publicKey"]
                == "PUBLICKEY"
            )

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testGCPSubnets(self):
        """Validates gcp _configure_subnet logic.

        Checks that _configure_subnet fills default networkInterfaces data for
        each node type that doesn't specify networkInterfaces.

        Checks that _list_subnets is not called if all node types specify
        networkInterfaces.
        """
        path = os.path.join(RAY_PATH, "autoscaler", "gcp", "example-full.yaml")
        config = yaml.safe_load(open(path).read())

        config_subnets_configured = copy.deepcopy(config)
        config_subnets_worker_configured = copy.deepcopy(config)
        config_subnets_head_configured = copy.deepcopy(config)
        config_subnets_no_type_configured = copy.deepcopy(config)

        config_subnets_configured["available_node_types"]["ray_head_default"][
            "node_config"
        ]["networkInterfaces"] = "mock_interfaces"
        config_subnets_configured["available_node_types"]["ray_worker_small"][
            "node_config"
        ]["networkInterfaces"] = "mock_interfaces"

        config_subnets_worker_configured["available_node_types"]["ray_worker_small"][
            "node_config"
        ]["networkInterfaces"] = "mock_interfaces"

        config_subnets_head_configured["available_node_types"]["ray_head_default"][
            "node_config"
        ]["networkInterfaces"] = "mock_interfaces"

        assert (
            "networkInterfaces"
            not in config_subnets_no_type_configured["available_node_types"][
                "ray_head_default"
            ]["node_config"]
        )
        assert (
            "networkInterfaces"
            not in config_subnets_no_type_configured["available_node_types"][
                "ray_worker_small"
            ]["node_config"]
        )

        # Configure subnets modifies configs in place so we need to copy
        # the configs for comparision after passing into the method.
        config_subnets_configured_post = copy.deepcopy(config_subnets_configured)
        config_subnets_worker_configured_post = copy.deepcopy(
            config_subnets_worker_configured
        )
        config_subnets_head_configured_post = copy.deepcopy(
            config_subnets_head_configured
        )
        config_subnets_no_type_configured_post = copy.deepcopy(
            config_subnets_no_type_configured
        )

        # Track number of times list_subnets has been called.
        list_subnets_counter = 0

        def mock_list_subnets(*args):
            nonlocal list_subnets_counter
            list_subnets_counter += 1
            return [{"selfLink": "link"}]

        # Attempting to patch produces an error in the CI.
        gcp_config._list_subnets = mock_list_subnets
        config_subnets_configured_post = gcp_config._configure_subnet(
            config_subnets_configured, compute="mock_compute"
        )
        # List subnets wasn't called
        assert list_subnets_counter == 0
        config_subnets_worker_configured_post = gcp_config._configure_subnet(
            config_subnets_worker_configured, compute="mock_compute"
        )
        # List subnets was called
        assert list_subnets_counter == 1
        config_subnets_head_configured_post = gcp_config._configure_subnet(
            config_subnets_head_configured, compute="mock_compute"
        )
        # List subnets was called
        assert list_subnets_counter == 2
        config_subnets_no_type_configured_post = gcp_config._configure_subnet(
            config_subnets_no_type_configured, compute="mock_compute"
        )
        # List subnets was called
        assert list_subnets_counter == 3

        # networkInterfaces field generated by configure subnets with mocked
        # _list_subnets
        default_interfaces = [
            {
                "subnetwork": "link",
                "accessConfigs": [
                    {
                        "name": "External NAT",
                        "type": "ONE_TO_ONE_NAT",
                    }
                ],
            }
        ]

        # Unchanged
        assert config_subnets_configured_post == config_subnets_configured
        assert (
            config_subnets_configured_post["available_node_types"]["ray_head_default"][
                "node_config"
            ]["networkInterfaces"]
            == "mock_interfaces"
        )
        assert (
            config_subnets_configured_post["available_node_types"]["ray_worker_small"][
                "node_config"
            ]["networkInterfaces"]
            == "mock_interfaces"
        )

        # Head subnets filled
        assert (
            config_subnets_worker_configured_post["available_node_types"][
                "ray_worker_small"
            ]["node_config"]["networkInterfaces"]
            == "mock_interfaces"
        )
        assert (
            config_subnets_worker_configured_post["available_node_types"][
                "ray_head_default"
            ]["node_config"]["networkInterfaces"]
            == default_interfaces
        )

        # Worker subnets filled
        assert (
            config_subnets_head_configured_post["available_node_types"][
                "ray_worker_small"
            ]["node_config"]["networkInterfaces"]
            == default_interfaces
        )
        assert (
            config_subnets_head_configured_post["available_node_types"][
                "ray_head_default"
            ]["node_config"]["networkInterfaces"]
            == "mock_interfaces"
        )

        # Head and worker subnets filled
        assert (
            config_subnets_no_type_configured_post["available_node_types"][
                "ray_worker_small"
            ]["node_config"]["networkInterfaces"]
            == default_interfaces
        )
        assert (
            config_subnets_no_type_configured_post["available_node_types"][
                "ray_head_default"
            ]["node_config"]["networkInterfaces"]
            == default_interfaces
        )

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Fails on Windows.")
    def testFaultyResourceValidation(self):
        """Checks that schema validation catches invalid node type resource
        field.

        Demonstrates a fix in https://github.com/ray-project/ray/pull/16691."""
        path = os.path.join(RAY_PATH, "autoscaler", "aws", "example-full.yaml")
        config = yaml.safe_load(open(path).read())
        node_type = config["available_node_types"]["ray.head.default"]
        # Invalid `resources` field, say user entered `resources: `.
        node_type["resources"] = None
        with pytest.raises(jsonschema.exceptions.ValidationError):
            validate_config(config)
        # Invalid value in resource dict.
        node_type["resources"] = {"CPU": "a string is not valid here"}
        with pytest.raises(jsonschema.exceptions.ValidationError):
            validate_config(config)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
