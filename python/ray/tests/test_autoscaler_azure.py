"""Tests for Azure autoscaler availability zone functionality."""
import copy
import importlib.util
import sys
import types
import unittest
from unittest.mock import Mock, patch

from azure.core.exceptions import ResourceNotFoundError

from ray.autoscaler._private._azure import config as azure_config
from ray.autoscaler._private._azure.node_provider import AzureNodeProvider


class TestAzureAvailabilityZones(unittest.TestCase):
    """Test cases for Azure autoscaler availability zone support."""

    def setUp(self):
        """Set up test fixtures."""
        self.provider_config = {
            "resource_group": "test-rg",
            "location": "westus2",
            "subscription_id": "test-sub-id",
        }
        self.cluster_name = "test-cluster"

        # Create a mock provider that doesn't initialize Azure clients
        with patch.object(
            AzureNodeProvider,
            "__init__",
            lambda self, provider_config, cluster_name: None,
        ):
            self.provider = AzureNodeProvider(self.provider_config, self.cluster_name)
            self.provider.provider_config = self.provider_config
            self.provider.cluster_name = self.cluster_name

    def test_parse_availability_zones_none_input(self):
        """Test _parse_availability_zones with None input returns empty list."""
        result = self.provider._parse_availability_zones(None)
        self.assertEqual(result, [])

    def test_parse_availability_zones_empty_string(self):
        """Test _parse_availability_zones with empty string returns empty list."""
        result = self.provider._parse_availability_zones("")
        self.assertEqual(result, [])

    def test_parse_availability_zones_auto(self):
        """Test _parse_availability_zones with 'auto' returns empty list."""
        result = self.provider._parse_availability_zones("auto")
        self.assertEqual(result, [])

    def test_parse_availability_zones_whitespace_only(self):
        """Test _parse_availability_zones with whitespace-only string returns empty list."""
        result = self.provider._parse_availability_zones("   ")
        self.assertEqual(result, [])

    def test_parse_availability_zones_single_zone(self):
        """Test _parse_availability_zones with single zone string."""
        result = self.provider._parse_availability_zones("1")
        self.assertEqual(result, ["1"])

    def test_parse_availability_zones_multiple_zones(self):
        """Test _parse_availability_zones with comma-separated zones."""
        result = self.provider._parse_availability_zones("1,2,3")
        self.assertEqual(result, ["1", "2", "3"])

    def test_parse_availability_zones_zones_with_spaces(self):
        """Test _parse_availability_zones with spaces around zones."""
        result = self.provider._parse_availability_zones("1, 2, 3")
        self.assertEqual(result, ["1", "2", "3"])

    def test_parse_availability_zones_zones_with_extra_spaces(self):
        """Test _parse_availability_zones with extra spaces and tabs."""
        result = self.provider._parse_availability_zones("  1 ,   2   , 3  ")
        self.assertEqual(result, ["1", "2", "3"])

    def test_parse_availability_zones_none_disable_case_insensitive(self):
        """Test _parse_availability_zones with 'none' variations disables zones."""
        test_cases = ["none", "None", "NONE"]
        for case in test_cases:
            with self.subTest(case=case):
                result = self.provider._parse_availability_zones(case)
                self.assertIsNone(result)

    def test_parse_availability_zones_null_disable_case_insensitive(self):
        """Test _parse_availability_zones with 'null' variations disables zones."""
        test_cases = ["null", "Null", "NULL"]
        for case in test_cases:
            with self.subTest(case=case):
                result = self.provider._parse_availability_zones(case)
                self.assertIsNone(result)

    def test_parse_availability_zones_invalid_type(self):
        """Test _parse_availability_zones with invalid input type raises ValueError."""
        with self.assertRaises(ValueError) as context:
            self.provider._parse_availability_zones(123)

        self.assertIn("availability_zone must be a string", str(context.exception))
        self.assertIn("got int: 123", str(context.exception))

    def test_parse_availability_zones_list_input_invalid(self):
        """Test _parse_availability_zones with list input raises ValueError."""
        with self.assertRaises(ValueError) as context:
            self.provider._parse_availability_zones(["1", "2", "3"])

        self.assertIn("availability_zone must be a string", str(context.exception))

    def test_parse_availability_zones_dict_input_invalid(self):
        """Test _parse_availability_zones with dict input raises ValueError."""
        with self.assertRaises(ValueError) as context:
            self.provider._parse_availability_zones({"zones": ["1", "2"]})

        self.assertIn("availability_zone must be a string", str(context.exception))

    def test_parse_availability_zones_numeric_zones(self):
        """Test _parse_availability_zones with numeric zone strings."""
        result = self.provider._parse_availability_zones("1,2,3")
        self.assertEqual(result, ["1", "2", "3"])

    def test_parse_availability_zones_alpha_zones(self):
        """Test _parse_availability_zones with alphabetic zone strings."""
        result = self.provider._parse_availability_zones("east,west,central")
        self.assertEqual(result, ["east", "west", "central"])

    def test_parse_availability_zones_mixed_zones(self):
        """Test _parse_availability_zones with mixed numeric and alpha zones."""
        result = self.provider._parse_availability_zones("1,zone-b,3")
        self.assertEqual(result, ["1", "zone-b", "3"])


class TestAzureAvailabilityZonePrecedence(unittest.TestCase):
    """Test cases for Azure availability zone precedence logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.base_provider_config = {
            "resource_group": "test-rg",
            "location": "westus2",
            "subscription_id": "test-sub-id",
        }
        self.cluster_name = "test-cluster"

    def _create_mock_provider(self, provider_config=None):
        """Create a mock Azure provider for testing."""
        config = copy.deepcopy(self.base_provider_config)
        if provider_config:
            config.update(provider_config)

        with patch.object(
            AzureNodeProvider,
            "__init__",
            lambda self, provider_config, cluster_name: None,
        ):
            provider = AzureNodeProvider(config, self.cluster_name)
            provider.provider_config = config
            provider.cluster_name = self.cluster_name

            # Mock the validation method to avoid Azure API calls
            provider._validate_zones_for_node_pool = Mock(
                side_effect=lambda zones, location, vm_size: zones
            )

            return provider

    def _extract_zone_logic(self, provider, node_config):
        """Extract zone determination logic similar to _create_node method."""
        node_availability_zone = node_config.get("azure_arm_parameters", {}).get(
            "availability_zone"
        )
        provider_availability_zone = provider.provider_config.get("availability_zone")

        if node_availability_zone is not None:
            return (
                provider._parse_availability_zones(node_availability_zone),
                "node config availability_zone",
            )
        elif provider_availability_zone is not None:
            return (
                provider._parse_availability_zones(provider_availability_zone),
                "provider availability_zone",
            )
        else:
            return ([], "default")

    def test_node_availability_zone_overrides_provider(self):
        """Test that node-level availability_zone overrides provider-level."""
        provider = self._create_mock_provider({"availability_zone": "1,2"})
        node_config = {
            "azure_arm_parameters": {
                "vmSize": "Standard_D2s_v3",
                "availability_zone": "3",
            }
        }

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, ["3"])
        self.assertEqual(source, "node config availability_zone")

    def test_provider_availability_zone_used_when_no_node_override(self):
        """Test that provider-level availability_zone is used when no node override."""
        provider = self._create_mock_provider({"availability_zone": "1,2"})
        node_config = {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}}

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, ["1", "2"])
        self.assertEqual(source, "provider availability_zone")

    def test_none_disables_zones_at_node_level(self):
        """Test that 'none' at node level disables zones even with provider zones."""
        provider = self._create_mock_provider({"availability_zone": "1,2"})
        node_config = {
            "azure_arm_parameters": {
                "vmSize": "Standard_D2s_v3",
                "availability_zone": "none",
            }
        }

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertIsNone(zones)
        self.assertEqual(source, "node config availability_zone")

    def test_no_zones_when_neither_provider_nor_node_specify(self):
        """Test default behavior when neither provider nor node specify zones."""
        provider = self._create_mock_provider()
        node_config = {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}}

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, [])
        self.assertEqual(source, "default")

    def test_node_empty_string_overrides_provider_zones(self):
        """Test that node empty string overrides provider zones (auto-selection)."""
        provider = self._create_mock_provider({"availability_zone": "1,2"})
        node_config = {
            "azure_arm_parameters": {
                "vmSize": "Standard_D2s_v3",
                "availability_zone": "",
            }
        }

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, [])
        self.assertEqual(source, "node config availability_zone")

    def test_node_auto_overrides_provider_zones(self):
        """Test that node 'auto' overrides provider zones (auto-selection)."""
        provider = self._create_mock_provider({"availability_zone": "1,2"})
        node_config = {
            "azure_arm_parameters": {
                "vmSize": "Standard_D2s_v3",
                "availability_zone": "auto",
            }
        }

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, [])
        self.assertEqual(source, "node config availability_zone")

    def test_provider_none_disables_zones(self):
        """Test that provider-level 'none' disables zones."""
        provider = self._create_mock_provider({"availability_zone": "none"})
        node_config = {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}}

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertIsNone(zones)
        self.assertEqual(source, "provider availability_zone")

    def test_provider_empty_string_allows_auto_selection(self):
        """Test that provider-level empty string allows auto-selection."""
        provider = self._create_mock_provider({"availability_zone": ""})
        node_config = {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}}

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, [])
        self.assertEqual(source, "provider availability_zone")

    def test_provider_auto_allows_auto_selection(self):
        """Test that provider-level 'auto' allows auto-selection."""
        provider = self._create_mock_provider({"availability_zone": "auto"})
        node_config = {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}}

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, [])
        self.assertEqual(source, "provider availability_zone")

    def test_node_null_overrides_provider_zones(self):
        """Test that node-level 'null' overrides provider zones."""
        provider = self._create_mock_provider({"availability_zone": "1,2,3"})
        node_config = {
            "azure_arm_parameters": {
                "vmSize": "Standard_D2s_v3",
                "availability_zone": "null",
            }
        }

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertIsNone(zones)
        self.assertEqual(source, "node config availability_zone")

    def test_provider_null_disables_zones(self):
        """Test that provider-level 'null' disables zones."""
        provider = self._create_mock_provider({"availability_zone": "NULL"})
        node_config = {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}}

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertIsNone(zones)
        self.assertEqual(source, "provider availability_zone")

    def test_complex_override_scenario(self):
        """Test complex scenario with multiple node types and different overrides."""
        provider = self._create_mock_provider({"availability_zone": "1,2,3"})

        # Test different node configurations
        test_cases = [
            # Node with specific zone override
            {
                "config": {
                    "azure_arm_parameters": {
                        "vmSize": "Standard_D2s_v3",
                        "availability_zone": "2",
                    }
                },
                "expected_zones": ["2"],
                "expected_source": "node config availability_zone",
            },
            # Node with disabled zones
            {
                "config": {
                    "azure_arm_parameters": {
                        "vmSize": "Standard_D2s_v3",
                        "availability_zone": "none",
                    }
                },
                "expected_zones": None,
                "expected_source": "node config availability_zone",
            },
            # Node with auto-selection
            {
                "config": {
                    "azure_arm_parameters": {
                        "vmSize": "Standard_D2s_v3",
                        "availability_zone": "",
                    }
                },
                "expected_zones": [],
                "expected_source": "node config availability_zone",
            },
            # Node using provider default
            {
                "config": {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}},
                "expected_zones": ["1", "2", "3"],
                "expected_source": "provider availability_zone",
            },
        ]

        for i, test_case in enumerate(test_cases):
            with self.subTest(case=i):
                zones, source = self._extract_zone_logic(provider, test_case["config"])
                self.assertEqual(zones, test_case["expected_zones"])
                self.assertEqual(source, test_case["expected_source"])

    def test_mixed_case_precedence(self):
        """Test precedence with mixed case 'none' values."""
        provider = self._create_mock_provider({"availability_zone": "None"})
        node_config = {
            "azure_arm_parameters": {
                "vmSize": "Standard_D2s_v3",
                "availability_zone": "NONE",
            }
        }

        zones, source = self._extract_zone_logic(provider, node_config)

        # Both should be None (disabled), but node should take precedence
        self.assertIsNone(zones)
        self.assertEqual(source, "node config availability_zone")

    def test_whitespace_handling_in_precedence(self):
        """Test that whitespace is properly handled in precedence logic."""
        provider = self._create_mock_provider({"availability_zone": "  1, 2, 3  "})
        node_config = {
            "azure_arm_parameters": {
                "vmSize": "Standard_D2s_v3",
                "availability_zone": "  2  ",
            }
        }

        zones, source = self._extract_zone_logic(provider, node_config)

        self.assertEqual(zones, ["2"])
        self.assertEqual(source, "node config availability_zone")


class FakeResources:
    """Models the strict keyword-only contract of azure-mgmt-resource>=26.

    Only the operations Ray calls are modelled. Optional and query parameters are
    keyword-only, so a call that passes them positionally raises TypeError from
    the signature itself. That form is still legal on azure-mgmt-resource 24/25,
    which is why a bare Mock or an autospec of the pinned 24.x client cannot
    catch it. Each call is recorded from inside the method body, so it is only
    recorded when the call is valid; callers that swallow exceptions therefore
    cannot hide a bad call from a test assertion.
    """

    def __init__(self, existing=None, vnet_resources=None, role_assignments=None):
        self.list_calls = []
        self.get_by_id_calls = []
        self.delete_by_id_calls = []
        self._existing = existing or {}
        self._vnet_resources = vnet_resources or []
        self._role_assignments = role_assignments or []

    def list_by_resource_group(
        self, resource_group_name, *, filter=None, expand=None, top=None
    ):
        self.list_calls.append(
            {"resource_group_name": resource_group_name, "filter": filter}
        )
        if filter and "virtualNetworks" in filter:
            return iter(self._vnet_resources)
        if filter and "roleAssignments" in filter:
            return iter(self._role_assignments)
        return iter([])

    def get_by_id(self, resource_id, *, api_version):
        self.get_by_id_calls.append(
            {"resource_id": resource_id, "api_version": api_version}
        )
        if resource_id in self._existing:
            return self._existing[resource_id]
        raise ResourceNotFoundError("not found")

    def delete_by_id(self, resource_id, *, api_version):
        self.delete_by_id_calls.append(
            {"resource_id": resource_id, "api_version": api_version}
        )
        return Mock()


class TestAzureDeploymentsClient(unittest.TestCase):
    """ARM deployments moved to azure-mgmt-resource-deployments in
    azure-mgmt-resource 25.0.0. Ray uses the split client when it is installed
    and falls back to the bundled azure-mgmt-resource<25 surface otherwise."""

    OUTPUTS = {
        "msi": {"value": "test-msi"},
        "nsg": {"value": "test-nsg"},
        "subnet": {"value": "test-subnet"},
    }

    def _bootstrap(self, resource_client):
        config = {
            "cluster_name": "test-cluster",
            "provider": {
                "resource_group": "test-rg",
                "location": "westus2",
                "subscription_id": "test-sub-id",
            },
        }
        self.resource_client_cls = Mock(return_value=resource_client)
        with (
            patch.object(azure_config, "AzureCliCredential", side_effect=object),
            patch.object(
                azure_config, "ResourceManagementClient", self.resource_client_cls
            ),
        ):
            return azure_config._configure_resource_group(config)

    def test_create_node_uses_deployments_client(self):
        provider_config = {
            "resource_group": "test-rg",
            "location": "westus2",
            "subscription_id": "test-sub-id",
            "unique_id": "abcd1234",
            "msi": "test-msi",
            "nsg": "test-nsg",
            "subnet": "test-subnet",
        }
        with patch.object(
            AzureNodeProvider,
            "__init__",
            lambda self, provider_config, cluster_name: None,
        ):
            provider = AzureNodeProvider(provider_config, "test-cluster")
        provider.provider_config = provider_config
        provider.cluster_name = "test-cluster"
        provider._validate_zones_for_node_pool = Mock(return_value=[])
        # The resource client has no `.deployments`, as in azure-mgmt-resource>=25.
        provider.resource_client = Mock(spec=["resources", "resource_groups"])
        provider.deployments_client = Mock(spec=["deployments"])
        provider.deployments_client.deployments = Mock(spec=["begin_create_or_update"])

        provider._create_node(
            {"azure_arm_parameters": {"vmSize": "Standard_D2s_v3"}},
            {"ray-node-kind": "worker"},
            1,
        )

        begin_create = provider.deployments_client.deployments.begin_create_or_update
        begin_create.assert_called_once()
        kwargs = begin_create.call_args.kwargs
        self.assertEqual(kwargs["resource_group_name"], "test-rg")
        self.assertEqual(
            kwargs["parameters"]["properties"]["mode"],
            azure_config.DeploymentMode.INCREMENTAL,
        )
        begin_create.return_value.wait.assert_called_once()

    def test_bootstrap_uses_deployments_client(self):
        # The resource client has no `.deployments`, as in azure-mgmt-resource>=25.
        resource_client = Mock(spec=["resources", "resource_groups", "providers"])
        resource_client.resources = FakeResources()
        deployments_client = Mock(spec=["deployments"])
        deployments_client.deployments = Mock(spec=["begin_create_or_update"])
        begin_create = deployments_client.deployments.begin_create_or_update
        begin_create.return_value.result.return_value.properties.outputs = self.OUTPUTS
        client_cls = Mock(return_value=deployments_client)

        with patch.object(azure_config, "DeploymentsMgmtClient", client_cls):
            result = self._bootstrap(resource_client)

        self.assertEqual(client_cls.call_args.args[1], "test-sub-id")
        # The credential is shared between the resource and deployments clients.
        self.assertIs(
            self.resource_client_cls.call_args.args[0], client_cls.call_args.args[0]
        )
        begin_create.assert_called_once()
        kwargs = begin_create.call_args.kwargs
        self.assertEqual(kwargs["resource_group_name"], "test-rg")
        self.assertEqual(kwargs["deployment_name"], "ray-config")
        self.assertEqual(
            kwargs["parameters"]["properties"]["mode"],
            azure_config.DeploymentMode.INCREMENTAL,
        )
        self.assertEqual(result["provider"]["msi"], "test-msi")

    def test_bootstrap_legacy_fallback_without_deployments_package(self):
        # azure-mgmt-resource<25 without azure-mgmt-resource-deployments: the
        # deployment operations live on the resource client itself.
        resource_client = Mock(spec=["resources", "resource_groups", "providers"])
        resource_client.resources = FakeResources()
        resource_client.deployments = Mock(spec=["create_or_update"])
        create = resource_client.deployments.create_or_update
        create.return_value.result.return_value.properties.outputs = self.OUTPUTS

        with patch.object(azure_config, "DeploymentsMgmtClient", None):
            result = self._bootstrap(resource_client)
            self.assertIs(
                azure_config.get_deployments_client(resource_client, None, "sub"),
                resource_client,
            )

        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs["resource_group_name"], "test-rg")
        self.assertEqual(result["provider"]["nsg"], "test-nsg")

    def test_fake_rejects_positional_optional_arguments(self):
        # The strict fake is what makes the Ray-path tests below meaningful: the
        # positional forms below are accepted by azure-mgmt-resource 24/25 but
        # raise TypeError from the signature on 26.
        resources = FakeResources()
        with self.assertRaises(TypeError):
            resources.get_by_id("/subscriptions/s/resourceGroups/rg", "2024-10-01")
        with self.assertRaises(TypeError):
            resources.list_by_resource_group("rg", "resourceType eq 'x'")
        with self.assertRaises(TypeError):
            resources.delete_by_id("/subscriptions/s/resourceGroups/rg", "2022-04-01")

    def test_bootstrap_resource_calls_use_keyword_arguments(self):
        # Exercises list-by-resource-group, get-by-id (vnet, MSI, role assignment)
        # and the vnet API-version lookup against the strict-signature fake.
        resources = FakeResources(
            vnet_resources=[Mock(id="/subscriptions/test-sub-id/vnet-id")],
            existing={
                "/subscriptions/test-sub-id/vnet-id": Mock(
                    properties={"subnets": [{"properties": {"addressPrefix": "x"}}]}
                )
            },
        )
        resource_client = Mock(spec=["resources", "resource_groups", "providers"])
        resource_client.resources = resources
        vnet_provider = Mock(
            resource_type="virtualNetworks", api_versions=["2024-05-01"]
        )
        resource_client.providers.get.return_value = Mock(
            resource_types=[vnet_provider]
        )
        deployments_client = Mock(spec=["deployments"])
        deployments_client.deployments = Mock(spec=["begin_create_or_update"])
        deployments_client.deployments.begin_create_or_update.return_value.result.return_value.properties.outputs = (
            self.OUTPUTS
        )

        with patch.object(
            azure_config, "DeploymentsMgmtClient", Mock(return_value=deployments_client)
        ):
            result = self._bootstrap(resource_client)

        self.assertEqual(result["provider"]["subnet"], "test-subnet")
        self.assertIn("virtualNetworks", resources.list_calls[0]["filter"])
        looked_up = {
            c["resource_id"]: c["api_version"] for c in resources.get_by_id_calls
        }
        # vnet lookup uses the API version reported by the provider.
        self.assertEqual(looked_up["/subscriptions/test-sub-id/vnet-id"], "2024-05-01")
        # MSI lookup and role-assignment verification both reached the SDK, i.e.
        # neither was swallowed by a surrounding `except Exception`.
        self.assertEqual(
            [v for k, v in looked_up.items() if "userAssignedIdentities" in k],
            ["2023-01-31"],
        )
        self.assertEqual(
            [v for k, v in looked_up.items() if "roleAssignments" in k], ["2022-04-01"]
        )
        # The lookup found no role assignment, so none was deleted. A lookup that
        # raised TypeError would be treated as a failed query and fall through to
        # a deletion attempt.
        self.assertEqual(resources.delete_by_id_calls, [])

    def test_role_assignment_cleanup_uses_keyword_arguments(self):
        assignment = Mock(
            id="/subscriptions/s/roleAssignments/a", properties={"principalId": "p"}
        )
        resources = FakeResources(role_assignments=[assignment])
        resource_client = Mock(spec=["resources"])
        resource_client.resources = resources

        azure_config._delete_role_assignments_for_principal(
            resource_client, "test-rg", "p"
        )

        self.assertEqual(
            resources.list_calls[0]["filter"],
            "resourceType eq 'Microsoft.Authorization/roleAssignments'",
        )
        self.assertEqual(
            resources.delete_by_id_calls,
            [{"resource_id": assignment.id, "api_version": "2022-04-01"}],
        )

    def test_wait_for_role_assignment_deletion_uses_keyword_arguments(self):
        resources = FakeResources()
        self.assertTrue(
            azure_config._wait_for_role_assignment_deletion(
                resources.get_by_id, "/subscriptions/s/roleAssignments/a", "a"
            )
        )
        self.assertEqual(
            resources.get_by_id_calls,
            [
                {
                    "resource_id": "/subscriptions/s/roleAssignments/a",
                    "api_version": "2022-04-01",
                }
            ],
        )

    def test_cleanup_managed_identity_uses_keyword_arguments(self):
        msi_id = "/subscriptions/s/resourceGroups/rg/providers/x/identities/msi"
        resources = FakeResources(
            existing={msi_id: Mock(properties={"principalId": "p"})}
        )
        with patch.object(
            AzureNodeProvider,
            "__init__",
            lambda self, provider_config, cluster_name: None,
        ):
            provider = AzureNodeProvider({}, "test-cluster")
        provider.provider_config = {}
        provider.resource_client = Mock(spec=["resources"])
        provider.resource_client.resources = resources

        principal_id = provider._cleanup_managed_identity("rg", msi_id)

        self.assertEqual(principal_id, "p")
        self.assertEqual(
            resources.get_by_id_calls,
            [{"resource_id": msi_id, "api_version": "2023-01-31"}],
        )
        self.assertEqual(
            resources.delete_by_id_calls,
            [{"resource_id": msi_id, "api_version": "2023-01-31"}],
        )

    def test_incompatible_sdk_raises_actionable_error(self):
        # Neither the split package nor the legacy deployments models exist.
        blocked = {
            "azure.mgmt.resource.deployments": None,
            "azure.mgmt.resource.deployments.models": None,
            "azure.mgmt.resource.resources.models": types.ModuleType("models"),
        }
        spec = importlib.util.spec_from_file_location(
            "_azure_config_probe", azure_config.__file__
        )
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, blocked):
            with self.assertRaisesRegex(ImportError, "azure-mgmt-resource-deployments"):
                spec.loader.exec_module(module)


if __name__ == "__main__":
    unittest.main()
