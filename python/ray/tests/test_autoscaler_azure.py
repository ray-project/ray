"""Tests for Azure autoscaler availability zone functionality."""
import copy
import unittest
from unittest.mock import Mock, patch

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


if __name__ == "__main__":
    unittest.main()
