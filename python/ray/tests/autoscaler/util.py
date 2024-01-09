import unittest
from unittest.mock import Mock
from ray.autoscaler._private.util import get_per_node_breakdown_as_dict


class TestGetPerNodeBreakdown(unittest.TestCase):
    def setUp(self):
        # Create a mock LoadMetricsSummary object with the required attributes
        lm_summary_mock_data = {
            "e9919752e5e8d757765d97d8bec910a2e78e8826f20bce46fd58f92e": {
                "node:172.31.6.57": [0.0, 1.0],
                "object_store_memory": [0.0, 13984228147.0],
                "memory": [0.0, 27968456295.0],
                "node:__internal_head__": [0.0, 1.0],
                "CPU": [0.0, 8.0],
            }
        }
        self.lm_summary_mock = Mock()
        self.lm_summary_mock.usage_by_node = lm_summary_mock_data

    def test_get_per_node_breakdown_as_dict(self):
        result = get_per_node_breakdown_as_dict(self.lm_summary_mock)

        expected_output = {
            "e9919752e5e8d757765d97d8bec910a2e78e8826f20bce46fd58f92e": (
                "0.0/8.0 CPU\n0B/26.05GiB memory\n0B/13.02GiB object_store_memory"
            )
        }

        self.assertEqual(result, expected_output)

    def test_get_per_node_breakdown_as_dict_empty_summary(self):
        # Test with an empty lm_summary
        lm_summary_mock_data = {}
        self.lm_summary_mock.usage_by_node = lm_summary_mock_data

        result = get_per_node_breakdown_as_dict(self.lm_summary_mock)

        expected_output = {}

        self.assertEqual(result, expected_output)

    def test_get_per_node_breakdown_as_dict_missing_usage(self):
        # Test with missing usage data for a node
        lm_summary_mock_data = {
            "e9919752e5e8d757765d97d8bec910a2e78e8826f20bce46fd58f92e": {
                "node:172.31.6.57": [0.0, 1.0],
                "object_store_memory": [0.0, 13984228147.0],
                # 'memory': [0.0, 27968456295.0],  # Missing memory data
                "node:__internal_head__": [0.0, 1.0],
                "CPU": [0.0, 8.0],
            }
        }
        self.lm_summary_mock.usage_by_node = lm_summary_mock_data

        result = get_per_node_breakdown_as_dict(self.lm_summary_mock)

        expected_output = {
            "e9919752e5e8d757765d97d8bec910a2e78e8826f20bce46fd58f92e": "0.0/8.0 CPU\n"
            "0B/13.02GiB object_store_memory"
        }

        self.assertEqual(result, expected_output)


if __name__ == "__main__":
    unittest.main()
