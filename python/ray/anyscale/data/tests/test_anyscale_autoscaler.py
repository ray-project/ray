import math
import unittest
from unittest.mock import MagicMock, patch

import pytest

from ray.anyscale.data.autoscaler.anyscale_autoscaler import (
    AnyscaleAutoscaler,
    _NodeResourceSpec,
    _TimeWindowAverageCalculator,
)


def test_calcuate_time_window_average():
    """Test _TimeWindowAverageCalculator."""
    cur_time = 0

    def patched_time():
        nonlocal cur_time

        return cur_time

    window_s = 10
    values_to_report = [i + 1 for i in range(20)]

    with patch("time.time", patched_time):
        calculator = _TimeWindowAverageCalculator(window_s)
        assert calculator.get_average() is None

        for value in values_to_report:
            # Report values, test `get_average`.
            # and proceed the time by 1 second each time.
            calculator.report(value)
            avg = calculator.get_average()
            values_in_window = values_to_report[max(cur_time - 10, 0) : cur_time + 1]
            expected = sum(values_in_window) / len(values_in_window)
            assert avg == expected, cur_time
            cur_time += 1

        for _ in range(10):
            # Keep proceeding the time, and test `get_average`.
            avg = calculator.get_average()
            values_in_window = values_to_report[max(cur_time - 10, 0) : 20]
            expected = sum(values_in_window) / len(values_in_window)
            assert avg == expected, cur_time
            cur_time += 1

        # Now no values in the time window, `get_average` should return None.
        assert calculator.get_average() is None


class TestAnyscaleAutoscaler(unittest.TestCase):
    """Tests for AnyscaleAutoscaler."""

    def setup_class(self):
        self._node_type1 = {
            "CPU": 4,
            "memory": 1000,
            "object_store_memory": 500,
        }
        self._node_type2 = {
            "CPU": 8,
            "memory": 2000,
            "object_store_memory": 500,
        }
        self._node_type3 = {
            "CPU": 4,
            "GPU": 1,
            "memory": 1000,
            "object_store_memory": 500,
        }
        self._head_node = {
            "CPU": 4,
            "memory": 1000,
            "object_store_memory": 500,
            "node:__internal_head__": 1.0,
        }

    def test_get_node_resource_spec_and_count(self):
        # Test _get_node_resource_spec_and_count
        autoscaler = AnyscaleAutoscaler(
            topology=MagicMock(),
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
        )

        node_table = [
            {
                "Resources": self._head_node,
                "Alive": True,
            },
            {
                "Resources": self._node_type1,
                "Alive": True,
            },
            {
                "Resources": self._node_type2,
                "Alive": True,
            },
            {
                "Resources": self._node_type3,
                "Alive": True,
            },
            {
                "Resources": self._node_type1,
                "Alive": True,
            },
            {
                "Resources": self._node_type2,
                "Alive": False,
            },
        ]

        expected = {
            _NodeResourceSpec.of(
                self._node_type1["CPU"], self._node_type1["memory"]
            ): 2,
            _NodeResourceSpec.of(
                self._node_type2["CPU"], self._node_type2["memory"]
            ): 1,
        }

        with patch("ray.nodes", return_value=node_table):
            assert autoscaler._get_node_resource_spec_and_count() == expected

    def test_try_scale_up_cluster(self):
        # Test _try_scale_up_cluster
        scaling_up_factor = 1.5
        autoscaler = AnyscaleAutoscaler(
            topology=MagicMock(),
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
            cluster_scaling_up_factor=scaling_up_factor,
        )
        autoscaler._get_cluster_cpu_and_mem_util = MagicMock(
            return_value=(1, 1),
        )
        resource_spec1 = _NodeResourceSpec.of(
            self._node_type1["CPU"], self._node_type1["memory"]
        )
        resource_spec2 = _NodeResourceSpec.of(
            self._node_type2["CPU"], self._node_type2["memory"]
        )
        autoscaler._get_node_resource_spec_and_count = MagicMock(
            return_value={
                resource_spec1: 2,
                resource_spec2: 1,
            },
        )
        autoscaler._send_resource_request = MagicMock()
        autoscaler._try_scale_up_cluster()

        expected_resource_request = [
            {"CPU": self._node_type1["CPU"], "memory": self._node_type1["memory"]}
        ] * math.ceil(scaling_up_factor * 2)
        expected_resource_request.extend(
            [{"CPU": self._node_type2["CPU"], "memory": self._node_type2["memory"]}]
            * math.ceil(scaling_up_factor * 1)
        )
        autoscaler._send_resource_request.assert_called_with(expected_resource_request)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
