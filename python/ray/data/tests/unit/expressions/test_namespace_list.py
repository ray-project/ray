"""Unit tests for list namespace expressions.

These tests verify expression construction logic without requiring Ray.
"""

import pytest

from ray.data.expressions import col


class TestListNamespaceErrors:
    """Tests for proper error handling in list namespace."""

    def test_list_invalid_index_type(self):
        """Test list bracket notation rejects invalid types."""
        with pytest.raises(TypeError, match="List indices must be integers or slices"):
            col("items").list["invalid"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
