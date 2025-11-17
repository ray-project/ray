"""End-to-end tests for projection pass-through optimization.

Tests actual data execution with operators that support
LogicalOperatorSupportsProjectionPassThrough.
"""

import pytest

import ray
from ray.data.expressions import col
from ray.tests.conftest import *  # noqa


class TestProjectionPassThroughE2E:
    """End-to-end tests for projection pass-through with actual data."""

    # Tests for operators without keys

    @pytest.mark.parametrize(
        "operator_name",
        ["randomize_blocks", "random_shuffle", "repartition", "union"],
    )
    @pytest.mark.parametrize("use_rename", [True, False])
    @pytest.mark.parametrize("use_complex_expr", [True, False])
    def test_operators_without_keys_e2e(
        self,
        ray_start_regular_shared_2_cpus,
        operator_name,
        use_rename,
        use_complex_expr,
    ):
        """Test projection pass-through for operators without keys."""
        # Create base datasets
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])

        # Apply operator
        if operator_name == "randomize_blocks":
            ds = ds.randomize_block_order(seed=42)
        elif operator_name == "random_shuffle":
            ds = ds.random_shuffle(seed=42)
        elif operator_name == "repartition":
            ds = ds.repartition(2)
        elif operator_name == "union":
            ds2 = ray.data.from_items([{"a": 7, "b": 8, "c": 9}])
            ds = ds.union(ds2)

        # Apply projection
        if use_complex_expr:
            # Complex expression (should prevent pass-through)
            ds = ds.with_column("sum", col("a") + col("b"))
            ds = ds.select_columns(["sum", "c"])
            expected_keys = {"sum", "c"}
        elif use_rename:
            ds = ds.rename_columns({"a": "x", "b": "y"})
            ds = ds.select_columns(["x", "y"])
            expected_keys = {"x", "y"}
        else:
            ds = ds.select_columns(["a", "b"])
            expected_keys = {"a", "b"}

        result = ds.take_all()
        assert len(result) >= 2
        for item in result:
            assert set(item.keys()) == expected_keys

    # Tests for operators with keys

    @pytest.mark.parametrize("operator_name", ["repartition", "sort"])
    @pytest.mark.parametrize("keys_in_output", [True, False])
    @pytest.mark.parametrize("use_rename", [True, False])
    @pytest.mark.parametrize("use_complex_expr", [True, False])
    def test_operators_with_keys_e2e(
        self,
        ray_start_regular_shared_2_cpus,
        operator_name,
        keys_in_output,
        use_rename,
        use_complex_expr,
    ):
        """Test projection pass-through for operators with keys (Sort, Repartition)."""
        ds = ray.data.from_items(
            [
                {"id": 3, "name": "C", "value": 300},
                {"id": 1, "name": "A", "value": 100},
                {"id": 2, "name": "B", "value": 200},
            ]
        )

        # Apply operator
        if operator_name == "repartition":
            ds = ds.repartition(2)
        elif operator_name == "sort":
            ds = ds.sort("id")

        # Apply projection
        if use_complex_expr:
            # Complex expression (should prevent pass-through)
            ds = ds.with_column("computed", col("value") * 2)
            ds = ds.select_columns(["computed", "name"])
            expected_keys = {"computed", "name"}
        elif keys_in_output:
            # Include keys in output
            if use_rename:
                ds = ds.rename_columns({"id": "identifier", "name": "nm"})
                ds = ds.select_columns(["identifier", "nm"])
                expected_keys = {"identifier", "nm"}
            else:
                ds = ds.select_columns(["id", "name"])
                expected_keys = {"id", "name"}
        else:
            # Exclude keys from output
            if use_rename:
                ds = ds.rename_columns({"name": "nm", "value": "val"})
                ds = ds.select_columns(["nm", "val"])
                expected_keys = {"nm", "val"}
            else:
                ds = ds.select_columns(["name", "value"])
                expected_keys = {"name", "value"}

        result = ds.take_all()
        assert len(result) == 3

        # Verify columns
        for item in result:
            assert set(item.keys()) == expected_keys

        # For sort, verify order is preserved
        if operator_name == "sort" and not use_complex_expr:
            if "name" in result[0]:
                assert result[0]["name"] == "A"
                assert result[1]["name"] == "B"
                assert result[2]["name"] == "C"
            elif "nm" in result[0]:
                assert result[0]["nm"] == "A"
                assert result[1]["nm"] == "B"
                assert result[2]["nm"] == "C"

    # Join tests

    @pytest.mark.parametrize("keys_in_output", [True, False])
    @pytest.mark.parametrize("use_rename", [True, False])
    @pytest.mark.parametrize("use_complex_expr", [True, False])
    def test_join_e2e(
        self,
        ray_start_regular_shared_2_cpus,
        keys_in_output,
        use_rename,
        use_complex_expr,
    ):
        """Test Join with various projection configurations."""
        left_ds = ray.data.from_items(
            [
                {"id": 1, "left_value": "a"},
                {"id": 2, "left_value": "b"},
                {"id": 3, "left_value": "c"},
            ]
        )
        right_ds = ray.data.from_items(
            [
                {"id": 1, "right_value": "x"},
                {"id": 2, "right_value": "y"},
                {"id": 3, "right_value": "z"},
            ]
        )

        ds = left_ds.join(right_ds, join_type="inner", num_partitions=2, on=("id",))

        if use_complex_expr:
            # Complex expression (should prevent pass-through)
            ds = ds.with_column("combined", col("left_value") + col("right_value"))
            ds = ds.select_columns(["combined", "id"])
            expected_keys = {"combined", "id"}
        elif keys_in_output:
            # Include join keys in output
            if use_rename:
                ds = ds.rename_columns({"id": "identifier", "left_value": "lv"})
                ds = ds.select_columns(["identifier", "lv", "right_value"])
                expected_keys = {"identifier", "lv", "right_value"}
            else:
                ds = ds.select_columns(["id", "left_value", "right_value"])
                expected_keys = {"id", "left_value", "right_value"}
        else:
            # Exclude join keys from output
            if use_rename:
                ds = ds.rename_columns({"left_value": "lv", "right_value": "rv"})
                ds = ds.select_columns(["lv", "rv"])
                expected_keys = {"lv", "rv"}
            else:
                ds = ds.select_columns(["left_value", "right_value"])
                expected_keys = {"left_value", "right_value"}

        result = ds.take_all()
        assert len(result) == 3
        for item in result:
            assert set(item.keys()) == expected_keys

    # Zip tests

    @pytest.mark.parametrize("use_rename", [True, False])
    @pytest.mark.parametrize("use_complex_expr", [True, False])
    def test_zip_e2e(
        self, ray_start_regular_shared_2_cpus, use_rename, use_complex_expr
    ):
        """End-to-end test for Zip with projection."""
        left_ds = ray.data.from_items([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        right_ds = ray.data.from_items([{"c": 5, "d": 6}, {"c": 7, "d": 8}])

        ds = left_ds.zip(right_ds)

        if use_complex_expr:
            # Complex expression
            ds = ds.with_column("product", col("a") * col("c"))
            ds = ds.select_columns(["product", "b"])
            expected_keys = {"product", "b"}
            result = ds.take_all()
            assert len(result) == 2
            for item in result:
                assert set(item.keys()) == expected_keys
        elif use_rename:
            ds = ds.rename_columns({"a": "x", "c": "z"})
            ds = ds.select_columns(["x", "z"])
            result = ds.take_all()
            assert len(result) == 2
            assert result[0] == {"x": 1, "z": 5}
            assert result[1] == {"x": 3, "z": 7}
        else:
            ds = ds.select_columns(["a", "c"])
            result = ds.take_all()
            assert len(result) == 2
            assert result[0] == {"a": 1, "c": 5}
            assert result[1] == {"a": 3, "c": 7}

    # Integration tests combining multiple operators

    def test_projection_fusion_with_repartition(self, ray_start_regular_shared_2_cpus):
        """Test that multiple projections fuse before passing through repartition."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3} for _ in range(10)])

        # Add multiple projections
        ds = ds.with_column("d", col("a") + col("b"))
        ds = ds.with_column("e", col("b") * 2)
        ds = ds.repartition(5)
        ds = ds.select_columns(["a", "d", "e"])

        result = ds.take_all()
        assert len(result) == 10
        for item in result:
            assert set(item.keys()) == {"a", "d", "e"}
            assert item["d"] == item["a"] + 2  # Since b=2
            assert item["e"] == 4  # Since b=2

    def test_complex_chain_with_multiple_operators(
        self, ray_start_regular_shared_2_cpus
    ):
        """Test projection pass-through with a complex chain of operators."""
        ds = ray.data.from_items(
            [{"id": i, "val1": i * 2, "val2": i * 3, "extra": i * 4} for i in range(20)]
        )

        # Chain multiple operations
        ds = ds.random_shuffle(seed=42)
        ds = ds.with_column("computed", col("val1") + col("val2"))
        ds = ds.repartition(4)
        ds = ds.sort("id")
        ds = ds.select_columns(["id", "computed"])

        result = ds.take_all()
        assert len(result) == 20
        for i, item in enumerate(result):
            assert item["id"] == i
            assert item["computed"] == i * 2 + i * 3  # val1 + val2

    def test_rename_through_shuffle_operations(self, ray_start_regular_shared_2_cpus):
        """Test column renaming through shuffle operations."""
        ds = ray.data.from_items([{"x": 1, "y": 2}, {"x": 3, "y": 4}])

        ds = ds.rename_columns({"x": "a"})
        ds = ds.random_shuffle(seed=42)
        ds = ds.rename_columns({"y": "b"})
        ds = ds.repartition(2)

        result = ds.take_all()
        assert len(result) == 2
        for item in result:
            assert set(item.keys()) == {"a", "b"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
