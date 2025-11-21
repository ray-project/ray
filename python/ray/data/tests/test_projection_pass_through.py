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
    @pytest.mark.parametrize("has_rename", [True, False])
    @pytest.mark.parametrize("has_complex_expr", [True, False])
    def test_operators_without_keys_e2e(
        self,
        ray_start_regular_shared_2_cpus,
        operator_name,
        has_rename,
        has_complex_expr,
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

        # Apply projection based on flags
        if has_complex_expr:
            ds = ds.with_column("sum", col("a") + col("b"))
            ds = ds.select_columns(["sum", "c"])
            expected_keys = {"sum", "c"}
        elif has_rename:
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
    @pytest.mark.parametrize("has_rename", [True, False])
    @pytest.mark.parametrize("has_complex_expr", [True, False])
    def test_operators_with_keys_e2e(
        self,
        ray_start_regular_shared_2_cpus,
        operator_name,
        keys_in_output,
        has_rename,
        has_complex_expr,
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

        # Apply projection based on flags
        if has_complex_expr:
            ds = ds.with_column("computed", col("value") * 2)
            ds = ds.select_columns(["computed", "name"])
            expected_keys = {"computed", "name"}
        elif keys_in_output:
            if has_rename:
                ds = ds.rename_columns({"id": "identifier", "name": "nm"})
                ds = ds.select_columns(["identifier", "nm"])
                expected_keys = {"identifier", "nm"}
            else:
                ds = ds.select_columns(["id", "name"])
                expected_keys = {"id", "name"}
        else:
            if has_rename:
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

        # For sort without complex expressions, verify order is preserved
        if operator_name == "sort" and not has_complex_expr:
            if "name" in result[0]:
                assert result[0]["name"] == "A"
                assert result[1]["name"] == "B"
                assert result[2]["name"] == "C"
            elif "nm" in result[0]:
                assert result[0]["nm"] == "A"
                assert result[1]["nm"] == "B"
                assert result[2]["nm"] == "C"

    @pytest.mark.parametrize(
        "join_type,projection_fn,expected_keys",
        [
            # Inner join - simple selection
            (
                "inner",
                lambda ds: ds.select_columns(["id", "left_value"]),
                {"id", "left_value"},
            ),
            # Inner join - with rename
            (
                "inner",
                lambda ds: ds.rename_columns({"left_value": "lv"}).select_columns(
                    ["id", "lv"]
                ),
                {"id", "lv"},
            ),
            # Inner join - keys not in output
            (
                "inner",
                lambda ds: ds.select_columns(["left_value", "right_value"]),
                {"left_value", "right_value"},
            ),
            # Inner join - complex expression
            (
                "inner",
                lambda ds: ds.with_column("doubled", col("id") * 2).select_columns(
                    ["doubled", "left_value"]
                ),
                {"doubled", "left_value"},
            ),
            # Left semi - simple
            (
                "left_semi",
                lambda ds: ds.select_columns(["id", "left_value"]),
                {"id", "left_value"},
            ),
            # Left semi - with rename
            (
                "left_semi",
                lambda ds: ds.rename_columns({"left_value": "lv"}).select_columns(
                    ["id", "lv"]
                ),
                {"id", "lv"},
            ),
            # Left semi - keys not in output
            ("left_semi", lambda ds: ds.select_columns(["left_value"]), {"left_value"}),
            # Right semi - simple
            (
                "right_semi",
                lambda ds: ds.select_columns(["id", "right_value"]),
                {"id", "right_value"},
            ),
            # Right semi - with rename
            (
                "right_semi",
                lambda ds: ds.rename_columns({"right_value": "rv"}).select_columns(
                    ["id", "rv"]
                ),
                {"id", "rv"},
            ),
            # Right semi - keys not in output
            (
                "right_semi",
                lambda ds: ds.select_columns(["right_value"]),
                {"right_value"},
            ),
        ],
        ids=[
            "inner_simple",
            "inner_rename",
            "inner_keys_out",
            "inner_complex",
            "left_semi_simple",
            "left_semi_rename",
            "left_semi_keys_out",
            "right_semi_simple",
            "right_semi_rename",
            "right_semi_keys_out",
        ],
    )
    def test_join_e2e(
        self,
        ray_start_regular_shared_2_cpus,
        join_type,
        projection_fn,
        expected_keys,
    ):
        """Test Join with various projection configurations and join types."""
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

        ds = left_ds.join(right_ds, join_type=join_type, num_partitions=2, on=("id",))

        # Apply projection using the function
        ds = projection_fn(ds)

        result = ds.take_all()
        assert len(result) == 3

        for item in result:
            assert set(item.keys()) == expected_keys

    # Zip tests

    @pytest.mark.parametrize("has_rename", [True, False])
    @pytest.mark.parametrize("has_complex_expr", [True, False])
    def test_zip_e2e(
        self, ray_start_regular_shared_2_cpus, has_rename, has_complex_expr
    ):
        """End-to-end test for Zip with projection."""
        left_ds = ray.data.from_items([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        right_ds = ray.data.from_items([{"c": 5, "d": 6}, {"c": 7, "d": 8}])

        ds = left_ds.zip(right_ds)

        # Apply projection based on flags
        if has_complex_expr:
            ds = ds.with_column("product", col("a") * col("c"))
            ds = ds.select_columns(["product", "b"])
            expected_first = {"product": 5, "b": 2}
            expected_second = {"product": 21, "b": 4}
        elif has_rename:
            ds = ds.rename_columns({"a": "x", "c": "z"})
            ds = ds.select_columns(["x", "z"])
            expected_first = {"x": 1, "z": 5}
            expected_second = {"x": 3, "z": 7}
        else:
            ds = ds.select_columns(["a", "c"])
            expected_first = {"a": 1, "c": 5}
            expected_second = {"a": 3, "c": 7}

        result = ds.take_all()
        assert len(result) == 2
        assert result[0] == expected_first
        assert result[1] == expected_second

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
