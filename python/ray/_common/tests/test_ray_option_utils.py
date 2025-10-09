import re
import sys
from unittest.mock import patch

import pytest

from ray._common.ray_option_utils import (
    Option,
    _check_deprecate_placement_group,
    _counting_option,
    _resource_option,
    _validate_resource_quantity,
    _validate_resources,
    update_options,
    validate_actor_options,
    validate_task_options,
)
from ray.util.placement_group import PlacementGroup


class TestOptionValidation:
    def test_option_validate(self):
        opt = Option(
            type_constraint=int, value_constraint=lambda v: "error" if v < 0 else None
        )
        opt.validate("test", 1)
        with pytest.raises(TypeError):
            opt.validate("test", "a")
        with pytest.raises(ValueError, match="error"):
            opt.validate("test", -1)

    def test_counting_option(self):
        # Test infinite counting option
        opt_inf = _counting_option("test_inf", infinite=True)
        opt_inf.validate("test_inf", 5)
        opt_inf.validate("test_inf", 0)
        opt_inf.validate("test_inf", -1)  # Represents infinity
        opt_inf.validate("test_inf", None)
        with pytest.raises(ValueError):
            opt_inf.validate("test_inf", -2)
        with pytest.raises(TypeError):
            opt_inf.validate("test_inf", 1.5)

        # Test non-infinite counting option
        opt_non_inf = _counting_option("test_non_inf", infinite=False)
        opt_non_inf.validate("test_non_inf", 5)
        opt_non_inf.validate("test_non_inf", 0)
        opt_non_inf.validate("test_non_inf", None)
        with pytest.raises(ValueError):
            opt_non_inf.validate("test_non_inf", -1)

    @patch("ray._raylet.RESOURCE_UNIT_SCALING", 10000)
    @patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value={"GPU", "TPU"},
    )
    @patch("ray._private.accelerators.get_accelerator_manager_for_resource")
    def test_validate_resource_quantity(self, mock_get_manager, mock_get_all_names):
        # Valid cases
        assert _validate_resource_quantity("CPU", 1) is None
        assert _validate_resource_quantity("memory", 0) is None
        assert _validate_resource_quantity("custom", 0.5) is None

        # Invalid cases
        err = _validate_resource_quantity("CPU", -1)
        assert isinstance(err, str)
        assert "cannot be negative" in err
        err = _validate_resource_quantity("CPU", 0.00001)
        assert isinstance(err, str)
        assert "cannot go beyond 0.0001" in err

        # Accelerator validation
        mock_manager_instance = mock_get_manager.return_value
        mock_manager_instance.validate_resource_request_quantity.return_value = (
            False,
            "mock error",
        )
        err = _validate_resource_quantity("GPU", 1.5)
        assert isinstance(err, str)
        assert "mock error" in err
        mock_get_manager.assert_called_with("GPU")
        mock_manager_instance.validate_resource_request_quantity.assert_called_with(1.5)

        mock_manager_instance.validate_resource_request_quantity.return_value = (
            True,
            "",
        )
        assert _validate_resource_quantity("TPU", 1) is None

    def test_resource_option(self):
        opt = _resource_option("CPU")
        opt.validate("CPU", 1)
        opt.validate("CPU", 0.5)
        opt.validate("CPU", None)
        with pytest.raises(TypeError):
            opt.validate("CPU", "1")
        with pytest.raises(ValueError):
            opt.validate("CPU", -1.0)

    def test_validate_resources(self):
        assert _validate_resources(None) is None
        assert _validate_resources({"custom": 1}) is None
        err = _validate_resources({"CPU": 1, "GPU": 1})
        assert isinstance(err, str)
        assert "Use the 'num_cpus' and 'num_gpus' keyword" in err
        err = _validate_resources({"custom": -1})
        assert isinstance(err, str)
        assert "cannot be negative" in err


class TestTaskActorOptionValidation:
    def test_validate_task_options_valid(self):
        validate_task_options({"num_cpus": 2, "max_retries": 3}, in_options=False)

    def test_validate_task_options_invalid_keyword(self):
        with pytest.raises(ValueError, match="Invalid option keyword"):
            validate_task_options({"invalid_option": 1}, in_options=False)

    def test_validate_task_options_in_options_invalid(self):
        with pytest.raises(
            ValueError,
            match=re.escape("Setting 'max_calls' is not supported in '.options()'."),
        ):
            validate_task_options({"max_calls": 5}, in_options=True)

    def test_validate_actor_options_valid(self):
        validate_actor_options({"max_concurrency": 2, "name": "abc"}, in_options=False)

    def test_validate_actor_options_invalid_keyword(self):
        with pytest.raises(ValueError, match="Invalid option keyword"):
            validate_actor_options({"invalid_option": 1}, in_options=False)

    def test_validate_actor_options_in_options_invalid(self):
        with pytest.raises(
            ValueError,
            match=re.escape(
                "Setting 'concurrency_groups' is not supported in '.options()'."
            ),
        ):
            validate_actor_options({"concurrency_groups": {}}, in_options=True)

    def test_validate_actor_get_if_exists_no_name(self):
        with pytest.raises(
            ValueError, match="must be specified to use `get_if_exists`"
        ):
            validate_actor_options({"get_if_exists": True}, in_options=False)

    def test_validate_actor_object_store_memory_warning(self):
        with pytest.warns(
            DeprecationWarning,
            match="Setting 'object_store_memory' for actors is deprecated",
        ):
            validate_actor_options({"object_store_memory": 100}, in_options=False)

    def test_check_deprecate_placement_group(self):
        pg = PlacementGroup.empty()
        # No error if only one is specified
        _check_deprecate_placement_group({"placement_group": pg})
        _check_deprecate_placement_group({"scheduling_strategy": "SPREAD"})

        # Error if both are specified
        with pytest.raises(
            ValueError, match="Placement groups should be specified via"
        ):
            _check_deprecate_placement_group(
                {"placement_group": pg, "scheduling_strategy": "SPREAD"}
            )

        # Check no error with default or None placement_group
        _check_deprecate_placement_group(
            {"placement_group": "default", "scheduling_strategy": "SPREAD"}
        )
        _check_deprecate_placement_group(
            {"placement_group": None, "scheduling_strategy": "SPREAD"}
        )


class TestUpdateOptions:
    def test_simple_update(self):
        original = {"num_cpus": 1, "name": "a"}
        new = {"num_cpus": 2, "num_gpus": 1}
        updated = update_options(original, new)
        assert updated == {"num_cpus": 2, "name": "a", "num_gpus": 1}

    def test_metadata_update(self):
        original = {"_metadata": {"ns1": {"config1": "val1"}}}
        new = {"_metadata": {"ns1": {"config2": "val2"}, "ns2": {"config3": "val3"}}}
        updated = update_options(original, new)
        expected_metadata = {
            "ns1": {"config1": "val1", "config2": "val2"},
            "ns2": {"config3": "val3"},
        }
        assert updated["_metadata"] == expected_metadata

    def test_metadata_update_no_original_metadata(self):
        original = {"num_cpus": 1}
        new = {"_metadata": {"ns1": {"config1": "val1"}}}
        updated = update_options(original, new)
        assert updated["num_cpus"] == 1
        assert updated["_metadata"] == new["_metadata"]

    def test_metadata_update_no_new_metadata(self):
        original = {"_metadata": {"ns1": {"config1": "val1"}}}
        new = {"num_cpus": 1}
        updated = update_options(original, new)
        assert updated["num_cpus"] == 1
        assert updated["_metadata"] == original["_metadata"]

    def test_update_with_empty_new(self):
        original = {"num_cpus": 1}
        updated = update_options(original, {})
        assert updated == original

    def test_update_empty_original(self):
        new = {"num_cpus": 1}
        updated = update_options({}, new)
        assert updated == new


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
