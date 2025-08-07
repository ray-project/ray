import json
import os
from dataclasses import asdict, dataclass

import pytest

import ray
from ray.data import DataContext
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.metadata_exporter import (
    UNKNOWN,
    Operator,
    Topology,
    sanitize_for_struct,
)
from ray.data._internal.stats import _get_or_create_stats_actor
from ray.tests.conftest import _ray_start

STUB_JOB_ID = "stub_job_id"
STUB_DATASET_ID = "stub_dataset_id"


def _get_export_file_path() -> str:
    return os.path.join(
        ray._private.worker._global_node.get_session_dir_path(),
        "logs",
        "export_events",
        "event_EXPORT_DATASET_METADATA.log",
    )


def _get_exported_data():
    exported_file = _get_export_file_path()
    assert os.path.isfile(exported_file)

    with open(exported_file, "r") as f:
        data = f.readlines()

    return [json.loads(line) for line in data]


@pytest.fixture
def ray_start_cluster_with_export_api_config(shutdown_only):
    """Enable export API for the EXPORT_TRAIN_RUN resource type."""
    with _ray_start(
        num_cpus=4,
        runtime_env={
            "env_vars": {
                "RAY_enable_export_api_write_config": "EXPORT_DATASET_METADATA"
            }
        },
    ) as res:
        yield res


@pytest.fixture
def ray_start_cluster_with_export_api_write(shutdown_only):
    """Enable export API for all resource types."""
    with _ray_start(
        num_cpus=4,
        runtime_env={"env_vars": {"RAY_enable_export_api_write": "1"}},
    ) as res:
        yield res


@pytest.fixture
def dummy_dataset_topology():
    """Create a dummy Topology."""
    dummy_topology = Topology(
        operators=[
            Operator(
                name="Input",
                id="Input_0",
                uuid="uuid_0",
                input_dependencies=[],
                sub_stages=[],
            ),
            Operator(
                name="ReadRange->Map(<lambda>)->Filter(<lambda>)",
                id="ReadRange->Map(<lambda>)->Filter(<lambda>)_1",
                uuid="uuid_1",
                input_dependencies=["Input_0"],
                sub_stages=[],
            ),
        ],
    )
    return dummy_topology


def test_export_disabled(ray_start_regular, dummy_dataset_topology):
    """Test that no export files are created when export API is disabled."""
    stats_actor = _get_or_create_stats_actor()

    # Create or update train run
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag="test_dataset",
            operator_tags=["ReadRange->Map(<lambda>)->Filter(<lambda>)"],
            topology=dummy_dataset_topology,
            job_id=STUB_JOB_ID,
            data_context=DataContext.get_current(),
        )
    )

    # Check that no export files were created
    assert not os.path.exists(_get_export_file_path())


def _test_dataset_metadata_export(topology):
    """Test that dataset metadata export events are written when export API is enabled."""
    stats_actor = _get_or_create_stats_actor()

    # Simulate a dataset registration
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag=STUB_DATASET_ID,
            operator_tags=["ReadRange->Map(<lambda>)->Filter(<lambda>)"],
            topology=topology,
            job_id=STUB_JOB_ID,
            data_context=DataContext.get_current(),
        )
    )

    # Check that export files were created
    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["source_type"] == "EXPORT_DATASET_METADATA"
    assert data[0]["event_data"]["topology"] == sanitize_for_struct(asdict(topology))
    assert data[0]["event_data"]["dataset_id"] == STUB_DATASET_ID
    assert data[0]["event_data"]["job_id"] == STUB_JOB_ID
    assert data[0]["event_data"]["start_time"] is not None


def test_export_dataset_metadata_enabled_by_config(
    ray_start_cluster_with_export_api_config, dummy_dataset_topology
):
    _test_dataset_metadata_export(dummy_dataset_topology)


def test_export_dataset_metadata(
    ray_start_cluster_with_export_api_write, dummy_dataset_topology
):
    _test_dataset_metadata_export(dummy_dataset_topology)


@pytest.mark.parametrize(
    "expected_logical_op_args",
    [
        {
            "fn_args": [1],
            "fn_constructor_kwargs": [2],
            "fn_kwargs": {"a": 3},
            "fn_constructor_args": {"b": 4},
            "compute": ray.data.ActorPoolStrategy(max_tasks_in_flight_per_actor=2),
        },
    ],
)
def test_logical_op_args(
    ray_start_cluster_with_export_api_write, expected_logical_op_args
):
    class Udf:
        def __init__(self, a, b):
            self.a = a
            self.b = b

        def __call__(self, x):
            return x

    ds = ray.data.range(1).map_batches(
        Udf,
        **expected_logical_op_args,
    )
    dag = ds._plan._logical_plan.dag
    args = dag._get_args()
    assert len(args) > 0, "Export args should not be empty"
    for k, v in expected_logical_op_args.items():
        k = f"_{k}"
        assert k in args, f"Export args should contain key '{k}'"
        assert (
            args[k] == v
        ), f"Export args for key '{k}' should match expected value {v}, found {args[k]}"


def test_export_multiple_datasets(
    ray_start_cluster_with_export_api_write, dummy_dataset_topology
):
    """Test that multiple datasets can be exported when export API is enabled."""
    stats_actor = _get_or_create_stats_actor()

    # Create a second dataset structure that's different from the dummy one
    second_topology = Topology(
        operators=[
            Operator(
                name="Input",
                id="Input_0",
                uuid="second_uuid_0",
                input_dependencies=[],
                sub_stages=[],
            ),
            Operator(
                name="ReadRange->Map(<lambda>)",
                id="ReadRange->Map(<lambda>)_1",
                uuid="second_uuid_1",
                input_dependencies=["Input_0"],
                sub_stages=[],
            ),
        ],
    )

    # Dataset IDs for each dataset
    first_dataset_id = "first_dataset"
    second_dataset_id = "second_dataset"

    # Register the first dataset
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag=first_dataset_id,
            operator_tags=["ReadRange->Map(<lambda>)->Filter(<lambda>)"],
            topology=dummy_dataset_topology,
            job_id=STUB_JOB_ID,
            data_context=DataContext.get_current(),
        )
    )

    # Register the second dataset
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag=second_dataset_id,
            operator_tags=["ReadRange->Map(<lambda>)"],
            topology=second_topology,
            job_id=STUB_JOB_ID,
            data_context=DataContext.get_current(),
        )
    )

    # Check that export files were created with both datasets
    data = _get_exported_data()
    assert len(data) == 2, f"Expected 2 exported datasets, got {len(data)}"

    # Create a map of dataset IDs to their exported data for easier verification
    datasets_by_id = {entry["event_data"]["dataset_id"]: entry for entry in data}

    # Verify first dataset
    assert (
        first_dataset_id in datasets_by_id
    ), f"First dataset {first_dataset_id} not found in exported data"
    first_entry = datasets_by_id[first_dataset_id]
    assert first_entry["source_type"] == "EXPORT_DATASET_METADATA"
    assert first_entry["event_data"]["topology"] == sanitize_for_struct(
        asdict(dummy_dataset_topology)
    )
    assert first_entry["event_data"]["job_id"] == STUB_JOB_ID
    assert first_entry["event_data"]["start_time"] is not None

    # Verify second dataset
    assert (
        second_dataset_id in datasets_by_id
    ), f"Second dataset {second_dataset_id} not found in exported data"
    second_entry = datasets_by_id[second_dataset_id]
    assert second_entry["source_type"] == "EXPORT_DATASET_METADATA"
    assert second_entry["event_data"]["topology"] == sanitize_for_struct(
        asdict(second_topology)
    )
    assert second_entry["event_data"]["job_id"] == STUB_JOB_ID
    assert second_entry["event_data"]["start_time"] is not None


class UnserializableObject:
    """A test class that can't be JSON serialized or converted to string easily."""

    def __str__(self):
        raise ValueError("Cannot convert to string")

    def __repr__(self):
        raise ValueError("Cannot convert to repr")


class BasicObject:
    """A test class that can be converted to string."""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return f"BasicObject({self.value})"


@pytest.mark.parametrize(
    "input_obj,expected_output,truncate_length",
    [
        # Basic types - should return as-is
        (42, 42, 100),
        (3.14, 3.14, 100),
        (True, True, 100),
        (False, False, 100),
        (None, None, 100),
        # Strings - short strings return as-is
        ("hello", "hello", 100),
        # Strings - long strings get truncated
        ("a" * 150, "a" * 100 + "...", 100),
        ("hello world", "hello...", 5),
        # Mappings - should recursively sanitize values
        ({"key": "value"}, {"key": "value"}, 100),
        ({"long_key": "a" * 150}, {"long_key": "a" * 100 + "..."}, 100),
        ({"nested": {"inner": "value"}}, {"nested": {"inner": "value"}}, 100),
        # Sequences - should recursively sanitize elements
        ([1, 2, 3], [1, 2, 3], 100),
        (["short", "a" * 150], ["short", "a" * 100 + "..."], 100),
        # Complex nested structures
        (
            {"list": [1, "a" * 150], "dict": {"key": "a" * 150}},
            {"list": [1, "a" * 100 + "..."], "dict": {"key": "a" * 100 + "..."}},
            100,
        ),
        # Objects that can be converted to string
        (BasicObject("test"), "BasicObject(test)", 100),  # Falls back to str()
        # Objects that can't be JSON serialized but can be stringified
        ({1, 2, 3}, "{1, 2, 3}", 100),  # Falls back to str()
        # Objects that can't be serialized or stringified
        (UnserializableObject(), UNKNOWN, 100),
        # Empty containers
        ({}, {}, 100),
        ([], [], 100),
        # Mixed type sequences
        (
            [1, "hello", {"key": "value"}, None],
            [1, "hello", {"key": "value"}, None],
            100,
        ),
    ],
)
def test_sanitize_for_struct(input_obj, expected_output, truncate_length):
    """Test sanitize_for_struct with various input types and truncation lengths."""
    result = sanitize_for_struct(input_obj, truncate_length)
    assert result == expected_output


class DummyLogicalOperator(LogicalOperator):
    """A dummy logical operator for testing _get_logical_args with various data types."""

    def __init__(self, input_op=None):
        super().__init__(input_op)

        # Test various data types that might be returned by _get_logical_args
        self._string_value = "test_string"
        self._int_value = 42
        self._float_value = 3.14
        self._bool_value = True
        self._none_value = None
        self._list_value = [1, 2, 3, "string", None]
        self._dict_value = {"key1": "value1", "key2": 123, "key3": None}
        self._nested_dict = {
            "level1": {
                "level2": {
                    "level3": "deep_value",
                    "numbers": [1, 2, 3],
                    "mixed": {"a": 1, "b": "string", "c": None},
                }
            }
        }
        self._tuple_value = (1, "string", None, 3.14)
        self._set_value = {1, 2, 3, "string"}  # Sets should be converted to lists
        self._bytes_value = b"binary_data"
        self._complex_dict = {
            "string_keys": {"a": 1, "b": 2},
            "int_keys": {1: "one", 2: "two"},  # This should cause issues if not handled
            "mixed_keys": {"str": "value", 1: "int_key", None: "none_key"},
        }
        self._empty_containers = {
            "empty_list": [],
            "empty_dict": {},
            "empty_tuple": (),
            "empty_set": set(),
        }
        self._special_values = {
            "zero": 0,
            "negative": -1,
            "large_int": 999999999999999999,
            "small_float": 0.0000001,
            "inf": float("inf"),
            "neg_inf": float("-inf"),
            "nan": float("nan"),
        }


def test_dummy_operator_get_logical_args():
    """Test that _get_logical_args properly handles all data types."""

    # Create dummy operator
    dummy_op = DummyLogicalOperator()

    # Get the args
    args = dummy_op._get_args()

    # Verify it's a dictionary
    assert isinstance(args, dict), f"Expected dict, got {type(args)}"

    # Verify all keys are strings
    for key in args.keys():
        assert isinstance(key, str), f"Expected string key, got {type(key)}: {key}"

    # Test specific values
    assert args["_string_value"] == "test_string"
    assert args["_int_value"] == 42
    assert args["_float_value"] == 3.14
    assert args["_bool_value"] is True
    assert args["_none_value"] is None
    assert args["_list_value"] == [1, 2, 3, "string", None]
    assert args["_dict_value"] == {"key1": "value1", "key2": 123, "key3": None}

    # Test nested structures
    nested = args["_nested_dict"]
    assert isinstance(nested, dict)
    assert nested["level1"]["level2"]["level3"] == "deep_value"
    assert nested["level1"]["level2"]["numbers"] == [1, 2, 3]

    # Test tuple (should be converted to list)
    assert args["_tuple_value"] == [1, "string", None, 3.14]

    # Test set (should be converted to list)
    set_value = args["_set_value"]
    assert isinstance(set_value, list)
    assert len(set_value) == 4
    assert 1 in set_value
    assert "string" in set_value

    # Test bytes (should be converted to string)
    assert args["_bytes_value"] == "binary_data"

    # Test complex dict with int keys (should be converted to string keys)
    complex_dict = args["_complex_dict"]
    assert isinstance(complex_dict, dict)
    assert "string_keys" in complex_dict
    assert "int_keys" in complex_dict
    assert "mixed_keys" in complex_dict

    # Verify int keys were converted to strings
    int_keys = complex_dict["int_keys"]
    assert isinstance(int_keys, dict)
    assert "1" in int_keys  # Should be string key
    assert "2" in int_keys  # Should be string key
    assert int_keys["1"] == "one"
    assert int_keys["2"] == "two"

    # Test empty containers
    empty = args["_empty_containers"]
    assert empty["empty_list"] == []
    assert empty["empty_dict"] == {}
    assert empty["empty_tuple"] == []
    assert empty["empty_set"] == []

    # Test special values
    special = args["_special_values"]
    assert special["zero"] == 0
    assert special["negative"] == -1
    assert special["large_int"] == 999999999999999999
    assert special["small_float"] == 0.0000001
    # Note: inf, neg_inf, nan might be converted to strings or handled specially


def test_dummy_operator_serialization():
    """Test that the dummy operator can be properly serialized for metadata export."""

    from ray.data._internal.metadata_exporter import sanitize_for_struct

    # Create dummy operator
    dummy_op = DummyLogicalOperator()

    # Get the args
    args = dummy_op._get_args()

    # Test that sanitize_for_struct can handle all the data
    try:
        sanitized = sanitize_for_struct(args)
        # If we get here, serialization succeeded
        assert isinstance(sanitized, dict)

        # Test that all keys are strings
        for key in sanitized.keys():
            assert isinstance(key, str), f"Expected string key, got {type(key)}: {key}"

    except Exception as e:
        pytest.fail(f"Serialization failed: {e}")


def test_dummy_operator_with_metadata_export():
    """Test that the dummy operator works with the full metadata export pipeline."""

    stats_actor = _get_or_create_stats_actor()

    # Create a topology with our dummy operator
    topology = Topology(
        operators=[
            Operator(
                name="DummyOperator",
                id="DummyOperator_0",
                uuid="dummy_uuid_0",
                input_dependencies=[],
                sub_stages=[],
            ),
        ],
    )

    # Register the dataset
    try:
        ray.get(
            stats_actor.register_dataset.remote(
                dataset_tag="dummy_test_dataset",
                operator_tags=["DummyOperator"],
                topology=topology,
                job_id=STUB_JOB_ID,
                data_context=DataContext.get_current(),
            )
        )

        # If we get here, the registration succeeded
        assert True

    except Exception as e:
        pytest.fail(f"Dataset registration failed: {e}")


@dataclass
class TestDataclass:
    """A test dataclass for testing dataclass serialization."""

    string_field: str
    int_field: int
    float_field: float
    bool_field: bool
    list_field: list
    dict_field: dict
    none_field: None = None


class DummyLogicalOperatorWithDataclass(LogicalOperator):
    """A dummy logical operator that includes dataclass instances."""

    def __init__(self, input_op=None):
        super().__init__(input_op)

        # Include a dataclass instance
        self._dataclass_instance = TestDataclass(
            string_field="test_string",
            int_field=42,
            float_field=3.14,
            bool_field=True,
            list_field=[1, 2, 3],
            dict_field={"key": "value"},
            none_field=None,
        )

        # Include nested dataclass
        self._nested_dataclass = {
            "level1": {
                "dataclass": TestDataclass(
                    string_field="nested",
                    int_field=100,
                    float_field=2.718,
                    bool_field=False,
                    list_field=["a", "b", "c"],
                    dict_field={"nested": "data"},
                )
            }
        }

        # Include dataclass in list
        self._dataclass_list = [
            TestDataclass(
                string_field="list_item_1",
                int_field=1,
                float_field=1.0,
                bool_field=True,
                list_field=[],
                dict_field={},
            ),
            TestDataclass(
                string_field="list_item_2",
                int_field=2,
                float_field=2.0,
                bool_field=False,
                list_field=[1, 2],
                dict_field={"list": "item"},
            ),
        ]


def test_dataclass_serialization():
    """Test that dataclass instances are properly serialized using asdict()."""

    # Create dummy operator with dataclass
    dummy_op = DummyLogicalOperatorWithDataclass()

    # Get the args
    args = dummy_op._get_args()

    # Verify it's a dictionary
    assert isinstance(args, dict)

    # Verify all keys are strings
    for key in args.keys():
        assert isinstance(key, str), f"Expected string key, got {type(key)}: {key}"

    # Test dataclass instance
    dataclass_instance = args["_dataclass_instance"]
    assert isinstance(dataclass_instance, dict)
    assert dataclass_instance["string_field"] == "test_string"
    assert dataclass_instance["int_field"] == 42
    assert dataclass_instance["float_field"] == 3.14
    assert dataclass_instance["bool_field"] is True
    assert dataclass_instance["list_field"] == [1, 2, 3]
    assert dataclass_instance["dict_field"] == {"key": "value"}
    assert dataclass_instance["none_field"] is None

    # Test nested dataclass
    nested = args["_nested_dataclass"]
    assert isinstance(nested, dict)
    nested_dataclass = nested["level1"]["dataclass"]
    assert isinstance(nested_dataclass, dict)
    assert nested_dataclass["string_field"] == "nested"
    assert nested_dataclass["int_field"] == 100
    assert nested_dataclass["float_field"] == 2.718
    assert nested_dataclass["bool_field"] is False

    # Test dataclass list
    dataclass_list = args["_dataclass_list"]
    assert isinstance(dataclass_list, list)
    assert len(dataclass_list) == 2

    # Test first dataclass in list
    first_dataclass = dataclass_list[0]
    assert isinstance(first_dataclass, dict)
    assert first_dataclass["string_field"] == "list_item_1"
    assert first_dataclass["int_field"] == 1
    assert first_dataclass["bool_field"] is True

    # Test second dataclass in list
    second_dataclass = dataclass_list[1]
    assert isinstance(second_dataclass, dict)
    assert second_dataclass["string_field"] == "list_item_2"
    assert second_dataclass["int_field"] == 2
    assert second_dataclass["bool_field"] is False


def test_dataclass_serialization_with_sanitize():
    """Test that dataclass instances are properly serialized through sanitize_for_struct."""

    from ray.data._internal.metadata_exporter import sanitize_for_struct

    # Create dummy operator with dataclass
    dummy_op = DummyLogicalOperatorWithDataclass()

    # Get the args
    args = dummy_op._get_args()

    # Test that sanitize_for_struct can handle the dataclass data
    try:
        sanitized = sanitize_for_struct(args)
        # If we get here, serialization succeeded
        assert isinstance(sanitized, dict)

        # Test that all keys are strings
        for key in sanitized.keys():
            assert isinstance(key, str), f"Expected string key, got {type(key)}: {key}"

        # Test that dataclass was properly converted
        dataclass_instance = sanitized["_dataclass_instance"]
        assert isinstance(dataclass_instance, dict)
        assert dataclass_instance["string_field"] == "test_string"
        assert dataclass_instance["int_field"] == 42

    except Exception as e:
        pytest.fail(f"Dataclass serialization failed: {e}")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
