import json
import os
from dataclasses import asdict, dataclass
from typing import Tuple

import pytest

import ray
from ray.data._internal.execution.dataset_state import DatasetState
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.metadata_exporter import (
    UNKNOWN,
    Operator,
    Topology,
    sanitize_for_struct,
)
from ray.data._internal.stats import _get_or_create_stats_actor
from ray.data.context import DataContext
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


@dataclass
class TestDataclass:
    """A test dataclass for testing dataclass serialization."""

    list_field: list = None
    dict_field: dict = None
    string_field: str = "test"
    int_field: int = 1
    float_field: float = 1.0
    set_field: set = None
    tuple_field: Tuple[int] = None
    bool_field: bool = True
    none_field: None = None

    def __post_init__(self):
        self.list_field = [1, 2, 3]
        self.dict_field = {1: 2, "3": "4"}
        self.set_field = {1, 2, 3}
        self.tuple_field = (1, 2, 3)


class DummyLogicalOperator(LogicalOperator):
    """A dummy logical operator for testing _get_logical_args with various data types."""

    def __init__(self, input_op=None):
        super().__init__("DummyOperator", [])

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
        self._set_value = {1}
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

        self._data_class = TestDataclass()


@pytest.fixture
def dummy_dataset_topology():
    """Create a dummy Topology."""
    dummy_operator = DummyLogicalOperator()
    dummy_topology = Topology(
        operators=[
            Operator(
                name="Input",
                id="Input_0",
                uuid="uuid_0",
                input_dependencies=[],
                sub_stages=[],
                execution_start_time=1.0,
                execution_end_time=1.0,
                state="FINISHED",
                args=sanitize_for_struct(dummy_operator._get_args()),
            ),
            Operator(
                name="ReadRange->Map(<lambda>)->Filter(<lambda>)",
                id="ReadRange->Map(<lambda>)->Filter(<lambda>)_1",
                uuid="uuid_1",
                input_dependencies=["Input_0"],
                sub_stages=[],
                execution_start_time=0.0,
                execution_end_time=0.0,
                state="RUNNING",
                args=sanitize_for_struct(dummy_operator._get_args()),
            ),
        ],
    )
    return dummy_topology


@pytest.fixture
def dummy_dataset_topology_expected_output():
    return {
        "operators": [
            {
                "name": "Input",
                "id": "Input_0",
                "uuid": "uuid_0",
                "args": {
                    "_num_outputs": "None",
                    "_int_value": "42",
                    "_special_values": {
                        "negative": "-1",
                        "inf": "inf",
                        "zero": "0",
                        "large_int": "999999999999999999",
                        "small_float": "1e-07",
                        "neg_inf": "-inf",
                        "nan": "nan",
                    },
                    "_none_value": "None",
                    "_name": "DummyOperator",
                    "_output_dependencies": [],
                    "_float_value": "3.14",
                    "_list_value": ["1", "2", "3", "string", "None"],
                    "_dict_value": {"key1": "value1", "key3": "None", "key2": "123"},
                    "_set_value": ["1"],
                    "_tuple_value": ["1", "string", "None", "3.14"],
                    "_bytes_value": [
                        "98",
                        "105",
                        "110",
                        "97",
                        "114",
                        "121",
                        "95",
                        "100",
                        "97",
                        "116",
                        "97",
                    ],
                    "_input_dependencies": [],
                    "_empty_containers": {
                        "empty_set": [],
                        "empty_tuple": [],
                        "empty_dict": {},
                        "empty_list": [],
                    },
                    "_bool_value": "True",
                    "_nested_dict": {
                        "level1": {
                            "level2": {
                                "mixed": {"a": "1", "b": "string", "c": "None"},
                                "numbers": ["1", "2", "3"],
                                "level3": "deep_value",
                            }
                        }
                    },
                    "_string_value": "test_string",
                    "_complex_dict": {
                        "string_keys": {"a": "1", "b": "2"},
                        "mixed_keys": {
                            "None": "none_key",
                            "str": "value",
                            "1": "int_key",
                        },
                        "int_keys": {"1": "one", "2": "two"},
                    },
                    "_data_class": {
                        "list_field": ["1", "2", "3"],
                        "dict_field": {"3": "4", "1": "2"},
                        "tuple_field": ["1", "2", "3"],
                        "set_field": ["1", "2", "3"],
                        "int_field": "1",
                        "none_field": "None",
                        "bool_field": "True",
                        "string_field": "test",
                        "float_field": "1.0",
                    },
                },
                "input_dependencies": [],
                "sub_stages": [],
                "execution_start_time": 1.0,
                "execution_end_time": 1.0,
                "state": "FINISHED",
            },
            {
                "name": "ReadRange->Map(<lambda>)->Filter(<lambda>)",
                "id": "ReadRange->Map(<lambda>)->Filter(<lambda>)_1",
                "uuid": "uuid_1",
                "input_dependencies": ["Input_0"],
                "args": {
                    "_num_outputs": "None",
                    "_int_value": "42",
                    "_special_values": {
                        "negative": "-1",
                        "inf": "inf",
                        "zero": "0",
                        "large_int": "999999999999999999",
                        "small_float": "1e-07",
                        "neg_inf": "-inf",
                        "nan": "nan",
                    },
                    "_none_value": "None",
                    "_name": "DummyOperator",
                    "_output_dependencies": [],
                    "_float_value": "3.14",
                    "_list_value": ["1", "2", "3", "string", "None"],
                    "_dict_value": {"key1": "value1", "key3": "None", "key2": "123"},
                    "_set_value": ["1"],
                    "_tuple_value": ["1", "string", "None", "3.14"],
                    "_bytes_value": [
                        "98",
                        "105",
                        "110",
                        "97",
                        "114",
                        "121",
                        "95",
                        "100",
                        "97",
                        "116",
                        "97",
                    ],
                    "_input_dependencies": [],
                    "_empty_containers": {
                        "empty_set": [],
                        "empty_tuple": [],
                        "empty_dict": {},
                        "empty_list": [],
                    },
                    "_bool_value": "True",
                    "_nested_dict": {
                        "level1": {
                            "level2": {
                                "mixed": {"a": "1", "b": "string", "c": "None"},
                                "numbers": ["1", "2", "3"],
                                "level3": "deep_value",
                            }
                        }
                    },
                    "_string_value": "test_string",
                    "_complex_dict": {
                        "string_keys": {"a": "1", "b": "2"},
                        "mixed_keys": {
                            "None": "none_key",
                            "str": "value",
                            "1": "int_key",
                        },
                        "int_keys": {"1": "one", "2": "two"},
                    },
                    "_data_class": {
                        "list_field": ["1", "2", "3"],
                        "dict_field": {"3": "4", "1": "2"},
                        "tuple_field": ["1", "2", "3"],
                        "set_field": ["1", "2", "3"],
                        "int_field": "1",
                        "none_field": "None",
                        "bool_field": "True",
                        "string_field": "test",
                        "float_field": "1.0",
                    },
                },
                "sub_stages": [],
                "execution_start_time": 0.0,
                "execution_end_time": 0.0,
                "state": "RUNNING",
            },
        ]
    }


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


def _test_dataset_metadata_export(topology, dummy_dataset_topology_expected_output):
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
    assert data[0]["event_data"]["topology"] == dummy_dataset_topology_expected_output
    assert data[0]["event_data"]["dataset_id"] == STUB_DATASET_ID
    assert data[0]["event_data"]["job_id"] == STUB_JOB_ID
    assert data[0]["event_data"]["start_time"] is not None


def test_export_dataset_metadata_enabled_by_config(
    ray_start_cluster_with_export_api_config,
    dummy_dataset_topology,
    dummy_dataset_topology_expected_output,
):
    _test_dataset_metadata_export(
        dummy_dataset_topology, dummy_dataset_topology_expected_output
    )


def test_export_dataset_metadata(
    ray_start_cluster_with_export_api_write,
    dummy_dataset_topology,
    dummy_dataset_topology_expected_output,
):
    _test_dataset_metadata_export(
        dummy_dataset_topology, dummy_dataset_topology_expected_output
    )


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
    ray_start_cluster_with_export_api_write,
    dummy_dataset_topology,
    dummy_dataset_topology_expected_output,
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
                execution_start_time=1.0,
                execution_end_time=1.0,
                state="FINISHED",
            ),
            Operator(
                name="ReadRange->Map(<lambda>)",
                id="ReadRange->Map(<lambda>)_1",
                uuid="second_uuid_1",
                input_dependencies=["Input_0"],
                sub_stages=[],
                execution_start_time=2.0,
                execution_end_time=0.0,
                state="RUNNING",
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
    assert (
        first_entry["event_data"]["topology"] == dummy_dataset_topology_expected_output
    )
    assert first_entry["event_data"]["job_id"] == STUB_JOB_ID
    assert first_entry["event_data"]["start_time"] is not None

    # Verify second dataset
    assert (
        second_dataset_id in datasets_by_id
    ), f"Second dataset {second_dataset_id} not found in exported data"
    second_entry = datasets_by_id[second_dataset_id]
    assert second_entry["source_type"] == "EXPORT_DATASET_METADATA"
    assert second_entry["event_data"]["topology"] == asdict(second_topology)
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
        # Basic types - should return as strings
        (42, "42", 100),
        (3.14, "3.14", 100),
        (True, "True", 100),
        (False, "False", 100),
        (None, "None", 100),
        # Strings - short strings return as-is
        ("hello", "hello", 100),
        # Strings - long strings get truncated
        ("a" * 150, "a" * 100 + "...", 100),
        ("hello world", "hello...", 5),
        # Mappings - should recursively sanitize values
        ({"key": "value"}, {"key": "value"}, 100),
        ({"long_key": "a" * 150}, {"long_key": "a" * 100 + "..."}, 100),
        ({"nested": {"inner": "value"}}, {"nested": {"inner": "value"}}, 100),
        # Sequences - should recursively sanitize elements (convert to strings)
        ([1, 2, 3], ["1", "2", "3"], 100),
        (["short", "a" * 150], ["short", "a" * 100 + "..."], 100),
        # Complex nested structures
        (
            {"list": [1, "a" * 150], "dict": {"key": "a" * 150}},
            {"list": ["1", "a" * 100 + "..."], "dict": {"key": "a" * 100 + "..."}},
            100,
        ),
        # Objects that can be converted to string
        (BasicObject("test"), "BasicObject(test)", 100),  # Falls back to str()
        # Sets can be converted to Lists of strings
        ({1, 2, 3}, ["1", "2", "3"], 100),
        ((1, 2, 3), ["1", "2", "3"], 100),
        # Objects that can't be serialized or stringified
        (UnserializableObject(), f"{UNKNOWN}: {UnserializableObject.__name__}", 100),
        # Empty containers
        ({}, {}, 100),
        ([], [], 100),
        # Mixed type sequences - all converted to strings
        (
            [1, "hello", {"key": "value"}, None],
            ["1", "hello", {"key": "value"}, "None"],
            100,
        ),
        # Bytearrays/bytes - should be converted to lists of string representations
        (bytearray(b"hello"), ["104", "101", "108", "108", "111"], 100),
        (bytearray([1, 2, 3, 4, 5]), ["1", "2", "3", "4", "5"], 100),
        (bytes(b"test"), ["116", "101", "115", "116"], 100),
        # Dataclass
        (
            TestDataclass(),
            {
                "list_field": ["1", "2", "3"],
                "dict_field": {"1": "2", "3": "4"},  # key should be strings
                "string_field": "test",
                "int_field": "1",
                "float_field": "1.0",
                "set_field": [
                    "1",
                    "2",
                    "3",
                ],  # sets will be converted to Lists of strings
                "tuple_field": [
                    "1",
                    "2",
                    "3",
                ],  # tuples will be converted to Lists of strings
                "bool_field": "True",
                "none_field": "None",
            },
            100,
        ),
        # Test sequence truncation - list longer than truncate_length gets truncated
        (
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ["1", "2", "3", "..."],  # Only first 3 elements after truncation + ...
            3,
        ),
    ],
)
def test_sanitize_for_struct(input_obj, expected_output, truncate_length):
    """Test sanitize_for_struct with various input types and truncation lengths."""
    result = sanitize_for_struct(input_obj, truncate_length)
    assert result == expected_output, f"Expected {expected_output}, got {result}"


def test_update_dataset_metadata_state(
    ray_start_cluster_with_export_api_write, dummy_dataset_topology
):
    """Test dataset state update at the export API"""
    stats_actor = _get_or_create_stats_actor()
    # Register dataset
    ray.get(
        stats_actor.register_dataset.remote(
            job_id=STUB_JOB_ID,
            dataset_tag=STUB_DATASET_ID,
            operator_tags=["Input_0", "ReadRange->Map(<lambda>)->Filter(<lambda>)_1"],
            topology=dummy_dataset_topology,
            data_context=DataContext.get_current(),
        )
    )
    # Check that export files were created as expected
    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["event_data"]["state"] == DatasetState.PENDING.name

    # Test update state to RUNNING
    ray.get(
        stats_actor.update_dataset_metadata_state.remote(
            dataset_id=STUB_DATASET_ID, new_state=DatasetState.RUNNING.name
        )
    )
    data = _get_exported_data()
    assert len(data) == 2
    assert data[1]["event_data"]["state"] == DatasetState.RUNNING.name
    assert data[1]["event_data"]["execution_start_time"] > 0

    # Test update to FINISHED
    ray.get(
        stats_actor.update_dataset_metadata_state.remote(
            dataset_id=STUB_DATASET_ID, new_state=DatasetState.FINISHED.name
        )
    )
    data = _get_exported_data()
    assert len(data) == 3
    assert data[2]["event_data"]["state"] == DatasetState.FINISHED.name
    assert data[2]["event_data"]["execution_end_time"] > 0
    assert (
        data[2]["event_data"]["topology"]["operators"][1]["state"]
        == DatasetState.FINISHED.name
    )
    assert data[2]["event_data"]["topology"]["operators"][1]["execution_end_time"] > 0


def test_update_dataset_metadata_operator_states(
    ray_start_cluster_with_export_api_write, dummy_dataset_topology
):
    stats_actor = _get_or_create_stats_actor()
    # Register dataset
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag=STUB_DATASET_ID,
            operator_tags=["Input_0", "ReadRange->Map(<lambda>)->Filter(<lambda>)_1"],
            topology=dummy_dataset_topology,
            job_id=STUB_JOB_ID,
            data_context=DataContext.get_current(),
        )
    )
    data = _get_exported_data()
    assert len(data) == 1
    assert (
        data[0]["event_data"]["topology"]["operators"][1]["state"]
        == DatasetState.RUNNING.name
    )

    # Test update to FINISHED
    operator_id = "ReadRange->Map(<lambda>)->Filter(<lambda>)_1"
    ray.get(
        stats_actor.update_dataset_metadata_operator_states.remote(
            dataset_id=STUB_DATASET_ID,
            operator_states={operator_id: DatasetState.FINISHED.name},
        )
    )
    data = _get_exported_data()
    assert len(data) == 2
    assert (
        data[1]["event_data"]["topology"]["operators"][1]["state"]
        == DatasetState.FINISHED.name
    )
    assert data[1]["event_data"]["topology"]["operators"][1]["execution_end_time"] > 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
