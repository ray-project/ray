import json
import os
from dataclasses import asdict

import pytest

import ray
from ray.data import DataContext
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
