import os
import ray
import pytest
import json
from dataclasses import asdict

from ray.data._internal.stats import _get_or_create_stats_actor
from ray.data._internal.metadata_exporter import OperatorDAG, Operator

STUB_JOB_ID = "stub_job_id"
STUB_DATASET_ID = "stub_dataset_id"


def _get_export_file_path() -> str:
    return os.path.join(
        ray._private.worker._global_node.get_session_dir_path(),
        "logs",
        "export_events",
        "event_EXPORT_DATA_METADATA.log",
    )


def _get_exported_data():
    exported_file = _get_export_file_path()
    assert os.path.isfile(exported_file)

    with open(exported_file, "r") as f:
        data = f.readlines()
    print(exported_file)
    return [json.loads(line) for line in data]


@pytest.fixture
def ray_start_cluster_with_export_api_config(shutdown_only):
    """Enable export API for the EXPORT_TRAIN_RUN resource type."""
    ray.init(
        num_cpus=4,
        runtime_env={
            "env_vars": {"RAY_enable_export_api_write_config": "EXPORT_DATA_METADATA"}
        },
    )
    yield


@pytest.fixture
def ray_start_cluster_with_export_api_write(shutdown_only):
    """Enable export API for all resource types."""
    ray.init(
        num_cpus=4,
        runtime_env={"env_vars": {"RAY_enable_export_api_write": "1"}},
    )
    yield


@pytest.fixture
def dummy_dataset_dag_structure():
    """Create a dummy OperatorDAG."""
    dummy_dag_structure = OperatorDAG(
        operators=[
            Operator(
                name="Input",
                id="Input_0",
                uuid="uuid_0",
                input_dependencies=[],
                sub_operators=[],
            ),
            Operator(
                name="ReadRange->Map(<lambda>)->Filter(<lambda>)",
                id="ReadRange->Map(<lambda>)->Filter(<lambda>)_1",
                uuid="uuid_1",
                input_dependencies=["Input_0"],
                sub_operators=[],
            ),
        ],
    )
    return dummy_dag_structure


def test_export_disabled(ray_start_regular, dummy_dataset_dag_structure):
    """Test that no export files are created when export API is disabled."""
    stats_actor = _get_or_create_stats_actor()
    # self, dataset_tag, operator_tags, dag_structure=None

    # Create or update train run
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag="test_dataset",
            operator_tags=["ReadRange->Map(<lambda>)->Filter(<lambda>)"],
            dag_structure=dummy_dataset_dag_structure,
            job_id=STUB_JOB_ID,
        )
    )

    # Check that no export files were created
    assert not os.path.exists(_get_export_file_path())


def _test_data_metadata_export(dag_structure):
    """Test that data metadata export events are written when export API is enabled."""
    stats_actor = _get_or_create_stats_actor()

    # Simulate a dataset registration
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag=STUB_DATASET_ID,
            operator_tags=["ReadRange->Map(<lambda>)->Filter(<lambda>)"],
            dag_structure=dag_structure,
            job_id=STUB_JOB_ID,
        )
    )

    # Check that export files were created
    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["source_type"] == "EXPORT_DATA_METADATA"
    assert data[0]["event_data"]["dag"] == asdict(dag_structure)
    assert data[0]["event_data"]["dataset_id"] == STUB_DATASET_ID
    assert data[0]["event_data"]["job_id"] == STUB_JOB_ID
    assert data[0]["event_data"]["start_time"] is not None


def test_export_data_metadata_enabled_by_config(
    ray_start_cluster_with_export_api_config, dummy_dataset_dag_structure
):
    _test_data_metadata_export(dummy_dataset_dag_structure)


def test_export_data_metadata(
    ray_start_cluster_with_export_api_write, dummy_dataset_dag_structure
):
    _test_data_metadata_export(dummy_dataset_dag_structure)


def test_export_multiple_datasets(
    ray_start_cluster_with_export_api_write, dummy_dataset_dag_structure
):
    """Test that multiple datasets can be exported when export API is enabled."""
    stats_actor = _get_or_create_stats_actor()

    # Create a second dataset structure that's different from the dummy one
    second_dag_structure = OperatorDAG(
        operators=[
            Operator(
                name="Input",
                id="Input_0",
                uuid="second_uuid_0",
                input_dependencies=[],
                sub_operators=[],
            ),
            Operator(
                name="ReadRange->Map(<lambda>)",
                id="ReadRange->Map(<lambda>)_1",
                uuid="second_uuid_1",
                input_dependencies=["Input_0"],
                sub_operators=[],
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
            dag_structure=dummy_dataset_dag_structure,
            job_id=STUB_JOB_ID,
        )
    )

    # Register the second dataset
    ray.get(
        stats_actor.register_dataset.remote(
            dataset_tag=second_dataset_id,
            operator_tags=["ReadRange->Map(<lambda>)"],
            dag_structure=second_dag_structure,
            job_id=STUB_JOB_ID,
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
    assert first_entry["source_type"] == "EXPORT_DATA_METADATA"
    assert first_entry["event_data"]["dag"] == asdict(dummy_dataset_dag_structure)
    assert first_entry["event_data"]["job_id"] == STUB_JOB_ID
    assert first_entry["event_data"]["start_time"] is not None

    # Verify second dataset
    assert (
        second_dataset_id in datasets_by_id
    ), f"Second dataset {second_dataset_id} not found in exported data"
    second_entry = datasets_by_id[second_dataset_id]
    assert second_entry["source_type"] == "EXPORT_DATA_METADATA"
    assert second_entry["event_data"]["dag"] == asdict(second_dag_structure)
    assert second_entry["event_data"]["job_id"] == STUB_JOB_ID
    assert second_entry["event_data"]["start_time"] is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
