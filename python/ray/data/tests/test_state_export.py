import os
import ray
import pytest
import json

from ray.data._internal.stats import _get_or_create_stats_actor

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
def enable_export_api_config(shutdown_only):
    """Enable export API for the EXPORT_TRAIN_RUN resource type."""
    ray.init(
        num_cpus=4,
        runtime_env={
            "env_vars": {"RAY_enable_export_api_write_config": "EXPORT_DATA_METADATA"}
        },
    )
    yield


@pytest.fixture
def enable_export_api_write(shutdown_only):
    """Enable export API for all resource types."""
    ray.init(
        num_cpus=4,
        runtime_env={"env_vars": {"RAY_enable_export_api_write": "1"}},
    )
    yield


@pytest.fixture
def dummy_dataset_dag_structure():
    """Create a dummy dataset DAG structure."""
    return {
        "operators": [
            {
                "name": "Input",
                "id": "Input_0",
                "uuid": "uuid_0",
                "input_dependencies": [],
                "sub_operators": [],
            },
            {
                "name": "ReadRange->Map(<lambda>)->Filter(<lambda>)",
                "id": "ReadRange->Map(<lambda>)->Filter(<lambda>)_1",
                "uuid": "uuid_1",
                "input_dependencies": ["Input_0"],
                "sub_operators": [],
            },
        ]
    }


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
    assert data[0]["event_data"]["dag"] == dag_structure
    assert data[0]["event_data"]["dataset_id"] == STUB_DATASET_ID
    assert data[0]["event_data"]["job_id"] == STUB_JOB_ID
    assert data[0]["event_data"]["start_time"] is not None


def test_export_data_metadata_enabled_by_config(
    enable_export_api_config, dummy_dataset_dag_structure
):
    _test_data_metadata_export(dummy_dataset_dag_structure)


def test_export_data_metadata(enable_export_api_write, dummy_dataset_dag_structure):
    _test_data_metadata_export(dummy_dataset_dag_structure)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
