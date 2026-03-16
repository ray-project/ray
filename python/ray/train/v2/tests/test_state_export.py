import json
import os

import pytest
from google.protobuf.json_format import MessageToDict, MessageToJson

import ray
from ray.train.v2._internal.state.export import _dict_to_human_readable_struct
from ray.train.v2._internal.state.schema import (
    RunAttemptStatus,
    RunStatus,
)
from ray.train.v2._internal.state.state_actor import get_or_create_state_actor
from ray.train.v2.tests.util import (
    create_mock_train_run,
    create_mock_train_run_attempt,
)


@pytest.fixture
def shutdown_only():
    yield
    ray.shutdown()


def _get_export_file_path() -> str:
    return os.path.join(
        ray._private.worker._global_node.get_session_dir_path(),
        "logs",
        "export_events",
        "event_EXPORT_TRAIN_STATE.log",
    )


def _get_exported_data():
    exported_file = _get_export_file_path()
    assert os.path.isfile(exported_file)

    with open(exported_file, "r") as f:
        data = f.readlines()

    return [json.loads(line) for line in data]


@pytest.fixture
def enable_export_api_config(shutdown_only):
    """Enable export API for the EXPORT_TRAIN_RUN resource type."""
    ray.init(
        num_cpus=4,
        runtime_env={
            "env_vars": {"RAY_enable_export_api_write_config": "EXPORT_TRAIN_RUN"}
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


def test_export_disabled(ray_start_4_cpus):
    """Test that no export files are created when export API is disabled."""
    state_actor = get_or_create_state_actor()

    # Create or update train run
    ray.get(state_actor.create_or_update_train_run.remote(create_mock_train_run()))
    ray.get(
        state_actor.create_or_update_train_run_attempt.remote(
            create_mock_train_run_attempt()
        )
    )

    # Check that no export files were created
    assert not os.path.exists(_get_export_file_path())


def _test_train_run_export():
    """Test that train run export events are written when export API is enabled."""
    state_actor = get_or_create_state_actor()

    # Create or update train run
    ray.get(
        state_actor.create_or_update_train_run.remote(
            create_mock_train_run(RunStatus.RUNNING)
        )
    )

    # Check that export files were created
    data = _get_exported_data()

    assert len(data) == 1
    assert data[0]["source_type"] == "EXPORT_TRAIN_RUN"
    assert data[0]["event_data"]["status"] == "RUNNING"


def test_export_train_run_enabled_by_config(enable_export_api_config):
    _test_train_run_export()


def test_export_train_run(enable_export_api_write):
    _test_train_run_export()


def test_export_train_run_run_settings_fields(enable_export_api_write):
    """Test that RunSettings fields are exported with expected shapes/types.

    This is a regression test for the JSON export format produced by the Train state
    export logger and the `export_train_state.proto` schema.
    """
    state_actor = get_or_create_state_actor()

    ray.get(
        state_actor.create_or_update_train_run.remote(
            create_mock_train_run(RunStatus.RUNNING)
        )
    )

    data = _get_exported_data()
    assert len(data) == 1
    run_settings = data[0]["event_data"]["run_settings"]

    assert run_settings == {
        "backend_config": {
            "config": {},
            "framework": "TRAINING_FRAMEWORK_UNSPECIFIED",
        },
        "scaling_config": {
            "num_workers_fixed": 1,
            "placement_strategy": "PACK",
            "use_gpu": False,
            "use_tpu": False,
            "bundle_label_selector": [],
        },
        "datasets": ["dataset_1"],
        "data_config": {"all": {}, "enable_shard_locality": True},
        "run_config": {
            "name": "test_run",
            "failure_config": {"controller_failure_limit": -1, "max_failures": 0},
            "worker_runtime_env": {"type": "conda"},
            "checkpoint_config": {"checkpoint_score_order": "MAX"},
            "storage_path": "s3://bucket/path",
        },
    }


def test_export_oneof_num_workers(enable_export_api_write):
    """
    Test that the num_workers oneof field exports correctly for both fixed for a fixed
    number of workers and range for when elastic training is enabled.
    """
    state_actor = get_or_create_state_actor()

    run_fixed = create_mock_train_run(RunStatus.RUNNING, id="fixed_workers")
    run_fixed.run_settings.scaling_config.num_workers = 4

    run_range = create_mock_train_run(RunStatus.RUNNING, id="range_workers")
    run_range.run_settings.scaling_config.num_workers = (2, 8)

    ray.get(
        [
            state_actor.create_or_update_train_run.remote(run_fixed),
            state_actor.create_or_update_train_run.remote(run_range),
        ]
    )

    runs = _get_exported_data()
    assert len(runs) == 2

    runs_by_id = {run["event_data"]["id"]: run for run in runs}

    # Fixed num_workers
    fixed_scaling = runs_by_id["fixed_workers"]["event_data"]["run_settings"][
        "scaling_config"
    ]
    assert fixed_scaling["num_workers_fixed"] == 4
    assert "num_workers_range" not in fixed_scaling

    # Range num_workers
    range_scaling = runs_by_id["range_workers"]["event_data"]["run_settings"][
        "scaling_config"
    ]
    assert range_scaling["num_workers_range"] == {"min": 2, "max": 8}
    assert "num_workers_fixed" not in range_scaling


def test_export_train_loop_config_integer_keys(enable_export_api_write):
    """Test that integer keys in train_loop_config are properly stringified in export."""
    state_actor = get_or_create_state_actor()

    ray.get(
        state_actor.create_or_update_train_run.remote(
            create_mock_train_run(train_loop_config={1: "one", 2: "two"})
        )
    )

    data = _get_exported_data()
    assert len(data) == 1

    run_settings = data[0]["event_data"]["run_settings"]

    assert run_settings["train_loop_config"] == {"1": "one", "2": "two"}


def test_export_train_run_attempt(enable_export_api_write):
    """Test that train run attempt export events are written when export API is enabled."""
    state_actor = get_or_create_state_actor()

    # Create or update train run attempt
    ray.get(
        state_actor.create_or_update_train_run_attempt.remote(
            create_mock_train_run_attempt(RunAttemptStatus.RUNNING)
        )
    )

    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["source_type"] == "EXPORT_TRAIN_RUN_ATTEMPT"
    assert data[0]["event_data"]["status"] == "RUNNING"


def test_dict_to_human_readable_struct_complex_dict():
    class UserObj:
        def __init__(self):
            self.a = 1
            self.b = "hello"

        def __str__(self) -> str:
            return "UserObj(a=1, b=hello)"

    obj = {
        "user_obj": UserObj(),
        "nested_dict": {"level1": 2},
        "list": [UserObj(), 3, 4],
        "json_native_types": {
            "str": "t",
            "int": 1,
            "float": 1.5,
            "bool": True,
            "none": None,
        },
        42: "integer_key_value",
    }

    expected = {
        "user_obj": "UserObj(a=1, b=hello)",
        "nested_dict": {"level1": 2},
        "list": [
            "UserObj(a=1, b=hello)",
            3,
            4,
        ],
        "json_native_types": {
            "str": "t",
            "int": 1,
            "float": 1.5,
            "bool": True,
            "none": None,
        },
        "42": "integer_key_value",
    }

    assert json.loads(MessageToJson(_dict_to_human_readable_struct(obj))) == expected


def test_dict_to_human_readable_struct_max_depth():
    """Test that _dict_to_human_readable_struct respects the max_depth argument.

    - `max_depth <= 0` raises.
    - Depth counts only dict nesting; lists recurse at the same depth as their parent,
      so a list of primitives is always shown in full regardless of depth.
    - Positive depths truncate nested dicts and custom objects appropriately.
    """

    class CustomObj:
        def __str__(self) -> str:
            return "CustomObj"

    obj = {
        "native": 42,
        "sequence": [1, CustomObj()],
        "nested": {"inner": {"deep": 99}},
        "obj": CustomObj(),
    }

    # max_depth <= 0 raises
    with pytest.raises(ValueError, match="max_depth must be greater than 0"):
        _dict_to_human_readable_struct(obj, max_depth=0)

    # max_depth=2: lists shown in full (lists don't consume depth);
    # dict nested 2 levels deep is truncated at the innermost level
    assert json.loads(
        MessageToJson(_dict_to_human_readable_struct(obj, max_depth=2))
    ) == {
        "native": 42,
        "nested": {"inner": "..."},
        "obj": "CustomObj",
        "sequence": [1, "CustomObj"],
    }

    # max_depth=3: all dict nesting fits within depth; full output
    assert json.loads(
        MessageToJson(_dict_to_human_readable_struct(obj, max_depth=3))
    ) == {
        "native": 42,
        "nested": {"inner": {"deep": 99}},
        "obj": "CustomObj",
        "sequence": [1, "CustomObj"],
    }


def test_dict_to_human_readable_struct_non_dict_raises():
    """Test that _dict_to_human_readable_struct raises ValueError for non-dict inputs."""
    with pytest.raises(ValueError, match="argument must be a dictionary"):
        _dict_to_human_readable_struct([1, 2, 3])

    with pytest.raises(ValueError, match="argument must be a dictionary"):
        _dict_to_human_readable_struct("a string")

    with pytest.raises(ValueError, match="argument must be a dictionary"):
        _dict_to_human_readable_struct(42)


def test_dict_to_human_readable_struct_conversion_error_falls_back_to_empty():
    """Test that when conversion raises unexpectedly, an empty Struct is returned."""

    class BrokenStr:
        def __str__(self):
            raise RuntimeError("__str__ failed")

    result = _dict_to_human_readable_struct({"key": BrokenStr()})
    assert MessageToDict(result) == {}


def test_export_oneof_datasets_to_split(enable_export_api_write):
    """Test that proto oneof fields are exported with only the active variant set.

    In ExportTrainRunEventData.RunSettings.DataConfig, `datasets_to_split` is a oneof:
    - `all`
    - `datasets` (StringList)
    """
    state_actor = get_or_create_state_actor()

    run_all = create_mock_train_run(RunStatus.RUNNING, id="with_all")
    run_all.run_settings.data_config.datasets_to_split = "all"

    run_ds = create_mock_train_run(RunStatus.RUNNING, id="with_datasets")
    run_ds.run_settings.data_config.datasets_to_split = ["dataset_1", "dataset_2"]

    ray.get(
        [
            state_actor.create_or_update_train_run.remote(run_all),
            state_actor.create_or_update_train_run.remote(run_ds),
        ]
    )

    runs = _get_exported_data()
    assert len(runs) == 2

    runs_by_id = {run["event_data"]["id"]: run for run in runs}
    assert set(runs_by_id.keys()) == {"with_all", "with_datasets"}

    # Verify train data config when splitting all datasets
    all_settings = runs_by_id["with_all"]["event_data"]["run_settings"]
    assert "data_config" in all_settings
    assert "all" in all_settings["data_config"]
    assert "datasets" not in all_settings["data_config"]

    # Verify train data config when splitting specific datasets
    ds_settings = runs_by_id["with_datasets"]["event_data"]["run_settings"]
    assert "data_config" in ds_settings
    assert "datasets" in ds_settings["data_config"]
    assert (
        ds_settings["data_config"]["datasets"]["values"]
        == run_ds.run_settings.data_config.datasets_to_split
    )
    assert "all" not in ds_settings["data_config"]


def test_export_multiple_source_types(enable_export_api_write):
    """Test that multiple source types (Run and RunAttempt) can be written to the same file."""
    state_actor = get_or_create_state_actor()

    events = [
        state_actor.create_or_update_train_run.remote(
            create_mock_train_run(RunStatus.RUNNING)
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            create_mock_train_run_attempt(
                attempt_id="attempt_1", status=RunAttemptStatus.RUNNING
            )
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            create_mock_train_run_attempt(
                attempt_id="attempt_2", status=RunAttemptStatus.RUNNING
            )
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            create_mock_train_run_attempt(
                attempt_id="attempt_1", status=RunAttemptStatus.FINISHED
            )
        ),
        state_actor.create_or_update_train_run_attempt.remote(
            create_mock_train_run_attempt(
                attempt_id="attempt_2", status=RunAttemptStatus.FINISHED
            )
        ),
        state_actor.create_or_update_train_run.remote(
            create_mock_train_run(RunStatus.FINISHED)
        ),
    ]
    ray.get(events)

    data = _get_exported_data()
    assert len(data) == len(events)

    expected_source_types = (
        ["EXPORT_TRAIN_RUN"] + ["EXPORT_TRAIN_RUN_ATTEMPT"] * 4 + ["EXPORT_TRAIN_RUN"]
    )
    expected_statuses = ["RUNNING"] * 3 + ["FINISHED"] * 3

    assert [d["source_type"] for d in data] == expected_source_types
    assert [d["event_data"]["status"] for d in data] == expected_statuses


def test_export_optional_fields(enable_export_api_write):
    """Test that optional fields are correctly exported when present and absent.

    This covers both top-level optional fields on TrainRun/TrainRunAttempt and
    optional nested fields inside TrainRun.run_settings (e.g., scaling/data/run
    configs).
    """
    state_actor = get_or_create_state_actor()

    # Create run with optional fields
    run_with_optional = create_mock_train_run(RunStatus.FINISHED)
    run_with_optional.status_detail = "Finished with details"
    run_with_optional.end_time_ns = 1000000000000000000
    run_with_optional.run_settings.train_loop_config = {"epochs": 1}
    run_with_optional.run_settings.scaling_config.resources_per_worker = {"CPU": 1}
    run_with_optional.run_settings.scaling_config.accelerator_type = "A100"
    run_with_optional.run_settings.scaling_config.topology = "v4-8"
    run_with_optional.run_settings.scaling_config.bundle_label_selector = {"k": "v"}
    run_with_optional.run_settings.data_config.datasets_to_split = ["dataset_1"]
    run_with_optional.run_settings.data_config.execution_options = {"foo": "bar"}
    run_with_optional.run_settings.run_config.checkpoint_config.num_to_keep = 2
    run_with_optional.run_settings.run_config.checkpoint_config.checkpoint_score_attribute = (
        "score"
    )
    run_with_optional.run_settings.run_config.storage_filesystem = (
        "<pyarrow._fs.S3FileSystem object>"
    )

    # Create attempt with optional fields
    attempt_with_optional = create_mock_train_run_attempt(
        attempt_id="attempt_with_optional",
        status=RunAttemptStatus.FINISHED,
    )
    attempt_with_optional.status_detail = "Attempt details"
    attempt_with_optional.end_time_ns = 1000000000000000000

    # Create and update states
    events = [
        state_actor.create_or_update_train_run.remote(create_mock_train_run()),
        state_actor.create_or_update_train_run_attempt.remote(
            create_mock_train_run_attempt()
        ),
        state_actor.create_or_update_train_run.remote(run_with_optional),
        state_actor.create_or_update_train_run_attempt.remote(attempt_with_optional),
    ]
    ray.get(events)

    data = _get_exported_data()

    assert len(data) == 4

    # Verify train run without optional fields
    run_data = data[0]

    assert run_data["source_type"] == "EXPORT_TRAIN_RUN"
    assert "status_detail" not in run_data["event_data"]
    assert "end_time_ns" not in run_data["event_data"]
    assert "run_settings" in run_data["event_data"]
    run_settings = run_data["event_data"]["run_settings"]
    # Optional protobuf scalar fields use their defaults
    assert "train_loop_config" not in run_settings
    assert "checkpoint_config" in run_settings["run_config"]
    assert "num_to_keep" not in run_settings["run_config"]["checkpoint_config"]
    assert (
        "checkpoint_score_attribute"
        not in run_settings["run_config"]["checkpoint_config"]
    )
    assert "scaling_config" in run_settings
    assert "resources_per_worker" not in run_settings["scaling_config"]
    assert "accelerator_type" not in run_settings["scaling_config"]
    assert "topology" not in run_settings["scaling_config"]
    assert "data_config" in run_settings
    assert "execution_options" not in run_settings["data_config"]
    assert "storage_filesystem" not in run_settings["run_config"]

    # Verify train run attempt without optional fields
    attempt_data = data[1]
    assert attempt_data["source_type"] == "EXPORT_TRAIN_RUN_ATTEMPT"
    assert "status_detail" not in attempt_data["event_data"]
    assert "end_time_ns" not in attempt_data["event_data"]

    # Verify train run with optional fields
    run_data = data[2]
    assert run_data["source_type"] == "EXPORT_TRAIN_RUN"
    assert run_data["event_data"]["status_detail"] == "Finished with details"
    assert "end_time_ns" in run_data["event_data"]

    run_settings = run_data["event_data"]["run_settings"]
    assert run_settings["train_loop_config"] == {"epochs": 1.0}
    assert run_settings["scaling_config"]["resources_per_worker"]["values"] == {
        k: float(v)
        for k, v in run_with_optional.run_settings.scaling_config.resources_per_worker.items()
    }
    assert (
        run_settings["scaling_config"]["accelerator_type"]
        == run_with_optional.run_settings.scaling_config.accelerator_type
    )
    assert (
        run_settings["scaling_config"]["topology"]
        == run_with_optional.run_settings.scaling_config.topology
    )
    assert run_settings["scaling_config"]["bundle_label_selector"] == [
        {"values": {"k": "v"}}
    ]
    assert (
        run_settings["data_config"]["datasets"]["values"]
        == run_with_optional.run_settings.data_config.datasets_to_split
    )
    assert (
        run_settings["data_config"]["execution_options"]
        == run_with_optional.run_settings.data_config.execution_options
    )
    assert run_settings["run_config"]["checkpoint_config"]["num_to_keep"] == str(
        run_with_optional.run_settings.run_config.checkpoint_config.num_to_keep
    )
    assert (
        run_settings["run_config"]["checkpoint_config"]["checkpoint_score_attribute"]
        == run_with_optional.run_settings.run_config.checkpoint_config.checkpoint_score_attribute
    )
    assert (
        run_settings["run_config"]["storage_filesystem"]
        == run_with_optional.run_settings.run_config.storage_filesystem
    )

    # Verify train run attempt with optional fields
    attempt_data = data[3]
    assert attempt_data["source_type"] == "EXPORT_TRAIN_RUN_ATTEMPT"
    assert attempt_data["event_data"]["status_detail"] == "Attempt details"
    assert "end_time_ns" in attempt_data["event_data"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
