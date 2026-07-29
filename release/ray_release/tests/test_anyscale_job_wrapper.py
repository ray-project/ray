import json
import os
import sys
from unittest.mock import patch

import pytest

from ray_release.command_runner._anyscale_job_wrapper import (
    OUTPUT_JSON_FILENAME,
    TIMEOUT_RETURN_CODE,
    main,
    run_bash_command,
    run_spilling_check,
)

cloud_storage_kwargs = dict(
    results_cloud_storage_uri=None,
    metrics_cloud_storage_uri=None,
    output_cloud_storage_uri=None,
    upload_cloud_storage_uri=None,
    artifact_path=None,
)


def test_run_bash_command_success():
    assert run_bash_command("exit 0", 1000) == 0


def test_run_bash_command_fail():
    assert run_bash_command("exit 1", 1000) == 1


def test_run_bash_command_timeout():
    assert run_bash_command("sleep 10", 1) == TIMEOUT_RETURN_CODE


def _check_output_json(expected_return_code, prepare_return_codes=None):
    with open(OUTPUT_JSON_FILENAME, "r") as fp:
        output = json.load(fp)
    assert output["return_code"] == expected_return_code
    assert output["prepare_return_codes"] == (prepare_return_codes or [])
    assert output["uploaded_results"] is False
    assert output["collected_metrics"] is False
    assert output["uploaded_metrics"] is False


def test_prepare_commands_validation(tmpdir):
    with pytest.raises(ValueError):
        main(
            test_workload="exit 0",
            test_workload_timeout=10,
            test_no_raise_on_timeout=False,
            prepare_commands=["exit 0"],
            prepare_commands_timeouts=[],
            **cloud_storage_kwargs
        )
    with pytest.raises(ValueError):
        main(
            test_workload="exit 0",
            test_workload_timeout=10,
            test_no_raise_on_timeout=False,
            prepare_commands=[],
            prepare_commands_timeouts=[1],
            **cloud_storage_kwargs
        )


def test_end_to_end(tmpdir):
    expected_return_code = 0
    assert (
        main(
            test_workload="exit 0",
            test_workload_timeout=10,
            test_no_raise_on_timeout=False,
            prepare_commands=[],
            prepare_commands_timeouts=[],
            **cloud_storage_kwargs
        )
        == expected_return_code
    )
    _check_output_json(expected_return_code)


def test_end_to_end_prepare_commands(tmpdir):
    expected_return_code = 0
    assert (
        main(
            test_workload="exit 0",
            test_workload_timeout=10,
            test_no_raise_on_timeout=False,
            prepare_commands=["exit 0", "exit 0"],
            prepare_commands_timeouts=[1, 1],
            **cloud_storage_kwargs
        )
        == expected_return_code
    )
    _check_output_json(expected_return_code, [0, 0])


def test_end_to_end_long_running(tmpdir):
    expected_return_code = 0
    assert (
        main(
            test_workload="sleep 10",
            test_workload_timeout=1,
            test_no_raise_on_timeout=True,
            prepare_commands=[],
            prepare_commands_timeouts=[],
            **cloud_storage_kwargs
        )
        == expected_return_code
    )
    _check_output_json(TIMEOUT_RETURN_CODE)


def test_end_to_end_timeout(tmpdir):
    expected_return_code = TIMEOUT_RETURN_CODE
    assert (
        main(
            test_workload="sleep 10",
            test_workload_timeout=1,
            test_no_raise_on_timeout=False,
            prepare_commands=[],
            prepare_commands_timeouts=[],
            **cloud_storage_kwargs
        )
        == expected_return_code
    )
    _check_output_json(expected_return_code)


def test_end_to_end_prepare_timeout(tmpdir):
    expected_return_code = 1
    assert (
        main(
            test_workload="exit 0",
            test_workload_timeout=10,
            test_no_raise_on_timeout=False,
            prepare_commands=["exit 0", "sleep 10"],
            prepare_commands_timeouts=[1, 1],
            **cloud_storage_kwargs
        )
        == expected_return_code
    )
    _check_output_json(None, [0, TIMEOUT_RETURN_CODE])


@pytest.mark.parametrize("long_running", (True, False))
def test_end_to_end_failure(tmpdir, long_running):
    expected_return_code = 1
    assert (
        main(
            test_workload="exit 1",
            test_workload_timeout=1,
            test_no_raise_on_timeout=long_running,
            prepare_commands=[],
            prepare_commands_timeouts=[],
            **cloud_storage_kwargs
        )
        == expected_return_code
    )
    _check_output_json(expected_return_code)


def test_end_to_end_prepare_failure(tmpdir):
    expected_return_code = 1
    assert (
        main(
            test_workload="exit 0",
            test_workload_timeout=10,
            test_no_raise_on_timeout=False,
            prepare_commands=["exit 0", "exit 1"],
            prepare_commands_timeouts=[1, 1],
            **cloud_storage_kwargs
        )
        == expected_return_code
    )
    _check_output_json(None, [0, 1])


_PROM_SPILL_SAMPLE = [
    {"metric": {}, "values": [[1700000000, "1073741824"]]},
]


@pytest.mark.parametrize(
    "metrics_payload,expected_return_code",
    [
        # No spilling — empty list (Prometheus `> 0` filter dropped all points)
        ({"spilled_bytes": []}, 0),
        # Spilling occurred — non-empty list
        ({"spilled_bytes": _PROM_SPILL_SAMPLE}, 1),
        # Missing value (None) — fail with "could not retrieve" error
        ({"spilled_bytes": None}, 1),
        # Missing key entirely — fail with "could not retrieve" error
        ({}, 1),
    ],
)
def test_run_spilling_check_with_metrics_file(
    tmpdir, metrics_payload, expected_return_code
):
    metrics_path = str(tmpdir / "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics_payload, f)
    with patch.dict(os.environ, {"METRICS_OUTPUT_JSON": metrics_path}):
        assert run_spilling_check() == expected_return_code


def test_run_spilling_check_missing_file(tmpdir):
    metrics_path = str(tmpdir / "missing.json")
    with patch.dict(os.environ, {"METRICS_OUTPUT_JSON": metrics_path}):
        assert run_spilling_check() == 1


def test_run_spilling_check_unset_metrics_env(tmpdir):
    env = {k: v for k, v in os.environ.items() if k != "METRICS_OUTPUT_JSON"}
    with patch.dict(os.environ, env, clear=True):
        assert run_spilling_check() == 1


def test_run_spilling_check_malformed_json(tmpdir):
    metrics_path = str(tmpdir / "metrics.json")
    with open(metrics_path, "w") as f:
        f.write("{not valid json")
    with patch.dict(os.environ, {"METRICS_OUTPUT_JSON": metrics_path}):
        assert run_spilling_check() == 1


@pytest.mark.parametrize("payload", [[1, 2, 3], "spilled_bytes", 42, None])
def test_run_spilling_check_non_dict_json(tmpdir, payload):
    # Valid JSON but not a dict, so `metrics.get(...)` raises AttributeError.
    metrics_path = str(tmpdir / "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(payload, f)
    with patch.dict(os.environ, {"METRICS_OUTPUT_JSON": metrics_path}):
        assert run_spilling_check() == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
