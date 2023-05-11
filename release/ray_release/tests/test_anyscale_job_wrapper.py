import pytest
import sys
import json

from ray_release.command_runner._anyscale_job_wrapper import (
    main,
    run_bash_command,
    TIMEOUT_RETURN_CODE,
    OUTPUT_JSON_FILENAME,
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
