import json
from unittest.mock import MagicMock, patch

import pytest

from ray_release.command_runner.anyscale_job_runner import (
    TIMEOUT_RETURN_CODE,
    AnyscaleJobRunner,
)
from ray_release.exception import (
    JobBrokenError,
    JobNoLogsError,
    JobOutOfRetriesError,
    PrepareCommandError,
    PrepareCommandTimeout,
    TestCommandError,
    TestCommandTimeout,
)
from ray_release.job_manager.anyscale_job_manager import (
    JOB_FAILED,
    JOB_SOFT_INFRA_ERROR,
    JOB_STATE_UNKNOWN,
    JOB_SUCCEEDED,
)


def _make_output_json(
    return_code=0,
    workload_time_taken=10.0,
    prepare_return_codes=None,
    last_prepare_time_taken=5.0,
):
    return {
        "return_code": return_code,
        "workload_time_taken": workload_time_taken,
        "prepare_return_codes": prepare_return_codes or [],
        "last_prepare_time_taken": last_prepare_time_taken,
        "uploaded_results": True,
        "uploaded_metrics": True,
        "uploaded_artifact": True,
    }


@pytest.fixture
def runner():
    with patch.object(AnyscaleJobRunner, "__init__", lambda self: None):
        r = AnyscaleJobRunner()
        r._results_uploaded = True
        r._metrics_uploaded = True
        r._artifact_uploaded = True
        r.prepare_commands = ["echo prepare"]
        return r


class TestHandleCommandOutputJobReturnCodes:
    def test_succeeded_with_output(self, runner):
        output = _make_output_json(return_code=0)
        runner.fetch_output = MagicMock(return_value=output)
        runner._handle_command_output(JOB_SUCCEEDED)

    def test_failed_with_output(self, runner):
        output = _make_output_json(return_code=1)
        runner.fetch_output = MagicMock(return_value=output)
        with pytest.raises(TestCommandError, match="1"):
            runner._handle_command_output(JOB_FAILED)

    def test_failed_without_output(self, runner):
        runner.fetch_output = MagicMock(side_effect=Exception("S3 error"))
        runner.get_last_logs = MagicMock(return_value=None)
        with pytest.raises(JobNoLogsError):
            runner._handle_command_output(JOB_FAILED)

    def test_soft_infra_error_raises(self, runner):
        with pytest.raises(JobOutOfRetriesError, match="FAILED"):
            runner._handle_command_output(JOB_SOFT_INFRA_ERROR)

    def test_state_unknown_raises(self, runner):
        with pytest.raises(JobBrokenError, match="UNKNOWN"):
            runner._handle_command_output(JOB_STATE_UNKNOWN)


class TestHandleCommandOutputFetchFailures:
    def test_no_output_and_no_logs_raises(self, runner):
        runner.fetch_output = MagicMock(side_effect=Exception("S3 error"))
        runner.get_last_logs = MagicMock(return_value=None)
        with pytest.raises(JobNoLogsError):
            runner._handle_command_output(0)

    def test_no_output_but_logs_parsed(self, runner):
        output = _make_output_json(return_code=0)
        log_line = f"### JSON |{json.dumps(output)}| ###"
        runner.fetch_output = MagicMock(side_effect=Exception("S3 error"))
        runner.get_last_logs = MagicMock(return_value=log_line)
        # Should succeed without raising
        runner._handle_command_output(0)

    def test_no_output_logs_with_nonzero_workload_status(self, runner):
        output = _make_output_json(return_code=1)
        log_line = f"### JSON |{json.dumps(output)}| ###"
        runner.fetch_output = MagicMock(side_effect=Exception("S3 error"))
        runner.get_last_logs = MagicMock(return_value=log_line)
        with pytest.raises(TestCommandError, match="1"):
            runner._handle_command_output(0)


class TestHandleCommandOutputPrepareCommands:
    def test_prepare_timeout_raises(self, runner):
        output = _make_output_json(
            prepare_return_codes=[TIMEOUT_RETURN_CODE],
            last_prepare_time_taken=60.0,
        )
        runner.fetch_output = MagicMock(return_value=output)
        with pytest.raises(PrepareCommandTimeout, match="60"):
            runner._handle_command_output(0)

    def test_prepare_error_raises(self, runner):
        output = _make_output_json(prepare_return_codes=[1])
        runner.fetch_output = MagicMock(return_value=output)
        with pytest.raises(PrepareCommandError, match="echo prepare"):
            runner._handle_command_output(0)

    def test_prepare_success_continues(self, runner):
        output = _make_output_json(prepare_return_codes=[0])
        runner.fetch_output = MagicMock(return_value=output)
        # Should succeed without raising
        runner._handle_command_output(0)


class TestHandleCommandOutputWorkloadStatus:
    def test_success(self, runner):
        output = _make_output_json(return_code=0)
        runner.fetch_output = MagicMock(return_value=output)
        # Should return without raising
        runner._handle_command_output(0)

    def test_nonzero_raises(self, runner):
        output = _make_output_json(return_code=42)
        runner.fetch_output = MagicMock(return_value=output)
        with pytest.raises(TestCommandError, match="42"):
            runner._handle_command_output(0)

    def test_none_return_code_raises(self, runner):
        output = _make_output_json(return_code=None)
        runner.fetch_output = MagicMock(return_value=output)
        with pytest.raises(TestCommandError, match="None"):
            runner._handle_command_output(0)

    def test_timeout_raises_by_default(self, runner):
        output = _make_output_json(
            return_code=TIMEOUT_RETURN_CODE, workload_time_taken=300.0
        )
        runner.fetch_output = MagicMock(return_value=output)
        with pytest.raises(TestCommandTimeout, match="300"):
            runner._handle_command_output(0)

    def test_timeout_suppressed_when_not_raising(self, runner):
        output = _make_output_json(return_code=TIMEOUT_RETURN_CODE)
        runner.fetch_output = MagicMock(return_value=output)
        # Should return without raising
        runner._handle_command_output(0, raise_on_timeout=False)


class TestHandleCommandOutputSideEffects:
    def test_upload_flags_set_from_output(self, runner):
        output = _make_output_json()
        output["uploaded_results"] = False
        output["uploaded_metrics"] = False
        output["uploaded_artifact"] = False
        runner.fetch_output = MagicMock(return_value=output)
        runner._handle_command_output(0)
        assert runner._results_uploaded is False
        assert runner._metrics_uploaded is False
        assert runner._artifact_uploaded is False
