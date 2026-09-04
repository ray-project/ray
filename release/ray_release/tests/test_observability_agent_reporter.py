import json
import os
import sys
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from ray_release.exception import ExitCode
from ray_release.logger import logger
from ray_release.reporter.observability_agent import (
    COMMAND_FAILURE_RETURN_CODES,
    DEBUG_SESSION_QUERY,
    FEEDBACK_REMINDER,
    ObservabilityAgentReporter,
)
from ray_release.result import Result, ResultStatus
from ray_release.test import Test

DEBUG_SESSION_ID = "oasess_48b26c83496443debae63335d82b3aae"
JOB_ID = "prodjob_bife7nuzw7c7t745pjvbxsj7tt"
SLACK_THREAD = "https://anyscaleteam.slack.com/archives/C0BN5R9M3SR/p1788452541746839"
SUMMARY = "The job ran out of object store memory."

CREATE_RESPONSE = {
    "result": {
        "debug_session_id": DEBUG_SESSION_ID,
        "context": {"kind": "job", "resource_id": JOB_ID},
    }
}
QUERY_RESPONSE = {
    "result": {
        "debug_session_id": DEBUG_SESSION_ID,
        "analysis": {
            "summary": SUMMARY,
            "metrics_findings": [],
            "log_findings": ["Plasma store debug dump: 10.3 GB / 10.3 GB"],
            "issues": [],
            "next_steps": ["Lower the batch size."],
        },
        "metadata": {"slack_thread": SLACK_THREAD},
    }
}


class FakeResponse:
    def __init__(self, json_data: Dict[str, Any], status_code: int = 200):
        self._json_data = json_data
        self.status_code = status_code
        self.ok = status_code < 400
        self.text = json.dumps(json_data)

    def json(self) -> Dict[str, Any]:
        return self._json_data


class FakePost:
    """Records the requests made, and replies with the given responses."""

    def __init__(self, responses: List[FakeResponse]):
        self._responses = responses
        self.requests: List[Dict[str, Any]] = []

    def __call__(
        self,
        url: str,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> FakeResponse:
        self.requests.append(
            {"url": url, "json": json, "headers": headers, "timeout": timeout}
        )
        return self._responses[len(self.requests) - 1]


def _test() -> Test:
    return Test({"name": "test_name"})


def _result(
    status: str,
    job_id: Optional[str] = JOB_ID,
    return_code: int = ExitCode.COMMAND_ERROR.value,
) -> Result:
    result = Result()
    result.status = status
    result.job_id = job_id
    result.return_code = return_code
    return result


def _report(
    result: Result,
    responses: List[FakeResponse],
    skip_command_failures: bool = False,
) -> FakePost:
    fake_post = FakePost(responses)
    with patch.dict(
        os.environ,
        {
            "ANYSCALE_HOST": "https://console.anyscale-staging.com",
            "ANYSCALE_CLI_TOKEN": "test_token",
        },
    ), patch(
        "ray_release.reporter.observability_agent.requests.post", fake_post
    ), patch(
        "ray_release.reporter.observability_agent.SKIP_COMMAND_FAILURES",
        skip_command_failures,
    ):
        ObservabilityAgentReporter().report_result(_test(), result)
    return fake_post


def test_trigger_on_error_statuses():
    for status in (
        ResultStatus.RUNTIME_ERROR.value,
        ResultStatus.ERROR.value,
        ResultStatus.UNKNOWN.value,
    ):
        fake_post = _report(
            _result(status),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        )

        create_request, query_request = fake_post.requests
        assert create_request["url"] == (
            "https://console.anyscale-staging.com"
            f"/api/v2/obs_agent/debug_sessions/job/{JOB_ID}"
        )
        assert create_request["json"] is None
        assert query_request["url"] == (
            "https://console.anyscale-staging.com"
            f"/api/v2/obs_agent/debug_sessions/{DEBUG_SESSION_ID}/messages"
        )
        assert query_request["json"] == {"query": "Why did this job fail?"}
        for request in (create_request, query_request):
            assert request["headers"] == {
                "Authorization": "Bearer test_token",
                "X-Customer-Id": "anyscale-internal",
            }


def test_no_op_on_other_statuses():
    for status in (
        ResultStatus.SUCCESS.value,
        ResultStatus.INFRA_ERROR.value,
        ResultStatus.INFRA_TIMEOUT.value,
        ResultStatus.TRANSIENT_INFRA_ERROR.value,
        ResultStatus.TIMEOUT.value,
    ):
        assert _report(_result(status), []).requests == []


def test_no_op_without_job_id():
    assert _report(_result(ResultStatus.ERROR.value, job_id=None), []).requests == []


def test_log_analysis(caplog):
    with caplog.at_level("INFO", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        )

    assert SUMMARY in caplog.text
    assert SLACK_THREAD in caplog.text
    assert FEEDBACK_REMINDER in caplog.text
    # The findings, issues and next steps are kept out of the logs.
    assert "Plasma store debug dump" not in caplog.text
    assert "Lower the batch size." not in caplog.text


def test_log_analysis_without_slack_thread(caplog):
    query_response = {"result": {"analysis": {"summary": SUMMARY}}}

    with caplog.at_level("INFO", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(query_response)],
        )

    assert SUMMARY in caplog.text
    # The reminder points at the thread, so neither is logged without one.
    assert FEEDBACK_REMINDER not in caplog.text
    assert "Full report and feedback" not in caplog.text
    # Every response is expected to carry a thread, so its absence is an error.
    assert [record.levelname for record in caplog.records if record.levelno >= 40] == [
        "ERROR"
    ]
    assert "carries no slack thread" in caplog.text


def test_error_response_does_not_raise(caplog):
    validation_error = {
        "detail": [
            {"loc": ["path", "job_id"], "msg": "invalid job id", "type": "value_error"}
        ]
    }

    with caplog.at_level("ERROR", logger=logger.name):
        fake_post = _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(validation_error, status_code=422)],
        )

    # The query is not attempted if the debug session could not be created.
    assert len(fake_post.requests) == 1
    assert "Could not obtain an observability agent analysis" in caplog.text
    assert "invalid job id" in caplog.text


def test_missing_debug_session_id_does_not_raise(caplog):
    with caplog.at_level("ERROR", logger=logger.name):
        fake_post = _report(
            _result(ResultStatus.ERROR.value), [FakeResponse({"result": {}})]
        )

    assert len(fake_post.requests) == 1
    assert "Could not obtain an observability agent analysis" in caplog.text


def test_command_failure_return_codes():
    assert COMMAND_FAILURE_RETURN_CODES == (
        ExitCode.COMMAND_ERROR.value,
        ExitCode.COMMAND_ALERT.value,
        ExitCode.COMMAND_TIMEOUT.value,
        ExitCode.PREPARE_ERROR.value,
    )


def test_command_failures_trigger_while_gate_is_off():
    for return_code in COMMAND_FAILURE_RETURN_CODES:
        fake_post = _report(
            _result(ResultStatus.ERROR.value, return_code=return_code),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        )
        assert len(fake_post.requests) == 2


def test_command_failures_skipped_when_gate_is_on():
    for return_code in COMMAND_FAILURE_RETURN_CODES:
        fake_post = _report(
            _result(ResultStatus.ERROR.value, return_code=return_code),
            [],
            skip_command_failures=True,
        )
        assert fake_post.requests == []


def test_other_return_codes_trigger_when_gate_is_on():
    fake_post = _report(
        _result(ResultStatus.UNKNOWN.value, return_code=ExitCode.UNKNOWN.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        skip_command_failures=True,
    )
    assert len(fake_post.requests) == 2


def test_query_is_constant():
    assert DEBUG_SESSION_QUERY == "Why did this job fail?"


@pytest.mark.parametrize(
    "query_response",
    [
        {"result": None},
        {"result": {"analysis": None}},
        {"result": {"analysis": {"summary": SUMMARY}, "metadata": None}},
    ],
    ids=["null_result", "null_analysis", "null_metadata"],
)
def test_null_fields_in_the_query_response_do_not_raise(query_response, caplog):
    """The agent sends explicit nulls, and this parsing is outside the try."""
    with caplog.at_level("INFO", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(query_response)],
        )

    assert "Observability agent analysis" in caplog.text


def test_null_result_in_the_create_response_is_reported_clearly(caplog):
    """A null result must reach the error below it, not an AttributeError."""
    with caplog.at_level("ERROR", logger=logger.name):
        fake_post = _report(
            _result(ResultStatus.ERROR.value), [FakeResponse({"result": None})]
        )

    # The query is not attempted, and the failure names the missing field
    # instead of surfacing an attribute lookup on None.
    assert len(fake_post.requests) == 1
    assert "contains no debug_session_id" in caplog.text
    assert "AttributeError" not in caplog.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
