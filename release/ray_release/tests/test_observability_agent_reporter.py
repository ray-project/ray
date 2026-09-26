import json
import os
import sys
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests

from ray_release.exception import ExitCode
from ray_release.github_client import GitHubException
from ray_release.logger import logger
from ray_release.reporter.observability_agent import (
    ANALYSIS_FILE_ENV,
    ANNOTATION_CONTEXT_PREFIX,
    ANNOTATION_SCOPE,
    COMMAND_FAILURE_RETURN_CODES,
    COMMENT_CLAIM_PREFIX,
    DEBUG_SESSION_QUERY,
    FEEDBACK_REMINDER,
    GITHUB_COMMENT_LIMIT,
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
    def __init__(
        self,
        json_data: Any = None,
        status_code: int = 200,
        text: Optional[str] = None,
    ):
        self._json_data = json_data
        self.status_code = status_code
        self.ok = status_code < 400
        # `text` is what a response carries when it is not json at all; passing
        # it makes json() raise, as requests does for a body it cannot parse.
        self._raises = text is not None
        self.text = text if self._raises else json.dumps(json_data)

    def json(self) -> Any:
        if self._raises:
            raise requests.exceptions.JSONDecodeError("Expecting value", self.text, 0)
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
    analysis_file: Optional[str] = None,
    test: Optional[Test] = None,
    repo: Optional["FakeRepo"] = None,
    get_ray_repo: Optional[MagicMock] = None,
) -> FakePost:
    """Run the reporter against fakes; no call here leaves the process.

    `repo` is the github repo the reporter is handed, and `get_ray_repo` the
    mock that hands it over, for the tests that care whether it was asked for
    at all -- it fetches a secret from AWS on its first call.
    """
    fake_post = FakePost(responses)
    env = {
        "ANYSCALE_HOST": "https://console.anyscale-staging.com",
        "ANYSCALE_CLI_TOKEN": "test_token",
    }
    if analysis_file:
        env[ANALYSIS_FILE_ENV] = analysis_file
    with (
        # clear=True: run_release_test.sh exports RELEASE_TEST_OBS_AGENT_FILE, so
        # an ambient value would otherwise decide whether these tests log or
        # write, and three of them would fail.
        patch.dict(
            os.environ,
            env,
            clear=True,
        ),
        patch("ray_release.reporter.observability_agent.requests.post", fake_post),
        patch(
            "ray_release.reporter.observability_agent.SKIP_COMMAND_FAILURES",
            skip_command_failures,
        ),
        patch(
            "ray_release.reporter.observability_agent.TestStateMachine.get_ray_repo",
            get_ray_repo or MagicMock(return_value=repo),
        ),
    ):
        ObservabilityAgentReporter().report_result(test or _test(), result)
    return fake_post


def test_trigger_on_error_statuses():
    for status in (
        ResultStatus.RUNTIME_ERROR.value,
        ResultStatus.ERROR.value,
        # The test command outran its own timeout; why is a question for the
        # agent, not one the harness can answer from the exit code.
        ResultStatus.TIMEOUT.value,
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
                # What asks for json back; Content-Type only describes the body
                # being sent, and is not set at all on the create call.
                "Accept": "application/json",
            }


def test_no_op_on_other_statuses():
    for status in (
        ResultStatus.SUCCESS.value,
        ResultStatus.INFRA_ERROR.value,
        ResultStatus.INFRA_TIMEOUT.value,
        ResultStatus.TRANSIENT_INFRA_ERROR.value,
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
    """The agent sends explicit nulls, and `or {}` covers each of them."""
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


@pytest.mark.parametrize(
    "body",
    [None, [{"result": {}}], "a string", 42],
    ids=["null", "list", "string", "number"],
)
def test_a_response_that_is_not_an_object_is_reported_clearly(body, caplog):
    """Every one of these is valid json, and none of them has .get()."""
    with caplog.at_level("ERROR", logger=logger.name):
        _report(_result(ResultStatus.ERROR.value), [FakeResponse(body)])

    assert "returned json that is not an object" in caplog.text
    assert "AttributeError" not in caplog.text


def test_a_response_that_is_not_json_is_reported_clearly(caplog):
    """A proxy in front of the api answers 200 with an html error page."""
    with caplog.at_level("ERROR", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(text="<html>502 Bad Gateway</html>")],
        )

    assert "returned a body that is not json" in caplog.text
    assert "502 Bad Gateway" in caplog.text


def test_a_misshapen_query_response_does_not_escape_the_reporter(caplog):
    """The create call succeeds; the query answers something unparseable."""
    with caplog.at_level("ERROR", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse({"result": "not an object"})],
        )

    # Caught and logged, rather than raised out into glue.py's reporting loop.
    assert "Could not obtain an observability agent analysis" in caplog.text


def test_analysis_written_to_file(caplog, tmpdir):
    analysis_file = os.path.join(tmpdir, "analysis.txt")

    with caplog.at_level("INFO", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
            analysis_file=analysis_file,
        )

    with open(analysis_file, "rt", encoding="utf-8") as fp:
        written = fp.read()
    assert SUMMARY in written
    assert SLACK_THREAD in written
    assert FEEDBACK_REMINDER in written

    # The message is handed to the file instead of being logged twice; only a
    # pointer to it stays in the reporting output.
    assert analysis_file in caplog.text
    assert SUMMARY not in caplog.text


def test_analysis_logged_when_no_file_is_configured(caplog):
    with caplog.at_level("INFO", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        )

    assert SUMMARY in caplog.text


def test_analysis_logged_when_the_file_cannot_be_written(caplog, tmpdir):
    # A directory that does not exist, so open() raises.
    analysis_file = os.path.join(tmpdir, "missing", "analysis.txt")

    with caplog.at_level("INFO", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
            analysis_file=analysis_file,
        )

    # The run is not failed by the write error; the analysis falls back to the
    # log so that it is not lost.
    assert "Could not write the observability agent analysis" in caplog.text
    assert SUMMARY in caplog.text


def test_no_file_written_when_the_agent_is_not_triggered(tmpdir):
    analysis_file = os.path.join(tmpdir, "analysis.txt")

    _report(
        _result(ResultStatus.SUCCESS.value),
        [],
        analysis_file=analysis_file,
    )

    assert not os.path.exists(analysis_file)


def test_analysis_file_handles_non_ascii(tmpdir):
    """The agent writes prose; an ascii container locale must not break it."""
    analysis_file = os.path.join(tmpdir, "analysis.txt")
    summary = "No metric spikes \u2014 the runtime_env setup failed."
    query_response = {
        "result": {
            "analysis": {"summary": summary},
            "metadata": {"slack_thread": SLACK_THREAD},
        }
    }

    _report(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(query_response)],
        analysis_file=analysis_file,
    )

    with open(analysis_file, "rt", encoding="utf-8") as fp:
        assert summary in fp.read()


def test_write_failures_never_propagate(caplog, tmpdir):
    """A reporter must not fail the test run, whatever open() raises."""
    analysis_file = os.path.join(tmpdir, "analysis.txt")

    with (
        caplog.at_level("INFO", logger=logger.name),
        # The module's own open, not every open in the process: a patch on
        # builtins would also be satisfied by an incidental open somewhere
        # else, and would survive _write_analysis no longer opening anything.
        patch(
            "ray_release.reporter.observability_agent.open",
            side_effect=UnicodeEncodeError("ascii", "x", 0, 1, "boom"),
            create=True,
        ),
    ):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
            analysis_file=analysis_file,
        )

    assert "Could not write the observability agent analysis" in caplog.text
    assert SUMMARY in caplog.text


def test_missing_summary_is_named_in_the_analysis(caplog, tmpdir):
    """A response with no summary must say so where the group is read."""
    analysis_file = os.path.join(tmpdir, "analysis.txt")
    query_response = {
        "result": {
            "analysis": {},
            "metadata": {"slack_thread": SLACK_THREAD},
        }
    }

    with caplog.at_level("ERROR", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(query_response)],
            analysis_file=analysis_file,
        )

    with open(analysis_file, "rt", encoding="utf-8") as fp:
        written = fp.read()
    assert "returned no summary" in written
    assert DEBUG_SESSION_ID in written
    # The thread is still reachable, so the full report is not lost with it.
    assert SLACK_THREAD in written
    assert "None" not in written
    assert "carries no summary" in caplog.text


def test_empty_analysis_still_reports_the_failure(caplog, tmpdir):
    """Neither field came back: the group must still say what happened."""
    analysis_file = os.path.join(tmpdir, "analysis.txt")

    with caplog.at_level("ERROR", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse({"result": {}})],
            analysis_file=analysis_file,
        )

    with open(analysis_file, "rt", encoding="utf-8") as fp:
        written = fp.read()
    # Non-empty, so run_release_test.sh still prints the group rather than
    # leaving the step looking as though the agent never ran.
    assert written.strip()
    assert "returned no summary" in written
    assert "no slack thread" in written
    assert DEBUG_SESSION_ID in written
    assert "None" not in written


class FakeCompleted:
    def __init__(self, returncode: int = 0, stderr: str = "", stdout: str = ""):
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout


def _report_annotating(result, responses, env=None):
    """Run the reporter as if on a buildkite agent, capturing the annotate call."""
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return FakeCompleted()

    fake_post = FakePost(responses)
    full_env = {
        "ANYSCALE_HOST": "https://console.anyscale-staging.com",
        "ANYSCALE_CLI_TOKEN": "test_token",
        "BUILDKITE": "true",
        "BUILDKITE_JOB_ID": "01a0691c-job",
        "BUILDKITE_RETRY_COUNT": "2",
        **(env or {}),
    }
    with (
        patch.dict(os.environ, full_env, clear=True),
        patch("ray_release.reporter.observability_agent.requests.post", fake_post),
        patch("ray_release.reporter.observability_agent.subprocess.run", fake_run),
    ):
        ObservabilityAgentReporter().report_result(_test(), result)
    return calls


def test_the_annotation_is_job_scoped_and_keyed_on_the_test():
    calls = _report_annotating(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
    )

    assert len(calls) == 1
    command = calls[0]
    assert command[:2] == ["buildkite-agent", "annotate"]
    assert "--append" in command
    assert f"--context={ANNOTATION_CONTEXT_PREFIX}test_name" in command
    assert "--style=info" in command
    # --scope is what decides where buildkite shows the annotation; without it
    # the default is "build", which would put it on the build page instead.
    assert f"--scope={ANNOTATION_SCOPE}" in command
    assert ANNOTATION_SCOPE == "job"
    # --scope is what puts the annotation on the job; --job names which job it
    # came from. A retry is a new job, so it annotates separately either way.
    assert command[command.index("--job") + 1] == "01a0691c-job"

    body = command[-1]
    assert SUMMARY in body
    assert SLACK_THREAD in body
    # The environment says 2 retries, which buildkite labels "Retry 3 of N";
    # both numbers appear so the annotation reconciles with the UI and the log.
    assert "attempt 3 (BUILDKITE_RETRY_COUNT=2)" in body


def test_no_annotation_outside_buildkite():
    calls = _report_annotating(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        env={"BUILDKITE": ""},
    )

    assert calls == []


def test_annotation_failures_never_propagate(caplog):
    """An annotation is advisory; it must not change the test's outcome."""
    fake_post = FakePost([FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)])
    with (
        caplog.at_level("WARNING", logger=logger.name),
        patch.dict(
            os.environ,
            {
                "ANYSCALE_HOST": "https://console.anyscale-staging.com",
                "ANYSCALE_CLI_TOKEN": "test_token",
                "BUILDKITE": "true",
            },
            clear=True,
        ),
        patch("ray_release.reporter.observability_agent.requests.post", fake_post),
        patch(
            "ray_release.reporter.observability_agent.subprocess.run",
            side_effect=FileNotFoundError("buildkite-agent"),
        ),
    ):
        ObservabilityAgentReporter().report_result(
            _test(), _result(ResultStatus.ERROR.value)
        )

    assert "Could not annotate the buildkite job" in caplog.text


def test_a_non_zero_annotate_exit_is_logged_not_raised(caplog):
    def fake_run(command, **kwargs):
        return FakeCompleted(returncode=1, stderr="boom")

    fake_post = FakePost([FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)])
    with (
        caplog.at_level("WARNING", logger=logger.name),
        patch.dict(
            os.environ,
            {
                "ANYSCALE_HOST": "https://console.anyscale-staging.com",
                "ANYSCALE_CLI_TOKEN": "test_token",
                "BUILDKITE": "true",
            },
            clear=True,
        ),
        patch("ray_release.reporter.observability_agent.requests.post", fake_post),
        patch("ray_release.reporter.observability_agent.subprocess.run", fake_run),
    ):
        ObservabilityAgentReporter().report_result(
            _test(), _result(ResultStatus.ERROR.value)
        )

    assert "buildkite-agent annotate exited 1" in caplog.text


def test_the_attempt_number_matches_the_buildkite_label():
    """BUILDKITE_RETRY_COUNT is 0 on the first try, which buildkite calls 1."""
    calls = _report_annotating(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        env={"BUILDKITE_RETRY_COUNT": "0"},
    )

    assert "attempt 1 (BUILDKITE_RETRY_COUNT=0)" in calls[0][-1]


def test_the_annotation_escapes_what_the_agent_sent():
    """The summary and the thread url are the agent's data, not ours."""
    query_response = {
        "result": {
            "analysis": {"summary": 'a <script>alert("x")</script> summary'},
            "metadata": {"slack_thread": 'https://x" onmouseover="alert(1)'},
        }
    }
    calls = _report_annotating(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(query_response)],
    )

    body = calls[0][-1]
    # Nothing the agent sent can open a tag or close an attribute.
    assert "<script>" not in body
    assert "&lt;script&gt;" in body
    assert 'href="https://x&quot; onmouseover=&quot;alert(1)"' in body
    # Our own markup is still markup.
    assert "<strong>" in body and "<br/>" in body


class FakeIssue:
    def __init__(self, state: str = "open"):
        self.state = state
        self.comments = []

    def create_comment(self, body: str) -> None:
        self.comments.append(body)


class FakeRepo:
    def __init__(self, issue=None, raises=None):
        self._issue = issue
        self._raises = raises
        self.get_issue_calls = []

    def get_issue(self, number):
        self.get_issue_calls.append(number)
        if self._raises:
            raise self._raises
        return self._issue


def _test_with_issue(issue_number: Optional[str] = "123") -> Test:
    test = Test({"name": "test_name"})
    if issue_number is not None:
        test[Test.KEY_GITHUB_ISSUE_NUMBER] = issue_number
    return test


def _comment_on(repo: FakeRepo, summary: str = SUMMARY, **kwargs) -> str:
    """Report a failure of a test with an open issue; return the comment body."""
    query_response = {
        "result": {
            "analysis": {"summary": summary},
            "metadata": {"slack_thread": SLACK_THREAD},
        }
    }
    _report(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(query_response)],
        test=_test_with_issue(),
        repo=repo,
        **kwargs,
    )
    return repo._issue.comments[0]


def test_comments_on_an_open_issue():
    issue = FakeIssue(state="open")
    repo = FakeRepo(issue=issue)
    result = _result(ResultStatus.ERROR.value)
    result.buildkite_url = "https://buildkite.com/ray-project/release/builds/1"

    _report(
        result,
        [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        test=_test_with_issue(),
        repo=repo,
    )

    assert len(issue.comments) == 1
    # One fetch, not two: the issue the open-check returned is the one
    # commented on, so nothing re-reads it between the check and the write.
    assert repo.get_issue_calls == ["123"]
    body = issue.comments[0]
    assert SUMMARY in body
    assert SLACK_THREAD in body
    # Laid out like the annotation: the run first, then the analysis, then
    # where to send feedback. The test is not named -- this is its own issue.
    assert body.splitlines()[0] == f"Latest run: {result.buildkite_url}"
    assert "Observability Agent RCA:" in body


def test_does_not_comment_on_a_closed_issue():
    issue = FakeIssue(state="closed")
    repo = FakeRepo(issue=issue)

    _report(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        test=_test_with_issue(),
        repo=repo,
    )

    assert issue.comments == []
    # The closed issue is still fetched exactly once to find that out.
    assert repo.get_issue_calls == ["123"]


@pytest.mark.parametrize("issue_number", [None, "", 0])
def test_does_not_reach_github_without_a_tracked_issue(issue_number):
    """Building the repo handle costs an AWS secret fetch; skip it entirely."""
    get_ray_repo = MagicMock()

    _report(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
        test=_test_with_issue(issue_number),
        get_ray_repo=get_ray_repo,
    )

    get_ray_repo.assert_not_called()


def test_a_github_failure_does_not_propagate(caplog):
    repo = FakeRepo(raises=RuntimeError("github is down"))

    with caplog.at_level("ERROR", logger=logger.name):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
            test=_test_with_issue(),
            repo=repo,
        )

    assert "Could not comment the observability agent analysis" in caplog.text


def test_the_comment_cannot_mention_people_or_link_issues():
    """The summary is the agent's prose; a mention would notify a real person."""
    issue = FakeIssue(state="open")
    repo = FakeRepo(issue=issue)
    result = _result(ResultStatus.ERROR.value)
    # A url fragment of our own, to show only the agent's prose is touched.
    result.buildkite_url = "https://buildkite.com/ray-project/release/builds/1#job"

    _report(
        result,
        [
            FakeResponse(CREATE_RESPONSE),
            FakeResponse(
                {
                    "result": {
                        "analysis": {"summary": "see #1234, raised by @someone"},
                        "metadata": {"slack_thread": SLACK_THREAD},
                    }
                }
            ),
        ],
        test=_test_with_issue(),
        repo=repo,
    )

    body = issue.comments[0]
    assert "@<!---->someone" in body
    assert "#<!---->1234" in body
    # Nothing github would autolink survives.
    assert "@someone" not in body
    assert "#1234" not in body
    # Our own text is untouched: the buildkite url keeps its fragment.
    assert "builds/1#job" in body


def test_the_comment_escapes_what_the_agent_sent():
    """Unescaped, github's sanitizer drops <lambda> and the reader never sees it."""
    issue = FakeIssue(state="open")

    body = _comment_on(
        FakeRepo(issue=issue),
        summary="failed inside <lambda> in <module> & exited",
    )

    assert "&lt;lambda&gt; in &lt;module&gt; &amp; exited" in body
    assert "<lambda>" not in body


def test_the_comment_leaves_the_agents_code_spans_alone():
    """Github renders code verbatim, so rewriting inside one corrupts it."""
    issue = FakeIssue(state="open")

    body = _comment_on(
        FakeRepo(issue=issue),
        summary="the actor `TrainWorker@10.0.1.5` died:\n```\nraise <lambda> #1\n```",
    )

    assert "`TrainWorker@10.0.1.5`" in body
    assert "raise <lambda> #1" in body


def test_the_slack_link_target_cannot_escape_the_link():
    """The thread url is another value the agent's response decides."""
    issue = FakeIssue(state="open")
    repo = FakeRepo(issue=issue)

    _report(
        _result(ResultStatus.ERROR.value),
        [
            FakeResponse(CREATE_RESPONSE),
            FakeResponse(
                {
                    "result": {
                        "analysis": {"summary": SUMMARY},
                        "metadata": {"slack_thread": "https://slack/p1) [x](evil)"},
                    }
                }
            ),
        ],
        test=_test_with_issue(),
        repo=repo,
    )

    body = issue.comments[0]
    # A url that cannot be a link destination is shown as code, not linked.
    assert "[Full report and feedback](" not in body
    assert "Full report and feedback: `https://slack/p1) [x](evil)`" in body


def test_the_slack_link_uses_a_bounded_destination():
    issue = FakeIssue(state="open")

    body = _comment_on(FakeRepo(issue=issue))

    assert f"[Full report and feedback](<{SLACK_THREAD}>)" in body


class FakeAgent:
    """`buildkite-agent`, backed by an in-memory build meta-data store."""

    def __init__(self, meta_data=None):
        self.meta_data = dict(meta_data or {})
        self.commands = []

    def run(self, command, **kwargs):
        self.commands.append(command)
        if command[:2] != ["buildkite-agent", "meta-data"]:
            return FakeCompleted()  # the annotate call
        operation, key = command[2], command[3]
        if operation == "get":
            # `get` exits non-zero for a key that was never set.
            if key not in self.meta_data:
                return FakeCompleted(returncode=1)
            return FakeCompleted(stdout=f"{self.meta_data[key]}\n")
        if operation == "set":
            self.meta_data[key] = command[4]
            return FakeCompleted()
        return FakeCompleted(returncode=1)


CLAIM_KEY = f"{COMMENT_CLAIM_PREFIX}test_name"


def _report_on_buildkite(
    repo, agent, job_id="01a0691c-job", summary=SUMMARY, result=None
):
    """Run the reporter as one job of a build, against a shared fake agent."""
    query_response = {
        "result": {
            "analysis": {"summary": summary} if summary else {},
            "metadata": {"slack_thread": SLACK_THREAD},
        }
    }
    with (
        patch.dict(
            os.environ,
            {
                "ANYSCALE_HOST": "https://console.anyscale-staging.com",
                "ANYSCALE_CLI_TOKEN": "test_token",
                "BUILDKITE": "true",
                "BUILDKITE_JOB_ID": job_id,
            },
            clear=True,
        ),
        patch(
            "ray_release.reporter.observability_agent.requests.post",
            FakePost([FakeResponse(CREATE_RESPONSE), FakeResponse(query_response)]),
        ),
        patch("ray_release.reporter.observability_agent.subprocess.run", agent.run),
        patch(
            "ray_release.reporter.observability_agent.TestStateMachine.get_ray_repo",
            MagicMock(return_value=repo),
        ),
    ):
        ObservabilityAgentReporter().report_result(
            _test_with_issue(), result or _result(ResultStatus.ERROR.value)
        )


def test_only_one_job_per_build_comments():
    """Five repeated_run jobs share a build; the issue gets one comment."""
    issue = FakeIssue(state="open")
    agent = FakeAgent()

    for job in ("job-1", "job-2", "job-3", "job-4", "job-5"):
        _report_on_buildkite(FakeRepo(issue=issue), agent, job_id=job)

    assert len(issue.comments) == 1
    assert agent.meta_data[CLAIM_KEY] == "job-1"


def test_the_claim_is_taken_before_the_comment_is_posted():
    """Claiming afterwards would leave the window this exists to close open."""
    issue = FakeIssue(state="open")
    issue.create_comment = MagicMock(side_effect=GitHubException(500, "boom"))
    agent = FakeAgent()

    _report_on_buildkite(FakeRepo(issue=issue), agent)

    # The key exists, so the claim was written before the post was attempted.
    assert CLAIM_KEY in agent.meta_data


def test_a_transient_failure_hands_the_claim_back():
    issue = FakeIssue(state="open")
    issue.create_comment = MagicMock(side_effect=GitHubException(503, "unavailable"))
    agent = FakeAgent()

    _report_on_buildkite(FakeRepo(issue=issue), agent, job_id="job-1")

    # Released, not deleted: the agent cannot delete a key.
    assert agent.meta_data[CLAIM_KEY] == ""

    # So the next job in the build takes it and comments.
    working = FakeIssue(state="open")
    _report_on_buildkite(FakeRepo(issue=working), agent, job_id="job-2")
    assert len(working.comments) == 1
    assert agent.meta_data[CLAIM_KEY] == "job-2"


@pytest.mark.parametrize(
    "status", [422, 403, 404], ids=["too_long", "forbidden", "gone"]
)
def test_a_permanent_failure_keeps_the_claim(status):
    """Releasing here would make every remaining job fail the same way."""
    issue = FakeIssue(state="open")
    issue.create_comment = MagicMock(side_effect=GitHubException(status, "nope"))
    agent = FakeAgent()

    _report_on_buildkite(FakeRepo(issue=issue), agent, job_id="job-1")

    assert agent.meta_data[CLAIM_KEY] == "job-1"

    second = FakeIssue(state="open")
    _report_on_buildkite(FakeRepo(issue=second), agent, job_id="job-2")
    assert second.comments == []


class RacingAgent(FakeAgent):
    """A FakeAgent where another job's `set` lands right after ours.

    Models the interleaving the read-back exists for: both jobs found the key
    unset, both wrote, and the other job's write was the one that stuck.
    """

    def __init__(self, other_job: str):
        super().__init__()
        self.other_job = other_job
        self.raced = False

    def run(self, command, **kwargs):
        completed = super().run(command, **kwargs)
        if command[:3] == ["buildkite-agent", "meta-data", "set"] and not self.raced:
            self.raced = True
            self.meta_data[command[3]] = self.other_job
        return completed


def test_the_loser_of_a_simultaneous_claim_does_not_comment():
    """Both jobs found the key unset; last write wins, the other stands down."""
    issue = FakeIssue(state="open")
    agent = RacingAgent(other_job="job-2")

    _report_on_buildkite(FakeRepo(issue=issue), agent, job_id="job-1")

    assert issue.comments == []
    assert agent.meta_data[CLAIM_KEY] == "job-2"


def test_an_unverifiable_claim_still_comments():
    """A failed read-back is not evidence that somebody else won."""
    issue = FakeIssue(state="open")

    class BlindAgent(FakeAgent):
        def run(self, command, **kwargs):
            completed = super().run(command, **kwargs)
            if command[:3] == ["buildkite-agent", "meta-data", "get"]:
                # Every get fails, so the read-back cannot confirm the claim.
                return FakeCompleted(returncode=1)
            return completed

    _report_on_buildkite(FakeRepo(issue=issue), BlindAgent(), job_id="job-1")

    assert len(issue.comments) == 1


def test_no_claim_without_a_job_id_to_claim_with():
    """Nothing to identify the holder by, so the protocol cannot be run."""
    issue = FakeIssue(state="open")
    agent = FakeAgent()

    _report_on_buildkite(FakeRepo(issue=issue), agent, job_id="")

    assert len(issue.comments) == 1
    assert CLAIM_KEY not in agent.meta_data


@pytest.mark.parametrize(
    "summary",
    [
        "x" * 200_000,
        "@" * 200_000,  # sanitizing expands each of these eightfold
        "<" * 200_000,  # ... and each of these fourfold
        "The actor <Worker> owned by @team died. " * 5_000,
        "y" * GITHUB_COMMENT_LIMIT,
    ],
    ids=["plain", "all_mentions", "all_tags", "mixed_prose", "exactly_at_limit"],
)
def test_an_oversized_summary_is_trimmed_to_fit(summary):
    """Github rejects a body over the limit, and the 422 loses the comment."""
    issue = FakeIssue(state="open")
    result = _result(ResultStatus.ERROR.value)
    result.buildkite_url = "https://buildkite.com/ray-project/release/builds/1"

    _report_on_buildkite(
        FakeRepo(issue=issue), FakeAgent(), summary=summary, result=result
    )

    body = issue.comments[0]
    assert len(body) <= GITHUB_COMMENT_LIMIT
    # The parts worth keeping survive the trim: where to look, and the thread
    # holding the analysis that was cut.
    assert result.buildkite_url in body.splitlines()[0]
    assert SLACK_THREAD in body
    assert "truncated" in body
    # And the trim spends the budget rather than discarding the analysis.
    assert len(body) > GITHUB_COMMENT_LIMIT * 0.9


def test_a_summary_that_fits_is_not_trimmed():
    issue = FakeIssue(state="open")

    _report_on_buildkite(FakeRepo(issue=issue), FakeAgent())

    assert "truncated" not in issue.comments[0]


def test_the_annotation_is_not_deduped_by_build():
    """One agent report per test job is the point of the annotation."""
    agent = FakeAgent()

    for job in ("job-1", "job-2", "job-3"):
        _report_on_buildkite(FakeRepo(issue=FakeIssue()), agent, job_id=job)

    annotates = [c for c in agent.commands if c[:2] == ["buildkite-agent", "annotate"]]
    assert len(annotates) == 3


def test_no_claim_is_taken_outside_buildkite():
    repo = FakeRepo(issue=FakeIssue(state="open"))
    agent = FakeAgent()
    with patch("ray_release.reporter.observability_agent.subprocess.run", agent.run):
        _report(
            _result(ResultStatus.ERROR.value),
            [FakeResponse(CREATE_RESPONSE), FakeResponse(QUERY_RESPONSE)],
            test=_test_with_issue(),
            repo=repo,
        )

    assert agent.commands == []


def test_the_comment_points_at_the_thread_when_there_is_no_summary():
    """The thread is the whole of what the agent has to say."""
    issue = FakeIssue(state="open")

    _report_on_buildkite(FakeRepo(issue=issue), FakeAgent(), summary=None)

    body = issue.comments[0]
    assert SLACK_THREAD in body
    assert "returned no summary" not in body


def test_the_comment_names_the_build_even_with_nothing_else_to_say():
    """With no summary, the run and the thread are still both reachable."""
    issue = FakeIssue(state="open")
    result = _result(ResultStatus.ERROR.value)
    result.buildkite_url = "https://buildkite.com/ray-project/release/builds/1"

    _report_on_buildkite(
        FakeRepo(issue=issue), FakeAgent(), summary=None, result=result
    )

    lines = issue.comments[0].splitlines()
    assert lines[0] == f"Latest run: {result.buildkite_url}"
    assert SLACK_THREAD in lines[-1]
    # No heading left standing over an analysis that never arrived.
    assert "Observability Agent RCA:" not in issue.comments[0]


def test_no_comment_when_the_agent_returned_nothing():
    """Neither a summary nor a thread means the comment would carry nothing."""
    issue = FakeIssue(state="open")
    repo = FakeRepo(issue=issue)

    _report(
        _result(ResultStatus.ERROR.value),
        [FakeResponse(CREATE_RESPONSE), FakeResponse({"result": {}})],
        test=_test_with_issue(),
        repo=repo,
    )

    assert issue.comments == []
    # Not even fetched: there was nothing to post before github was reached.
    assert repo.get_issue_calls == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
