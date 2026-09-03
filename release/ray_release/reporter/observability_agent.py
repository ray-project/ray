import json
import os
from typing import Any, Dict, Optional

import requests

from ray_release.exception import ExitCode
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result, ResultStatus
from ray_release.test import Test
from ray_release.util import ANYSCALE_HOST, format_link

# Result statuses that trigger the observability agent. These are the failures
# that are attributable to the test workload itself. Infra failures
# (INFRA_ERROR, INFRA_TIMEOUT and TRANSIENT_INFRA_ERROR) are excluded, as the
# agent has nothing to say about a job that never got to run.
OBSERVABILITY_AGENT_TRIGGER_STATUSES = (
    ResultStatus.RUNTIME_ERROR.value,
    ResultStatus.ERROR.value,
    ResultStatus.UNKNOWN.value,
)

# Return codes of the failures that are raised by the test command itself,
# rather than by the harness around it.
COMMAND_FAILURE_RETURN_CODES = (
    ExitCode.COMMAND_ERROR.value,
    ExitCode.COMMAND_ALERT.value,
    ExitCode.COMMAND_TIMEOUT.value,
    ExitCode.PREPARE_ERROR.value,
)

# Whether the failures in COMMAND_FAILURE_RETURN_CODES are skipped instead of
# handed to the observability agent.
# TODO: off until the reviewers of this change decide whether the agent should
# look at command failures. Note that with the trigger statuses as they are, a
# result with the ERROR status always carries one of those return codes, so
# turning this on leaves only the RUNTIME_ERROR and UNKNOWN statuses triggering.
SKIP_COMMAND_FAILURES = False

# The debug session is always asked the same question; the agent itself decides
# which metrics and logs of the job to look at.
DEBUG_SESSION_QUERY = "Why did this job fail?"

# Logged with every analysis. Only the summary is logged; the agent posts the
# full report to a slack thread, which is also where it collects its feedback,
# from the people who know what actually broke.
FEEDBACK_REMINDER = (
    ">>> Only the summary is logged here. The full report, with the evidence and\n"
    ">>> next steps behind it, is in the slack thread below.\n"
    ">>> The observability agent is under active development: please rate that\n"
    ">>> report with the 'All good' or 'Needs correction' buttons in the thread."
)

# Creating a debug session is a quick bookkeeping call, whereas the query runs
# the actual analysis over the job's metrics and logs.
CREATE_DEBUG_SESSION_TIMEOUT = 60
QUERY_DEBUG_SESSION_TIMEOUT = 900


class ObservabilityAgentReporter(Reporter):
    """
    Reporter that asks the Anyscale observability agent why a release test job
    failed, and logs its analysis.

    It creates a debug session for the Anyscale job of the failed test run, then
    queries that session. This is a no-op for test runs that did not fail with
    one of OBSERVABILITY_AGENT_TRIGGER_STATUSES, for failures that never got as
    far as creating an Anyscale job, and, once SKIP_COMMAND_FAILURES is enabled,
    for failures raised by the test command itself.
    """

    def report_result(self, test: Test, result: Result) -> None:
        if result.status not in OBSERVABILITY_AGENT_TRIGGER_STATUSES:
            logger.info(
                f"Skip triggering the observability agent for test "
                f"{test.get_name()} with result {result.status}"
            )
            return

        if SKIP_COMMAND_FAILURES and (
            result.return_code in COMMAND_FAILURE_RETURN_CODES
        ):
            logger.info(
                f"Skip triggering the observability agent for test "
                f"{test.get_name()} with command failure return code "
                f"{result.return_code}"
            )
            return

        # The job id is the Anyscale production job id, obtained through the
        # Anyscale SDK when the job was submitted; see AnyscaleJobManager.
        job_id = result.job_id
        if not job_id:
            logger.info(
                f"Skip triggering the observability agent for test "
                f"{test.get_name()}; the test run has no Anyscale job id"
            )
            return

        logger.info(
            f"Triggering the observability agent for test {test.get_name()} "
            f"with result {result.status}, job {job_id}"
        )
        try:
            debug_session_id = self._create_debug_session(job_id)
            response = self._query_debug_session(debug_session_id)
        except Exception:
            # The analysis is supplementary information; failing to obtain it
            # should never change the outcome of the test run.
            logger.exception(
                f"Could not obtain an observability agent analysis for job {job_id}"
            )
            return

        # The full analysis also holds the findings, issues and next steps that
        # back the summary; those stay out of the logs to keep them readable.
        logger.debug(f"Observability agent response: {json.dumps(response)}")

        query_result = response.get("result", {})
        summary = query_result.get("analysis", {}).get("summary")
        slack_thread = query_result.get("metadata", {}).get("slack_thread")

        message = f"Observability agent analysis of job {job_id}:\n{summary}"
        if slack_thread:
            message += (
                f"\n{FEEDBACK_REMINDER}"
                f"\n>>> Full report and feedback: {format_link(slack_thread)}"
            )
        else:
            logger.error(
                f"Observability agent response for job {job_id} carries no slack "
                "thread; the full report and its feedback buttons cannot be "
                "linked from here"
            )
        logger.info(message)

    def _create_debug_session(self, job_id: str) -> str:
        """Create a debug session for the job and return its id."""
        response = self._post(
            f"debug_sessions/job/{job_id}",
            timeout=CREATE_DEBUG_SESSION_TIMEOUT,
        )
        debug_session_id = response.get("result", {}).get("debug_session_id")
        if not debug_session_id:
            raise RuntimeError(
                f"Debug session response for job {job_id} contains no "
                f"debug_session_id: {json.dumps(response)}"
            )

        logger.info(f"Created debug session {debug_session_id} for job {job_id}")
        return debug_session_id

    def _query_debug_session(self, debug_session_id: str) -> Dict[str, Any]:
        """Query the debug session and return the full response.

        The response is expected to hold the id of the queried debug session,
        the analysis of the job, and metadata pointing at the slack thread the
        agent posted that analysis to:

            {
                "result": {
                    "debug_session_id": str,
                    "analysis": {
                        "summary": str,
                        "metrics_findings": List[str],
                        "log_findings": List[str],
                        "issues": List[Dict[str, Any]],
                        "next_steps": List[str]
                    },
                    "metadata": {"slack_thread": str}
                }
            }
        """
        return self._post(
            f"debug_sessions/{debug_session_id}/messages",
            json_data={"query": DEBUG_SESSION_QUERY},
            timeout=QUERY_DEBUG_SESSION_TIMEOUT,
        )

    def _post(
        self,
        path: str,
        timeout: int,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        token = os.environ.get("ANYSCALE_CLI_TOKEN")
        if not token:
            raise RuntimeError(
                "ANYSCALE_CLI_TOKEN is not set, cannot call the observability agent"
            )

        url = f"{ANYSCALE_HOST}/api/v2/obs_agent/{path}"
        response = requests.post(
            url,
            json=json_data,
            headers={
                "Authorization": f"Bearer {token}",
                "X-Customer-Id": "anyscale-internal",
            },
            timeout=timeout,
        )
        if not response.ok:
            # A 422 carries the validation error detail, for example when the
            # job id is not one the observability agent knows about.
            raise RuntimeError(
                f"POST {url} returned {response.status_code}: {response.text}"
            )

        return response.json()
