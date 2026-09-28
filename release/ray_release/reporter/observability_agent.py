import html
import json
import os
import re
import subprocess
from typing import Any, Dict, Optional

import requests

from ray_release.exception import ExitCode
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result, ResultStatus
from ray_release.test import Test
from ray_release.test_automation.state_machine import TestStateMachine
from ray_release.util import ANYSCALE_HOST, anyscale_job_url, format_link

# Result statuses that trigger the observability agent. These are the failures
# that are attributable to the test workload itself, TIMEOUT included: it is set
# only for ExitCode.COMMAND_TIMEOUT, the test command outrunning its own
# timeout, and what it was doing when the clock ran out is exactly the kind of
# question the agent is there to answer. Infra failures (INFRA_ERROR,
# INFRA_TIMEOUT and TRANSIENT_INFRA_ERROR) are excluded, as the agent has
# nothing to say about a job that never got to run.
OBSERVABILITY_AGENT_TRIGGER_STATUSES = (
    ResultStatus.RUNTIME_ERROR.value,
    ResultStatus.ERROR.value,
    ResultStatus.TIMEOUT.value,
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
# handed to the observability agent. Off: a command failure is still a failure
# somebody has to explain, and the agent sees the job's metrics and logs, which
# the exit code alone does not carry.
#
# Left here as a switch rather than deleted, because that judgement depends on
# traffic nobody has seen yet. If these turn out to be mostly straightforward
# errors whose cause is already plain from the log, turning this on skips them.
# Note what that costs: with the trigger statuses as they are, an ERROR or
# TIMEOUT result always carries one of these return codes, so switching it on
# leaves only RUNTIME_ERROR and UNKNOWN triggering the agent at all.
SKIP_COMMAND_FAILURES = False

# The debug session is always asked the same question; the agent itself decides
# which metrics and logs of the job to look at.
DEBUG_SESSION_QUERY = "Why did this job fail?"

# run_release_test.sh names a file here and prints it under its own buildkite
# group once the test is over. Writing the analysis there instead of logging it
# inline keeps it out of the middle of the reporting output, where it competes
# with the other reporters and the traceback.
ANALYSIS_FILE_ENV = "RELEASE_TEST_OBS_AGENT_FILE"

# An annotation is identified by its context within its scope, and buildkite
# defaults that context to "default". Scope already separates the attempts of a
# retried test -- a retry is a new job, so it annotates separately whatever this
# value is -- so the context is not what keeps their reports apart. It is the
# test name to make the annotation identifiable in the buildkite UI, and to keep
# it clear of anything else annotating the same job.
ANNOTATION_CONTEXT_PREFIX = "obs-agent-"

# info rather than warning or error: the analysis is advisory, and error is what
# the build's own failures use.
ANNOTATION_STYLE = "info"

# `--scope` is what decides where buildkite displays the annotation, and it
# defaults to "build". `--job` only records which job the annotation came from,
# so passing it alone is not enough: without this the annotation lands on the
# build page rather than the job it is about.
ANNOTATION_SCOPE = "job"

# TEMPORARY -- DO NOT MERGE. Stand-ins for what a PR build cannot supply: a real
# anyscale job, a real debug session, and a test with a tracked github issue.
# Set in release/run_release_test.sh.
FAKE_RESPONSE_ENV = "RELEASE_TEST_OBS_AGENT_FAKE_RESPONSE"
FAKE_JOB_ID_ENV = "RELEASE_TEST_OBS_AGENT_FAKE_JOB_ID"
FAKE_ISSUE_ENV = "RELEASE_TEST_OBS_AGENT_FAKE_ISSUE"
FAKE_DEBUG_SESSION_ID = "oasess_fake000000000000000000000000000"

# Github rejects a longer comment body. The summary is the only part this
# reporter does not control the length of, so it is what gets trimmed.
GITHUB_COMMENT_LIMIT = 65536

# Free of markdown, which the cut above it could leave dangling.
TRUNCATION_NOTE = "\n\n... truncated to fit github's comment limit."

# Holds the id of the job that took this build's comment. `repeated_run` and
# manual retries put several jobs in one build, and build meta-data is the only
# state they share. The annotation is deliberately not deduped this way.
COMMENT_CLAIM_PREFIX = "obs-agent-commented-"

# Logged with every analysis. Only the summary is logged; the agent posts the
# full report to a slack thread, which is also where it collects its feedback,
# from the people who know what actually broke.
FEEDBACK_REMINDER = (
    ">>> Only the summary is logged here. The full report, with the evidence and\n"
    ">>> next steps behind it, is in the slack thread below.\n"
    ">>> The observability agent is under active development: please rate that\n"
    ">>> report with the 'All good' or 'Needs correction' buttons in the thread."
)

# Regions github renders verbatim: it neither parses html nor autolinks inside
# them, so _sanitize_summary leaves them alone.
CODE_REGION = re.compile(
    r"^(?:```|~~~).*?(?:^(?:```|~~~)[^\n]*$|\Z)|`+[^`]*`+",
    re.DOTALL | re.MULTILINE,
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
    far as creating an Anyscale job, and, if SKIP_COMMAND_FAILURES is ever
    turned on, for failures raised by the test command itself.
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

        # TEMPORARY -- DO NOT MERGE, see FAKE_RESPONSE_ENV.
        self._apply_fakes(test, result)

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

            # The full analysis also holds the findings, issues and next steps
            # that back the summary; those stay out of the logs to keep them
            # readable.
            logger.debug(f"Observability agent response: {json.dumps(response)}")

            # Parsed in here rather than below, because reading the response is
            # as able to raise as fetching it was: `or {}` covers a null field,
            # but not a response whose shape is nothing like the one documented
            # on _query_debug_session. glue.py does not guard the reporting
            # loop, so anything that escapes changes the result of the test.
            query_result = response.get("result") or {}
            summary = (query_result.get("analysis") or {}).get("summary")
            slack_thread = (query_result.get("metadata") or {}).get("slack_thread")
        except Exception:
            # The analysis is supplementary information; failing to obtain it
            # should never change the outcome of the test run.
            logger.exception(
                f"Could not obtain an observability agent analysis for job {job_id}"
            )
            return

        # Whatever the agent leaves out is called out in the message rather
        # than left blank: the group is the prominent part of the step, so a
        # response that came back empty has to say so there, not only in an
        # error line buried in the reporting output above.
        message = f"Observability agent analysis of job {job_id}:"
        if summary:
            message += f"\n{summary}"
        else:
            logger.error(
                f"Observability agent response for job {job_id} carries no "
                f"summary; debug session {debug_session_id}"
            )
            message += (
                f"\n>>> The agent returned no summary for this job."
                f"\n>>> Debug session: {debug_session_id}"
            )

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
            message += (
                "\n>>> The agent returned no slack thread, so the full report"
                "\n>>> and its feedback buttons cannot be reached from here."
            )

        # Written before the remote calls below, so a hung github cannot cost
        # the build an analysis it already has.
        analysis_file = self._write_analysis(message)
        if analysis_file:
            logger.info(
                f"Observability agent analysis of job {job_id} written to "
                f"{analysis_file}; it is printed at the end of this step"
            )
        else:
            logger.info(message)

        self._annotate(test, job_id, debug_session_id, summary, slack_thread)
        self._comment_on_github_issue(test, result, summary, slack_thread)

    def _comment_on_github_issue(
        self,
        test: Test,
        result: Result,
        summary: Optional[str],
        slack_thread: Optional[str],
    ) -> None:
        """Comment the analysis on the test's github issue, if one is open.

        The issue number only reaches this reporter because RayTestDBReporter
        refreshes the test from S3 first. An unreachable github is
        indistinguishable from no open issue -- see Test.get_open_github_issue.
        """
        # Before the repo handle, which costs an AWS Secrets Manager fetch that
        # most failing tests have no issue to justify. `not`, as
        # state_machine.py guards it: an empty number would reach /issues/.
        issue_number = test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not issue_number:
            logger.info(
                f"Skip commenting the observability agent analysis for test "
                f"{test.get_name()}; no github issue is tracked for it"
            )
            return

        # Before the claim, so a job with nothing to say does not take it.
        if not summary and not slack_thread:
            logger.info(
                f"Skip commenting the observability agent analysis for test "
                f"{test.get_name()}; the agent returned neither a summary nor a "
                f"slack thread, so the comment would carry nothing"
            )
            return

        try:
            ray_repo = TestStateMachine.get_ray_repo()
            # The issue itself, so commenting does not re-fetch it.
            issue = test.get_open_github_issue(ray_repo)
            if issue is None:
                logger.info(
                    f"Skip commenting the observability agent analysis for test "
                    f"{test.get_name()}; no open github issue is known for it. "
                    f"Issue {issue_number} is closed, or github could not be "
                    f"reached -- a warning above says which"
                )
                return

            # Last, so an unreachable github or a closed issue above does not
            # consume this build's one comment.
            if not self._claim_the_builds_comment(test):
                return

            try:
                issue.create_comment(
                    self._issue_comment(test, result, summary, slack_thread)
                )
            except Exception as e:
                if self._is_transient(e):
                    self._release_the_builds_comment(test)
                raise
        except Exception:
            # glue.py does not guard the reporting loop.
            logger.exception(
                f"Could not comment the observability agent analysis on the "
                f"github issue for test {test.get_name()}"
            )
            return

        logger.info(
            f"Commented the observability agent analysis on github issue "
            f"{issue_number} for test {test.get_name()}"
        )

    @staticmethod
    def _apply_fakes(test: Test, result: Result) -> None:
        """TEMPORARY -- DO NOT MERGE. Stand in for the job and the issue."""
        fake_job_id = os.environ.get(FAKE_JOB_ID_ENV)
        if fake_job_id and not result.job_id:
            logger.warning(
                f"DO NOT MERGE: standing in a fake anyscale job id {fake_job_id}"
            )
            result.job_id = fake_job_id

        fake_issue = os.environ.get(FAKE_ISSUE_ENV)
        if fake_issue:
            logger.warning(
                f"DO NOT MERGE: targeting fake github issue {fake_issue} on "
                f"the state machine's repo"
            )
            test[Test.KEY_GITHUB_ISSUE_NUMBER] = fake_issue

    @staticmethod
    def _fake_response() -> Optional[Dict[str, Any]]:
        """TEMPORARY -- DO NOT MERGE. None means call the agent for real."""
        fake_response_file = os.environ.get(FAKE_RESPONSE_ENV)
        if not fake_response_file:
            return None
        logger.warning(
            f"DO NOT MERGE: serving a canned agent response from "
            f"{fake_response_file}; no debug session is created and no slack "
            f"thread is posted"
        )
        with open(fake_response_file, "rt", encoding="utf-8") as fp:
            return json.load(fp)

    def _meta_data(self, *args: str) -> Optional[str]:
        """Run `buildkite-agent meta-data`; None if it failed or the key is unset.

        Both mean no claim is recorded, so a broken agent falls open to
        commenting rather than dropping the analysis.
        """
        try:
            completed = subprocess.run(
                ["buildkite-agent", "meta-data", *args],
                capture_output=True,
                text=True,
            )
        except Exception as e:
            logger.warning(f"Could not run buildkite-agent meta-data: {e}")
            return None
        if completed.returncode != 0:
            return None
        return completed.stdout.strip()

    def _claim_the_builds_comment(self, test: Test) -> bool:
        """Take responsibility for this build's comment, if nobody else has.

        There is no compare-and-set, so the claim is written and read back.
        `set` is last-writer-wins: of two jobs that both found the key unset,
        the later writer reads its own id back and the other stands down.

        Narrowed, not closed -- both still post if one reads back before the
        other writes at all.
        """
        if not os.environ.get("BUILDKITE"):
            return True

        # Two jobs writing the same constant would both read it back and both
        # proceed, so without a job id the protocol cannot run. Fall open.
        claimant = os.environ.get("BUILDKITE_JOB_ID")
        if not claimant:
            return True

        key = f"{COMMENT_CLAIM_PREFIX}{test.get_name()}"
        # The value, not `exists`: a release leaves the key with an empty one,
        # since the agent cannot delete a key.
        claimed_by = self._meta_data("get", key)
        if claimed_by:
            logger.info(
                f"Skip commenting the observability agent analysis for test "
                f"{test.get_name()}; job {claimed_by} already commented for "
                f"this build"
            )
            return False

        self._meta_data("set", key, claimant)

        # None is a failed read-back, not a lost race -- the key was just
        # written. Stand down only on positively reading another job's id.
        winner = self._meta_data("get", key)
        if winner is not None and winner != claimant:
            logger.info(
                f"Skip commenting the observability agent analysis for test "
                f"{test.get_name()}; job {winner} claimed this build's comment "
                f"at the same time and won"
            )
            return False
        return True

    def _release_the_builds_comment(self, test: Test) -> None:
        """Give the claim back, so another job in this build can try."""
        if not os.environ.get("BUILDKITE"):
            return
        self._meta_data("set", f"{COMMENT_CLAIM_PREFIX}{test.get_name()}", "")

    @staticmethod
    def _is_transient(error: Exception) -> bool:
        """Whether retrying from another job could succeed.

        Releasing on a permanent failure would make every remaining job fail
        the same way -- an oversized body 422s every time.
        """
        from ray_release.github_client import GitHubException

        if isinstance(error, GitHubException):
            return error.status >= 500 or error.status == 429
        return isinstance(error, (requests.Timeout, requests.ConnectionError))

    @staticmethod
    def _sanitize_summary(summary: str) -> str:
        """Make the agent's prose safe to interpolate into a github comment.

        In free-form text an `@name` notifies a real person, a `#123` backlinks
        that issue, and anything read as an html tag (`<lambda>` in a traceback)
        is dropped by github's sanitizer. The empty html comment breaks the
        autolinks and renders as nothing.

        `#` only before a digit, so url fragments survive. Code regions are
        skipped -- rewriting there would show the markers literally.
        """

        def defuse(text: str) -> str:
            # Escape first, so the markers inserted after it survive.
            text = html.escape(text, quote=False)
            text = text.replace("@", "@<!---->")
            return re.sub(r"#(?=\d)", "#<!---->", text)

        parts = []
        position = 0
        for code in CODE_REGION.finditer(summary):
            parts.append(defuse(summary[position : code.start()]))
            parts.append(code.group(0))
            position = code.end()
        parts.append(defuse(summary[position:]))
        return "".join(parts)

    @staticmethod
    def _markdown_link(text: str, url: str) -> str:
        """Link a url from the agent's response.

        Only a plain http url is linked; anything else could close the link
        early and have its tail read as markdown, so it is shown as code.
        """
        if ObservabilityAgentReporter._is_plain_url(url):
            return f"[{text}](<{url}>)"
        return f"{text}: `{url.replace('`', '')}`"

    @staticmethod
    def _is_plain_url(url: str) -> bool:
        """Whether a url from the agent's response is safe as a link target."""
        return bool(re.fullmatch(r"https?://[^\s<>()\[\]`]+", url))

    def _issue_comment(
        self,
        test: Test,
        result: Result,
        summary: Optional[str],
        slack_thread: Optional[str],
    ) -> str:
        """The comment body, laid out like the buildkite annotation.

        The test is not named -- the comment is on its own issue. Each part is
        dropped rather than left empty; the one case where that would leave
        nothing worth posting is refused by _comment_on_github_issue.
        """
        lines = []
        if result.buildkite_url:
            lines.append(f"Latest run: {result.buildkite_url}")

        summary_index = None
        if summary:
            if lines:
                lines.append("")
            lines.append("Observability Agent RCA:")
            summary_index = len(lines) + 1
            lines += ["", self._sanitize_summary(summary)]

        if slack_thread:
            lines += [
                "",
                f"{self._markdown_link('Full report and feedback', slack_thread)} "
                "— rate it with the 'All good' or 'Needs correction' buttons in "
                "the thread.",
            ]

        body = "\n".join(lines)
        if len(body) <= GITHUB_COMMENT_LIMIT or summary_index is None:
            return body

        # The rest is this reporter's own text, so what it takes is what the
        # summary must fit inside -- the slack link included.
        overhead = len(body) - len(lines[summary_index])
        lines[summary_index] = self._fit_summary(
            summary, GITHUB_COMMENT_LIMIT - overhead
        )
        return "\n".join(lines)

    @classmethod
    def _fit_summary(cls, summary: str, budget: int) -> str:
        """Sanitize the summary, trimmed to at most `budget` characters.

        Trimmed raw and re-sanitized: sanitizing expands (`<` to four
        characters, `@` to eight), so a raw budget would still overflow and
        cutting sanitized text would cut through a half-written entity.

        Each pass shortens in proportion to the overshoot, and by at least one
        character so the loop ends. Subtracting the overshoot outright loses
        the whole analysis for a summary full of `@` or `<`.
        """
        rendered = cls._sanitize_summary(summary)
        if len(rendered) <= budget:
            return rendered

        room = budget - len(TRUNCATION_NOTE)
        if room <= 0:
            # No room for any of it; the rest of the comment still stands.
            return ""

        cut = room
        while cut > 0:
            rendered = cls._sanitize_summary(summary[:cut])
            if len(rendered) <= room:
                return rendered + TRUNCATION_NOTE
            cut = min(cut - 1, room * cut // len(rendered))
        return TRUNCATION_NOTE.strip()

    def _annotate(
        self,
        test: Test,
        job_id: str,
        debug_session_id: str,
        summary: Optional[str],
        slack_thread: Optional[str],
    ) -> None:
        """Annotate the buildkite job with this attempt's analysis.

        Job-scoped, so each attempt of a retried test annotates its own job and
        buildkite shows them together on the build page. `--append` is inert
        today -- one job runs one test once -- but replacing would be wrong if
        that ever changed.
        """
        if not os.environ.get("BUILDKITE"):
            return

        # Buildkite labels the first try "Retry 1 of N", while
        # BUILDKITE_RETRY_COUNT counts retries *after* it and so is 0 there.
        # Rendering the raw value would put "attempt 3" on the attempt the UI
        # calls "Retry 4 of 5"; render both, so the annotation reconciles with
        # the label it hangs off and with the job log, which prints the raw
        # value.
        retry_count = os.environ.get("BUILDKITE_RETRY_COUNT", "0")
        try:
            attempt = str(int(retry_count) + 1)
        except ValueError:
            attempt = "?"
        # Buildkite renders the body as markup, and everything interpolated
        # below either comes from the agent's response or is a name this
        # reporter does not control, so all of it is escaped. The summary
        # matters most: it is free-form prose, and an unescaped `<` in it would
        # be rendered rather than shown.
        def esc(value: str) -> str:
            return html.escape(str(value), quote=True)

        lines = [
            f"<strong>{esc(test.get_name())}</strong> — attempt {attempt} "
            f"(BUILDKITE_RETRY_COUNT={esc(retry_count)}) — "
            f'<a href="{esc(anyscale_job_url(job_id))}">{esc(job_id)}</a>',
            "",
            esc(summary) if summary else "The agent returned no summary for this job.",
        ]
        if slack_thread:
            lines += [
                "",
                f'<a href="{esc(slack_thread)}">Full report and feedback</a> — rate '
                "it with the 'All good' or 'Needs correction' buttons in the thread.",
            ]
        else:
            lines += ["", f"No slack thread; debug session {esc(debug_session_id)}."]
        lines.append("<br/>")

        command = [
            "buildkite-agent",
            "annotate",
            f"--style={ANNOTATION_STYLE}",
            f"--scope={ANNOTATION_SCOPE}",
            f"--context={ANNOTATION_CONTEXT_PREFIX}{test.get_name()}",
            "--append",
        ]
        if os.environ.get("BUILDKITE_JOB_ID"):
            command += ["--job", os.environ["BUILDKITE_JOB_ID"]]
        # The body is the positional argument, so it stays last.
        command.append("\n".join(lines))

        # Logged so that a build is self-describing about what was actually
        # run: which flags the annotation was created with is otherwise
        # invisible from the job log, and it decides where the annotation lands.
        logger.info(f"Annotating the buildkite job: {' '.join(command[:-1])}")
        try:
            # Not check=True: an annotation is advisory, and a missing binary or
            # a non-zero exit must not change the outcome of the test run.
            completed = subprocess.run(command, capture_output=True, text=True)
        except Exception as e:
            logger.warning(f"Could not annotate the buildkite job: {e}")
            return

        if completed.returncode != 0:
            logger.warning(
                f"buildkite-agent annotate exited {completed.returncode}: "
                f"{completed.stderr.strip()}"
            )

    def _write_analysis(self, message: str) -> Optional[str]:
        """Write the message to the file the test harness prints, if configured.

        Returns the path written to, or None when no file is configured or the
        write failed, in which case the caller logs the message inline instead.
        """
        analysis_file = os.environ.get(ANALYSIS_FILE_ENV)
        if not analysis_file:
            return None

        try:
            # The agent writes prose, so the summary carries non-ascii
            # characters; the encoding cannot be left to the container locale.
            with open(analysis_file, "wt", encoding="utf-8") as fp:
                fp.write(f"{message}\n")
        except Exception as e:
            logger.warning(
                f"Could not write the observability agent analysis to "
                f"{analysis_file}: {e}"
            )
            return None

        return analysis_file

    def _create_debug_session(self, job_id: str) -> str:
        """Create a debug session for the job and return its id."""
        # TEMPORARY -- DO NOT MERGE, see FAKE_RESPONSE_ENV.
        if os.environ.get(FAKE_RESPONSE_ENV):
            return FAKE_DEBUG_SESSION_ID

        response = self._post_json(
            f"debug_sessions/job/{job_id}",
            timeout=CREATE_DEBUG_SESSION_TIMEOUT,
        )
        # `or {}` so that a null result raises the error below rather than an
        # AttributeError.
        debug_session_id = (response.get("result") or {}).get("debug_session_id")
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
        # TEMPORARY -- DO NOT MERGE, see FAKE_RESPONSE_ENV.
        fake_response = self._fake_response()
        if fake_response is not None:
            return fake_response

        return self._post_json(
            f"debug_sessions/{debug_session_id}/messages",
            json_data={"query": DEBUG_SESSION_QUERY},
            timeout=QUERY_DEBUG_SESSION_TIMEOUT,
        )

    def _post_json(
        self,
        path: str,
        timeout: int,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """POST to the observability agent api and return the json object sent
        back.

        Named for what it returns rather than for the verb: the json object is
        the postcondition the two callers are written against, so it is checked
        here rather than left for whichever `.get()` reaches it first.
        """
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
                # The `json=` argument sets Content-Type, which describes the
                # body being sent; Accept is what asks for json back. Without
                # it nothing on the wire says what this expects in return.
                "Accept": "application/json",
            },
            timeout=timeout,
        )
        if not response.ok:
            # A 422 carries the validation error detail, for example when the
            # job id is not one the observability agent knows about.
            raise RuntimeError(
                f"POST {url} returned {response.status_code}: {response.text}"
            )

        # Asking is not receiving: a proxy or a load balancer in front of the
        # api can still answer 200 with an html error page, and .json() raises
        # on that. Nor does valid json promise an object -- `null`, a list and a
        # bare string are all valid at the top level, and .json() returns each
        # of them happily, leaving an AttributeError for whoever calls .get()
        # next. Both are turned into a RuntimeError naming the body, so that the
        # caller's `except` logs what the agent actually sent.
        try:
            body = response.json()
        except ValueError as e:
            raise RuntimeError(
                f"POST {url} returned a body that is not json: {response.text}"
            ) from e

        if not isinstance(body, dict):
            raise RuntimeError(
                f"POST {url} returned json that is not an object: {response.text}"
            )

        return body
