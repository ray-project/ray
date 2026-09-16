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

# Build-scoped agent meta-data key, one per test, holding the id of the job
# that took responsibility for this build's github comment. A test with
# `repeated_run` gets one job per repeat and a manual retry adds another, all in
# the same build and all in separate processes; meta-data is the only state they
# share. The annotation is deliberately *not* deduped this way -- one agent
# report per test job is the point of it.
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

# The markdown regions github renders verbatim: fenced code blocks and inline
# code spans. It neither parses html nor autolinks inside them, so the agent's
# prose is passed through untouched there -- see _sanitize_summary.
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

        # The analysis is already computed, and writing it is local and free,
        # so it is done before the two remote side effects below: if github
        # hangs and the step is killed, the build still has what it paid for.
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

        Only tests the state machine is tracking have an issue, and the number
        it is tracked by only reaches this reporter because RayTestDBReporter
        refreshes the test from S3 before this one runs. A test with no known
        open issue is skipped, which is also what a failure to reach GitHub
        looks like -- see Test.get_open_github_issue.
        """
        # Checked before the repo handle is built, because building it fetches
        # the github bot token from AWS Secrets Manager. Most failing tests have
        # no tracked issue, and they should not pay for that call -- nor carry
        # its failure modes on the critical path of a failed test.
        # `not`, not `is None`: state_machine.py guards this key the same way,
        # and an empty number would otherwise reach GitHub as a request for
        # /issues/ -- paying for the secret fetch this check exists to avoid.
        issue_number = test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not issue_number:
            logger.info(
                f"Skip commenting the observability agent analysis for test "
                f"{test.get_name()}; no github issue is tracked for it"
            )
            return

        # Checked before the claim below, so that a job with nothing to say does
        # not take the claim away from one that has an analysis.
        if not summary and not slack_thread:
            logger.info(
                f"Skip commenting the observability agent analysis for test "
                f"{test.get_name()}; the agent returned neither a summary nor a "
                f"slack thread, so the comment would carry nothing"
            )
            return

        try:
            ray_repo = TestStateMachine.get_ray_repo()
            # The issue itself rather than a yes/no, so that commenting on it
            # does not fetch it a second time.
            issue = test.get_open_github_issue(ray_repo)
            if issue is None:
                logger.info(
                    f"Skip commenting the observability agent analysis for test "
                    f"{test.get_name()}; no open github issue is known for it. "
                    f"Issue {issue_number} is closed, or github could not be "
                    f"reached -- a warning above says which"
                )
                return

            # Claimed last, immediately before posting: an unreachable github
            # or a closed issue above must not consume this build's one comment.
            if not self._claim_the_builds_comment(test):
                return

            try:
                issue.create_comment(
                    self._issue_comment(test, result, summary, slack_thread)
                )
            except Exception as e:
                if self._is_transient(e):
                    # Hand the claim back so a later job in this build can try.
                    self._release_the_builds_comment(test)
                raise
        except Exception:
            # Commenting is supplementary, and glue.py does not guard the
            # reporting loop, so nothing here may reach the test's result.
            logger.exception(
                f"Could not comment the observability agent analysis on the "
                f"github issue for test {test.get_name()}"
            )
            return

        logger.info(
            f"Commented the observability agent analysis on github issue "
            f"{issue_number} for test {test.get_name()}"
        )

    def _meta_data(self, *args: str) -> Optional[str]:
        """Run `buildkite-agent meta-data`, or None if it could not be run.

        None is also what an unset key gives, since `get` exits non-zero for
        one. Both mean "no claim is recorded", so a broken agent falls open to
        commenting rather than silently dropping the analysis.
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

        Claimed *before* the comment is posted rather than after, so that the
        window this exists to close stays shut.

        The agent has no compare-and-set, so the claim is written and then read
        back. `set` is last-writer-wins, which is what makes that work: when two
        jobs both find the key unset and both write, the one whose write landed
        last reads its own id back and owns the claim, and the other reads a
        stranger's and stands down. Without the read-back both would post.

        It narrows the race rather than closing it: two jobs still both post if
        one of them reads back before the other writes at all. That needs the
        second write to fall in the gap between the first job's write and its
        read-back -- two consecutive local calls -- against repeats that arrive
        here after their own minute-long agent queries.
        """
        if not os.environ.get("BUILDKITE"):
            return True

        # The claim is identified by the job holding it, so a job that cannot
        # name itself cannot run this protocol. Fall open: a duplicate comment
        # is a smaller failure than silently dropping the analysis.
        claimant = os.environ.get("BUILDKITE_JOB_ID")
        if not claimant:
            return True

        key = f"{COMMENT_CLAIM_PREFIX}{test.get_name()}"
        # The value, not `exists`: a released claim leaves the key in place with
        # an empty value, because the agent cannot delete one.
        claimed_by = self._meta_data("get", key)
        if claimed_by:
            logger.info(
                f"Skip commenting the observability agent analysis for test "
                f"{test.get_name()}; job {claimed_by} already commented for "
                f"this build"
            )
            return False

        self._meta_data("set", key, claimant)

        # None means the read-back itself failed, not that somebody else won:
        # the key was just written, so it is set. Stand down only on positively
        # reading another job's id.
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
        """Whether retrying this failure from another job could succeed.

        Releasing the claim on a permanent failure would make every remaining
        job try and fail the same way: a body over github's size cap 422s every
        time, and so does a missing permission.
        """
        from ray_release.github_client import GitHubException

        if isinstance(error, GitHubException):
            return error.status >= 500 or error.status == 429
        return isinstance(error, (requests.Timeout, requests.ConnectionError))

    @staticmethod
    def _sanitize_summary(summary: str) -> str:
        """Make the agent's prose safe to interpolate into a github comment.

        The body renders as markdown and the summary is free-form text from
        the agent, so three things in it are side effects rather than
        intentions: an `@name` notifies a real person, a `#123` posts a
        backlink on that issue, and anything github reads as an html tag --
        `<lambda>` and `<module>` are routine in a traceback -- is dropped by
        its sanitizer before the reader sees it. So escape the markup, as the
        buildkite annotation does, and break the two autolinks with an empty
        html comment. All of it renders as nothing, leaving the text reading
        exactly as the agent wrote it.

        `#` is only broken before a digit, so url fragments survive. Code
        spans and fenced blocks are left alone entirely: github autolinks
        neither and renders neither as html, and rewriting inside one would
        show the escapes and the comment marker literally in a quoted log
        line.
        """

        def defuse(text: str) -> str:
            # Escaping first, so that the markers inserted after it survive.
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
        """Render a link to a url that came out of the agent's response.

        Only a plain http url becomes a link, in the `<...>` destination form:
        anything else could close the link early and leave the rest of itself
        to be read as markdown of its own, in a comment written by the CI bot.
        A url that does not qualify is shown as code, which renders whatever
        it holds and links none of it.
        """
        if ObservabilityAgentReporter._is_plain_url(url):
            return f"[{text}](<{url}>)"
        return f"{text}: `{url.replace('`', '')}`"

    @staticmethod
    def _is_plain_url(url: str) -> bool:
        """Whether a url from the agent's response can be written as markdown.

        Anything else could be read as markdown of its own in a comment written
        by the CI bot, so callers render it as code instead.
        """
        return bool(re.fullmatch(r"https?://[^\s<>()\[\]`]+", url))

    def _issue_comment(
        self,
        test: Test,
        result: Result,
        summary: Optional[str],
        slack_thread: Optional[str],
    ) -> str:
        """The comment body: the summary, and where to go for the rest.

        With no summary the slack thread is the whole of what the agent has to
        say, so it is pasted as the body rather than buried under a line
        announcing that there is nothing to read. _comment_on_github_issue does
        not get this far when there is neither.
        """
        # The failing build named up front rather than below the analysis: it
        # is the first thing a reader of the issue needs in order to go look,
        # and with no summary it would otherwise trail the slack link.
        failure = f"`{test.get_name()}`"
        if result.buildkite_url:
            failure += f" at {result.buildkite_url}"
        lines = [f"The observability agent looked at the latest failure of {failure}."]
        if summary:
            lines += ["", self._sanitize_summary(summary)]
        else:
            lines += [
                "",
                slack_thread
                if self._is_plain_url(slack_thread)
                else f"`{slack_thread.replace('`', '')}`",
            ]
        if summary and slack_thread:
            lines += [
                "",
                f"The full report, with the evidence and next steps behind the "
                f"summary, is in "
                f"{self._markdown_link('this slack thread', slack_thread)}. The "
                "agent is under active development; please rate the report there "
                "with the 'All good' or 'Needs correction' buttons.",
            ]
        return "\n".join(lines)

    def _annotate(
        self,
        test: Test,
        job_id: str,
        debug_session_id: str,
        summary: Optional[str],
        slack_thread: Optional[str],
    ) -> None:
        """Annotate the buildkite job with this attempt's analysis.

        The annotation is scoped to the job, so each attempt of a retried test
        annotates its own job and buildkite shows them together on the build
        page: every attempt's report is kept, attributed to the attempt that
        produced it.

        `--append` is passed for the case of a job annotating twice under this
        context. That does not happen today -- one release test job runs one
        test once -- but replacing would be the wrong behaviour if it ever did.
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
