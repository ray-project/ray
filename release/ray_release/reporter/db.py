import time
import re
import json
import boto3
from typing import List
from botocore.config import Config

from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.config import Test
from ray_release.logger import logger

CRASH_PATTERN_MAX_LENGTH = 4000


class DBReporter(Reporter):
    def __init__(self):
        self.firehose = boto3.client("firehose", config=Config(region_name="us-west-2"))

    def compute_crash_pattern(self, logs: str) -> str:
        stack_trace = self._compute_stack_trace(logs.splitlines())
        return self._compute_unique_pattern(stack_trace)[:CRASH_PATTERN_MAX_LENGTH]

    def _compute_unique_pattern(self, stack_trace: List[str]) -> str:
        """
        Compute unique pattern from stack trace, by remove factors such as date, time,
        temp directory, line numbers, etc. This help to aggregate similar logs into
        same bug patterns
        """
        massaged_trace = []
        for line in stack_trace:
            line = re.sub(r"\d", "", line.strip())
            if line == "Traceback (most recent call last):":
                continue
            file_line = re.search(r'File "(.*)", (.*)', line)
            if file_line:
                line = f'{file_line.group(1).split("/")[-1]}{file_line.group(2)}'
            massaged_trace.append(line)
        return "".join(massaged_trace)

    def _compute_stack_trace(self, logs: List[str]) -> List[str]:
        """
        Extract stack trace pattern from the logs. Stack trace pattern often matches
        the following:
        ERROR ...
        Traceback (most recent call last):
            File "...", line ..., in ...
            ...
        Exception: exception error
        """
        error_stacktrace = []
        stacktrace = []
        i = 0
        while i < len(logs):
            stack = []
            trace = error_stacktrace
            if "ERROR" in logs[i]:
                stack.append(logs[i])
                next = i + 1
                if i + 1 < len(logs) and logs[i + 1].startswith("Traceback"):
                    stack.append(logs[i + 1])
                    next = i + 2
            elif logs[i].startswith("Traceback"):
                stack.append(logs[i])
                trace = stacktrace
                next = i + 1
            else:
                i = i + 1
                continue
            while next < len(logs):
                if logs[next].startswith((" ", "\t")):
                    stack.append(logs[next])
                    next = next + 1
                else:
                    break
            if next < len(logs):
                stack.append(logs[next])
            if stack:
                trace.append(stack)
            i = next + 1

        if error_stacktrace:
            return error_stacktrace[-1]

        if stacktrace:
            return stacktrace[-1]

        return []

    def report_result(self, test: Test, result: Result):
        logger.info("Persisting result to the databricks delta lake...")

        # Prometheus metrics are saved as buildkite artifacts
        # and can be obtained using buildkite API.

        result_json = {
            "_table": "release_test_result",
            "report_timestamp_ms": int(time.time() * 1000),
            "status": result.status or "",
            "results": result.results or {},
            "name": test.get("name", ""),
            "group": test.get("group", ""),
            "team": test.get("team", ""),
            "frequency": test.get("frequency", ""),
            "cluster_url": result.cluster_url or "",
            "job_id": result.job_id or "",
            "job_url": result.job_url or "",
            "cluster_id": result.cluster_id or "",
            "wheel_url": result.wheels_url or "",
            "buildkite_url": result.buildkite_url or "",
            "buildkite_job_id": result.buildkite_job_id or "",
            "runtime": result.runtime or -1.0,
            "stable": result.stable,
            "return_code": result.return_code,
            "smoke_test": result.smoke_test,
            "extra_tags": result.extra_tags or {},
            "crash_pattern": self.compute_crash_pattern(result.last_logs or ""),
        }

        logger.debug(f"Result json: {json.dumps(result_json)}")

        try:
            self.firehose.put_record(
                DeliveryStreamName="ray-ci-results",
                Record={"Data": json.dumps(result_json)},
            )
        except Exception:
            logger.exception("Failed to persist result to the databricks delta lake")
        else:
            logger.info("Result has been persisted to the databricks delta lake")
