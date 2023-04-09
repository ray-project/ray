import time
import json
import boto3
from typing import Optional, List
from botocore.config import Config

from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.config import Test
from ray_release.logger import logger


class DBReporter(Reporter):
    def __init__(self):
        self.firehose = boto3.client("firehose", config=Config(region_name="us-west-2"))

    def compute_stack_pattern(self, result: Result) -> Optional[str]:
        stack_trace = self.compute_stack_trace(result)
        return self.compute_unique_pattern(stack_trace)

    def compute_unique_pattern(stack_trace: List(str)) -> Optional[str]:
        return None

    def compute_stack_trace(self, result: Result) -> List(str):
        """
        Extract stack trace pattern from the logs. Stack trace pattern often matches 
        the following:
        ERROR
        Traceback (most recent call last):
            File "...", line ..., in ...
            ...
        Exception: exception error
        """
        error_stacktrace = []
        stacktrace = []
        logs = result.last_logs.split("\n")
        i = 0
        while i < len(logs):
            stack = []
            trace = error_stacktrace
            if 'ERROR' in logs[i]:
                stack.append(logs[i])
                next = i + 1
                if i+1 < len(logs) and logs[i+1].startswith('Traceback'):
                    stack.append(logs[i+1])
                    next = i + 2
            elif logs[i].startswith('Traceback'):
                stack.append(logs[i])
                trace = stacktrace
                next = i + 1
            else:
                i = i + 1
                continue
            while next < len(logs):
                if logs[next].startswith((' ', '\t')):
                    stack.append(logs[next])
                    next = next + 1
            if next < len(logs):
                stack.append(logs[next])
            if stack:
                trace.append(stack)
            i = next + 1

        if not error_stacktrace:
            return error_stacktrace[-1]

        if not stacktrace:
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
