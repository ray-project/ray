import time
import json
import os

import boto3
from botocore.config import Config

from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.test import Test
from ray_release.logger import logger
from ray_release.log_aggregator import LogAggregator


class DBReporter(Reporter):
    def __init__(self):
        self.firehose = boto3.client("firehose", config=Config(region_name="us-west-2"))

    def report_result(self, test: Test, result: Result):
        logger.info("Persisting result to the databricks delta lake...")

        # Prometheus metrics are saved as buildkite artifacts
        # and can be obtained using buildkite API.

        result_json = {
            "_table": "release_test_result",
            "report_timestamp_ms": int(time.time() * 1000),
            "status": result.status or "",
            "branch": os.environ.get("BUILDKITE_BRANCH", ""),
            "commit": os.environ.get("BUILDKITE_COMMIT", ""),
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
            "crash_pattern": LogAggregator(
                result.last_logs or ""
            ).compute_crash_pattern(),
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
