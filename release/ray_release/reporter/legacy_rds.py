import datetime
import json
from typing import Optional

import boto3

from ray_release.aws import (
    RELEASE_AWS_DB_SECRET_ARN,
    RELEASE_AWS_DB_RESOURCE_ARN,
    RELEASE_AWS_DB_NAME,
    RELEASE_AWS_DB_TABLE,
)
from ray_release.config import Test, get_test_env_var
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.util import exponential_backoff_retry

DEFAULT_LEGACY_DB_TABLE = "release_test_result"


class LegacyRDSReporter(Reporter):
    def __init__(
        self, database: Optional[str] = None, database_table: Optional[str] = None
    ):
        self.database = database or RELEASE_AWS_DB_NAME
        self.database_table = database_table or RELEASE_AWS_DB_TABLE

    def report_result(self, test: Test, result: Result):
        logger.info("Persisting results to database...")

        result_dict = {
            "_runtime": result.runtime,
            # Keep session url for legacy support
            "_session_url": result.cluster_url,
            "_cluster_url": result.cluster_url,
            "_commit_url": result.wheels_url,
            "_stable": result.stable,
        }

        now = datetime.datetime.utcnow()
        rds_data_client = boto3.client("rds-data", region_name="us-west-2")

        if "legacy" in test:
            test_name = test["legacy"]["test_name"]
            test_suite = test["legacy"]["test_suite"]
        else:
            test_name = test["name"]
            test_suite = ""

        team = test["team"] or ""

        # Branch name
        category = get_test_env_var("RAY_BRANCH", "")

        status = result.status or "invalid"
        last_logs = result.last_logs or ""

        if result.results:
            result_dict.update(result.results)
        artifacts = {}

        parameters = [
            {
                "name": "created_on",
                "typeHint": "TIMESTAMP",
                "value": {"stringValue": now.strftime("%Y-%m-%d %H:%M:%S")},
            },
            {"name": "test_suite", "value": {"stringValue": test_suite}},
            {"name": "test_name", "value": {"stringValue": test_name}},
            {"name": "status", "value": {"stringValue": status}},
            {"name": "last_logs", "value": {"stringValue": last_logs}},
            {
                "name": "results",
                "typeHint": "JSON",
                "value": {"stringValue": json.dumps(result_dict)},
            },
            {
                "name": "artifacts",
                "typeHint": "JSON",
                "value": {"stringValue": json.dumps(artifacts)},
            },
            {"name": "category", "value": {"stringValue": category}},
            {"name": "team", "value": {"stringValue": team}},
            {"name": "session_url", "value": {"stringValue": result.cluster_url or ""}},
            {"name": "commit_url", "value": {"stringValue": result.wheels_url or ""}},
            {"name": "runtime", "value": {"doubleValue": result.runtime or -1.0}},
            {"name": "stable", "value": {"booleanValue": result.stable}},
            {"name": "frequency", "value": {"stringValue": test.get("frequency", "")}},
            {"name": "return_code", "value": {"longValue": result.return_code}},
        ]

        columns = [param["name"] for param in parameters]
        values = [f":{param['name']}" for param in parameters]
        column_str = ", ".join(columns).strip(", ")
        value_str = ", ".join(values).strip(", ")

        sql = (
            f"INSERT INTO {self.database_table} "
            f"({column_str}) "
            f"VALUES ({value_str})"
        )

        logger.debug(f"SQL query: {sql}")

        # Default boto3 call timeout is 45 seconds.
        retry_delay_s = 64
        MAX_RDS_RETRY = 3
        exponential_backoff_retry(
            lambda: rds_data_client.execute_statement(
                database=self.database,
                parameters=parameters,
                secretArn=RELEASE_AWS_DB_SECRET_ARN,
                resourceArn=RELEASE_AWS_DB_RESOURCE_ARN,
                schema=self.database_table,
                sql=sql,
            ),
            retry_exceptions=rds_data_client.exceptions.StatementTimeoutException,
            initial_retry_delay_s=retry_delay_s,
            max_retries=MAX_RDS_RETRY,
        )
        logger.info("Result has been persisted to the database")
