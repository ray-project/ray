import json
import os
import time
from enum import Enum
from typing import Any, Dict

import ray
import boto3
from pydantic import BaseModel, field_validator

STREAM_NAME = "rayllm-ci-results"
DEFAULT_TABLE_NAME = "release_test_result"
# Time to sleep in-between firehose writes to make sure the timestamp between
# records are distinct
SLEEP_BETWEEN_FIREHOSE_WRITES_MS = 50


class RecordName(str, Enum):
    STARTUP_TEST = "service-startup-test"
    STARTUP_TEST_GCP = "service-startup-test-gcp"
    STARTUP_TEST_AWS = "service-startup-test-aws"
    RAYLLM_PERF_TEST = "rayllm-perf-test"
    VLLM_PERF_TEST = "vllm-perf-test"


class FirehoseRecord(BaseModel):
    record_name: RecordName
    record_metrics: Dict[str, Any]

    @field_validator("record_name", mode="before")
    def validate_record_name(cls, v):
        if isinstance(v, str):
            return RecordName(v)
        return v

    def write(self, verbose: bool = False):
        final_result = {
            "_table": DEFAULT_TABLE_NAME,
            "name": str(self.record_name.value),
            "branch": os.environ.get("BUILDKITE_BRANCH", ""),
            "commit": ray.__commit__,
            "report_timestamp_ms": int(time.time() * 1000),
            "results": {**self.record_metrics},
        }

        if verbose:
            print(
                "Writing final result to AWS Firehose:",
                json.dumps(final_result, indent=4, sort_keys=True),
                sep="\n",
            )

        # Add newline character to separate records
        data = json.dumps(final_result) + "\n"

        # Need to assume the role in order to share access to the Firehose
        sts_client = boto3.client("sts")

        assumed_role = sts_client.assume_role(
            RoleArn="arn:aws:iam::830883877497:role/service-role/KinesisFirehoseServiceRole-rayllm-ci-res-us-west-2-1728664186256",
            RoleSessionName="FirehosePutRecordSession",
        )

        credentials = assumed_role["Credentials"]

        # Use the assumed credentials to create a Firehose client
        firehose_client = boto3.client(
            "firehose",
            region_name="us-west-2",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        response = firehose_client.put_record(
            DeliveryStreamName=STREAM_NAME, Record={"Data": data}
        )

        if verbose:
            print("PutRecord response:")
            print(response)

        # Add some delay to make sure timestamps are unique ints.
        time.sleep(SLEEP_BETWEEN_FIREHOSE_WRITES_MS / 1000)
