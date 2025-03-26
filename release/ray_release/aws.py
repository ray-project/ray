import io
import os
import time
import sys
import requests
from copy import deepcopy
from typing import Optional
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

import boto3
from botocore.exceptions import ClientError
from ray_release.logger import logger
from ray_release.util import DeferredEnvVar

RELEASE_AWS_BUCKET = DeferredEnvVar(
    "RELEASE_AWS_BUCKET", "ray-release-automation-results"
)
RELEASE_AWS_DB_NAME = DeferredEnvVar("RELEASE_AWS_DB_NAME", "ray_ci")
RELEASE_AWS_DB_TABLE = DeferredEnvVar("RELEASE_AWS_DB_TABLE", "release_test_result")

RELEASE_AWS_ANYSCALE_SECRET_ARN = DeferredEnvVar(
    "RELEASE_AWS_ANYSCALE_SECRET_ARN",
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
    "release-automation/"
    "anyscale-token20210505220406333800000001-BcUuKB",
)

# If changed, update
# test_cluster_manager::MinimalSessionManagerTest.testClusterComputeExtraTags
RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING = [
    "instance",
    "volume",
]

S3_PRESIGNED_CACHE = None
S3_PRESIGNED_KEY = "rayci_result_bucket"


def get_secret_token(secret_id: str) -> str:
    return boto3.client("secretsmanager", region_name="us-west-2").get_secret_value(
        SecretId=secret_id
    )["SecretString"]


def maybe_fetch_api_token():
    from anyscale.authenticate import AuthenticationBlock

    if not os.environ.get("ANYSCALE_CLI_TOKEN"):
        try:
            token, _ = AuthenticationBlock._load_credentials()
            logger.info("Loaded anyscale credentials from local storage.")
            os.environ["ANYSCALE_CLI_TOKEN"] = token
            return
        except Exception:
            pass  # Ignore errors

        logger.info("Missing ANYSCALE_CLI_TOKEN, retrieving from AWS secrets store")
        # NOTE(simon) This should automatically retrieve
        # release-automation@anyscale.com's anyscale token
        cli_token = boto3.client(
            "secretsmanager", region_name="us-west-2"
        ).get_secret_value(SecretId=str(RELEASE_AWS_ANYSCALE_SECRET_ARN))[
            "SecretString"
        ]
        os.environ["ANYSCALE_CLI_TOKEN"] = cli_token


def add_tags_to_aws_config(aws_config: dict, tags_to_add: dict, resource_types: list):
    aws_config = deepcopy(aws_config)
    tag_specifications = aws_config.setdefault("TagSpecifications", [])

    for resource in resource_types:
        # Check if there is already a tag specification for the resource.
        # If so, return first item.
        resource_tags: dict = next(
            (x for x in tag_specifications if x.get("ResourceType", "") == resource),
            None,
        )

        # If no tag specification exists, add
        if resource_tags is None:
            resource_tags = {"ResourceType": resource, "Tags": []}
            tag_specifications.append(resource_tags)

        # Add our tags to the specification
        tags = resource_tags["Tags"]
        for key, value in tags_to_add.items():
            tags.append({"Key": key, "Value": value})

    return aws_config


def upload_to_s3(src_path: str, bucket: str, key_path: str) -> Optional[str]:
    """Upload a file to a S3 bucket

    This assumes the bucket has public read access on the objects uploaded.

    Args:
        src_path: local file path.
        bucket: S3 bucket name.
        key_path: destination url of the uploaded object.

    Return:
        HTTP URL where the uploaded object could be downloaded if successful,
        or None if fails.

    Raises:
        ClientError if upload fails
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(Filename=src_path, Bucket=bucket, Key=key_path)
    except ClientError as e:
        logger.warning(f"Failed to upload to s3:  {e}")
        return None

    return f"https://{bucket}.s3.us-west-2.amazonaws.com/{key_path}"


def _retry(f):
    def inner():
        resp = None
        for _ in range(5):
            resp = f()
            print("Getting Presigned URL, status_code", resp.status_code)
            if resp.status_code >= 500:
                print("errored, retrying...")
                print(resp.text)
                time.sleep(5)
            else:
                return resp
        if resp is None or resp.status_code >= 500:
            print("still errorred after many retries")
            sys.exit(1)

    return inner


@_retry
def _get_s3_rayci_test_data_presigned():
    global S3_PRESIGNED_CACHE
    if not S3_PRESIGNED_CACHE:
        auth = BotoAWSRequestsAuth(
            aws_host="vop4ss7n22.execute-api.us-west-2.amazonaws.com",
            aws_region="us-west-2",
            aws_service="execute-api",
        )
        S3_PRESIGNED_CACHE = requests.get(
            "https://vop4ss7n22.execute-api.us-west-2.amazonaws.com/endpoint/",
            auth=auth,
            params={"job_id": os.environ["BUILDKITE_JOB_ID"]},
        )
    return S3_PRESIGNED_CACHE


def s3_put_rayci_test_data(Bucket: str, Key: str, Body: str):
    try:
        boto3.client("s3").put_object(
            Bucket=Bucket,
            Key=Key,
            Body=Body,
        )
    except ClientError:
        # or use presigned URL
        resp = _get_s3_rayci_test_data_presigned().json()[S3_PRESIGNED_KEY]
        data = resp["fields"]
        data.update(
            {
                "key": Key,
                "file": io.StringIO(Body),
            }
        )
        requests.post(resp["url"], files=data)
