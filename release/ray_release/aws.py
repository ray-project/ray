import os
from copy import deepcopy

import boto3
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
