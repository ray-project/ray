import os

import boto3
from ray_release.logger import logger
from ray_release.util import DeferredEnvVar

RELEASE_AWS_BUCKET = DeferredEnvVar(
    "RELEASE_AWS_BUCKET", "ray-release-automation-results"
)
RELEASE_AWS_DB_NAME = DeferredEnvVar("RELEASE_AWS_DB_NAME", "ray_ci")
RELEASE_AWS_DB_TABLE = DeferredEnvVar("RELEASE_AWS_DB_TABLE", "release_test_result")

RELEASE_AWS_DB_SECRET_ARN = DeferredEnvVar(
    "RELEASE_AWS_DB_SECRET_ARN",
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
    "rds-db-credentials/cluster-7RB7EYTTBK2EUC3MMTONYRBJLE/ray_ci-MQN2hh",
)
RELEASE_AWS_DB_RESOURCE_ARN = DeferredEnvVar(
    "RELEASE_AWS_DB_RESOURCE_ARN",
    "arn:aws:rds:us-west-2:029272617770:cluster:ci-reporting",
)
RELEASE_AWS_ANYSCALE_SECRET_ARN = DeferredEnvVar(
    "RELEASE_AWS_ANYSCALE_SECRET_ARN",
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
    "release-automation/"
    "anyscale-token20210505220406333800000001-BcUuKB",
)


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
