"""
This script sets up credentials for some services in the
CI environment.
This generates a bash script in the following format, which will
then be sourced to run bazel test with.

export WANDB_API_KEY=abcd
export COMET_API_KEY=efgh
"""
import json
import subprocess
import sys

AWS_AIR_SECRETS_ARN = (
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
    "oss-ci/ray-air-test-secrets20221014164754935800000002-UONblX"
)


def get_ray_air_secrets():
    output = subprocess.check_output(
        [
            "aws",
            "secretsmanager",
            "get-secret-value",
            "--region",
            "us-west-2",
            "--secret-id",
            AWS_AIR_SECRETS_ARN,
        ]
    )

    parsed_output = json.loads(output)
    return json.loads(parsed_output["SecretString"])


SERVICES = {
    "wandb_key": "WANDB_API_KEY",
    "comet_ml_token": "COMET_API_KEY",
    "snowflake_schema": "SNOWFLAKE_SCHEMA",
    "snowflake_database": "SNOWFLAKE_DATABASE",
    "snowflake_user": "SNOWFLAKE_USER",
    "snowflake_account": "SNOWFLAKE_ACCOUNT",
    "snowflake_warehouse": "SNOWFLAKE_WAREHOUSE",
    "snowflake_private_key": "SNOWFLAKE_PRIVATE_KEY",
}


def main():
    try:
        ray_air_secrets = get_ray_air_secrets()
    except Exception as e:
        print(f"Could not get Ray AIR secrets: {e}")
        sys.exit(1)

    for key in SERVICES.keys():
        print(f"export {SERVICES[key]}={ray_air_secrets[key]}")


if __name__ == "__main__":
    main()
