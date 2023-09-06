"""
This script sets up credentials for some services in the
CI environment.
This prints out credentials in the following format, to be ingested
by as bazel test envs.
--test_env=WANDB_API_KEY=abcd --test_env=COMET_API_KEY=efgh
"""
import boto3
import json
import sys

AWS_AIR_SECRETS_ARN = (
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
    "oss-ci/ray-air-test-secrets20221014164754935800000002-UONblX"
)


def get_ray_air_secrets(client):
    raw_string = client.get_secret_value(SecretId=AWS_AIR_SECRETS_ARN)["SecretString"]
    return json.loads(raw_string)


SERVICES = {
    "wandb_key": "WANDB_API_KEY",
    "comet_ml_token": "COMET_API_KEY",
}


def main():

    try:
        client = boto3.client("secretsmanager", region_name="us-west-2")
        ray_air_secrets = get_ray_air_secrets(client)
    except Exception as e:
        print(f"Could not get Ray AIR secrets: {e}")
        sys.exit(1)
        return

    print(
        " ".join(
            [
                f"--test_env={SERVICES[key]}={ray_air_secrets[key]}"
                for key in SERVICES.keys()
            ]
        )
    )


if __name__ == "__main__":
    main()
