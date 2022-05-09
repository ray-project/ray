import os
import sys

import boto3

AWS_WANDB_SECRET_ARN = (
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:oss-ci/wandb-key-V8UeE5"
)


def get_and_write_wandb_api_key(client):
    api_key = client.get_secret_value(SecretId=AWS_WANDB_SECRET_ARN)["SecretString"]
    with open(os.path.expanduser("~/.netrc"), "w") as fp:
        fp.write(f"machine api.wandb.ai\n" f"  login user\n" f"  password {api_key}\n")


SERVICES = {"wandb": get_and_write_wandb_api_key}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <service1> [service2] ...")
        sys.exit(0)

    services = sys.argv[1:]

    if any(service not in SERVICES for service in services):
        raise RuntimeError(
            f"All services must be included in {list(SERVICES.keys())}. "
            f"Got: {services}"
        )

    client = boto3.client("secretsmanager", region_name="us-west-2")
    for service in services:
        SERVICES[service](client)
