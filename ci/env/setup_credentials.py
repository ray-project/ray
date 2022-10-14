"""
This script is used to set up credentials for some services in the
CI environment. For instance, it can fetch WandB API tokens and write
the WandB configuration file so test scripts can use the service.
"""
import json
import sys
from pathlib import Path

import boto3

AWS_AIR_SECRETS_ARN = (
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
    "oss-ci/ray-air-test-secrets20221014164754935800000002-UONblX"
)


def get_ray_air_secrets(client):
    raw_string = client.get_secret_value(SecretId=AWS_AIR_SECRETS_ARN)["SecretString"]
    return json.loads(raw_string)


def write_wandb_api_key(api_key: str):
    with open(Path("~/.netrc").expanduser(), "w") as fp:
        fp.write(f"machine api.wandb.ai\n" f"  login user\n" f"  password {api_key}\n")


def write_comet_ml_api_key(api_key: str):
    with open(Path("~/.comet.config").expanduser(), "w") as fp:
        fp.write(f"[comet]\napi_key={api_key}\n")


def write_sigopt_api_key(api_key: str):
    sigopt_config_file = Path("~/.sigopt/client/config.json").expanduser()
    sigopt_config_file.parent.mkdir(parents=True, exist_ok=True)
    with open(sigopt_config_file, "wt") as f:
        json.dump(
            {
                "api_token": api_key,
                "code_tracking_enabled": False,
                "log_collection_enabled": False,
            },
            f,
        )


SERVICES = {
    "wandb": ("wandb_key", write_wandb_api_key),
    "comet_ml": ("comet_ml_token", write_comet_ml_api_key),
    "sigopt": ("sigopt_key", write_sigopt_api_key),
}


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <service1> [service2] ...")
        sys.exit(0)

    services = sys.argv[1:]

    if any(service not in SERVICES for service in services):
        raise RuntimeError(
            f"All services must be included in {list(SERVICES.keys())}. "
            f"Got: {services}"
        )

    try:
        client = boto3.client("secretsmanager", region_name="us-west-2")
        ray_air_secrets = get_ray_air_secrets(client)
    except Exception as e:
        print(f"Could not get Ray AIR secrets: {e}")
        return

    for service in services:
        try:
            secret_key, setup_fn = SERVICES[service]
            setup_fn(ray_air_secrets[secret_key])
        except Exception as e:
            print(f"Could not setup service credentials for {service}: {e}")


if __name__ == "__main__":
    main()
