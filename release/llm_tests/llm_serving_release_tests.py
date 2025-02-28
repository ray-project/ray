import os
from typing import Any, Dict, List, Optional

import boto3
import click
import json
import pytest
import yaml

from anyscale_utils import start_service, get_current_compute_config_name

CLOUD = "serve_release_tests_cloud"
SERVE_CONFIG_FILE = "serve_llama_3dot1_8b_tp1.yaml"
SECRET_NAME = "llm_release_test_hf_token"
REGION_NAME = "us-west-2"


def get_applications() -> List[Any]:
    with open(SERVE_CONFIG_FILE, "r") as f:
        loaded_llm_config = yaml.safe_load(f)
    return loaded_llm_config["applications"]


def setup_envs(query_url: str):
    os.environ["OPENAI_API_BASE"] = query_url


def get_env_vars() -> Dict[str, str]:
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=REGION_NAME)
    secret_string = client.get_secret_value(SecretId=SECRET_NAME)["SecretString"]
    return json.loads(secret_string)


@click.command()
@click.option("--image-uri", type=str, default=None)
def main(
    image_uri: Optional[str],
):
    applications = get_applications()
    compute_config = get_current_compute_config_name()
    env_vars = get_env_vars()

    with start_service(
        service_name="llm_serving_release_test",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=applications,
        working_dir=".",
        cloud=CLOUD,
        env_vars=env_vars,
    ) as query_url:
        print(f"Service started: {query_url=}")
        setup_envs(query_url=query_url)
        pytest.main(
            [
                "./probes",
                "--timeout=30",
                "--durations=10",
                "-s",
                "-vv",
                "-rx",
            ]
        )


if __name__ == "__main__":
    main()
