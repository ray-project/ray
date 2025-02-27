from anyscale_service_utils import start_service
from typing import Any, List, Optional
import click
import os
import anyscale
import yaml
import pytest

CLOUD = "serve_release_tests_cloud"
SERVE_CONFIG_FILE = "standalone_serve_config.yaml"


def get_applications() -> List[Any]:
    with open(SERVE_CONFIG_FILE, "r") as f:
        loaded_llm_config = yaml.safe_load(f)
    return loaded_llm_config["applications"]


def get_current_compute_config_name() -> str:
    """Get the name of the current compute config."""
    cluster_id = os.environ["ANYSCALE_CLUSTER_ID"]
    sdk = anyscale.AnyscaleSDK()
    cluster = sdk.get_cluster(cluster_id)
    return anyscale.compute_config.get(
        name="", _id=cluster.result.cluster_compute_id
    ).name


def setup_envs():
    os.environ["ANYSCALE_JOB_CLUSTER_ENV_NAME"] = "standalone"
    os.environ["OPENAI_API_BASE"] = "standalone"


@click.option("--image-uri", type=str, default=None)
def main(
    image_uri: Optional[str],
):
    applications = get_applications()
    compute_config = get_current_compute_config_name()
    setup_envs()

    with start_service(
        service_name="llm_serving_release_test",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=applications,
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        print(f"{service_name=} started")
        pytest.main(
            [
                "./probes",
                "--timeout=30",
                "--durations=10",
                "-s",
                "-vv",
                "-rx",
                "-k=not test_bad_token_handling and not test_rate_limiting",
            ]
        )


if __name__ == "__main__":
    main()
