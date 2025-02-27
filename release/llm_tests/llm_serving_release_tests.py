import os
from typing import Any, List, Optional

import click
import pytest
import yaml

from anyscale_utils import start_service, get_current_compute_config_name

CLOUD = "serve_release_tests_cloud"
SERVE_CONFIG_FILE = "standalone_serve_config.yaml"


def get_applications() -> List[Any]:
    with open(SERVE_CONFIG_FILE, "r") as f:
        loaded_llm_config = yaml.safe_load(f)
    return loaded_llm_config["applications"]


def setup_envs(query_url: str):
    os.environ["OPENAI_API_BASE"] = query_url


@click.command()
@click.option("--image-uri", type=str, default=None)
def main(
    image_uri: Optional[str],
):
    applications = get_applications()
    compute_config = get_current_compute_config_name()

    with start_service(
        service_name="llm_serving_release_test",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=applications,
        working_dir=".",
        cloud=CLOUD,
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
