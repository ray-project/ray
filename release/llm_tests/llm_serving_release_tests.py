from typing import Optional

import click
import pytest

from test_utils import (
    start_service,
    get_current_compute_config_name,
    get_applications,
    get_hf_token_env_var,
    setup_url_base_envs,
)

CLOUD = "serve_release_tests_cloud"
SERVE_CONFIG_FILE = "serve_llama_3dot1_8b_tp1.yaml"


@click.command()
@click.option("--image-uri", type=str, default=None)
@click.option("--serve-config-file", type=str)
def main(
    image_uri: Optional[str],
    serve_config_file: str,
):
    applications = get_applications(serve_config_file)
    compute_config = get_current_compute_config_name()
    env_vars = get_hf_token_env_var()

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
        setup_url_base_envs(query_url=query_url)
        exit_code = pytest.main(
            [
                "./probes",
                "--timeout=30",
                "--durations=10",
                "-s",
                "-vv",
                "-rx",
            ]
        )
        if exit_code != 0:
            raise RuntimeError(f"Tests failed! {exit_code=}")


if __name__ == "__main__":
    main()
