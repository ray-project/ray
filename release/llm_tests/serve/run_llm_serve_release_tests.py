# NOTE: This needs to get imported first because locust does not work
# well with a lot of libraries including openai, boto3, ray
# ruff: noqa: I001
from benchmark.bm import run_bm

from typing import Optional

import click
import pytest
import logging
from test_utils import (
    start_service,
    get_current_compute_config_name,
    get_applications,
    get_hf_token_env_var,
    setup_client_env_vars,
)


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


CLOUD = "serve_release_tests_cloud"


@click.command()
@click.option("--image-uri", type=str, default=None)
@click.option("--serve-config-file", type=str)
@click.option("--run-probes", type=bool, default=True)
@click.option("--run-perf-profiler", type=bool, default=True)
@click.option("--skip-hf-token", type=bool, default=False)
@click.option("--timeout", type=int, default=600)
def main(
    image_uri: Optional[str],
    serve_config_file: str,
    run_probes: bool,
    run_perf_profiler: bool,
    skip_hf_token: bool,
    timeout: int,
):
    applications = get_applications(serve_config_file)
    compute_config = get_current_compute_config_name()
    env_vars = get_hf_token_env_var() if not skip_hf_token else {}

    with start_service(
        service_name="llm_serving_release_test",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=applications,
        working_dir=".",
        cloud=CLOUD,
        env_vars=env_vars,
        timeout_s=timeout,
    ) as service_info:
        api_url = service_info["api_url"]
        api_token = service_info["api_token"]
        startup_time = service_info["time_service_startup"]

        logger.info(f"Service started: {api_url=} in {startup_time:.2f} seconds")

        setup_client_env_vars(api_url=api_url, api_token=api_token)

        if run_probes:
            exit_code = pytest.main(
                [
                    "./probes",
                    # Some tests (e.g. test_json_mode) take a long time to run,
                    # so we set a relative long timeout. See
                    # https://github.com/vllm-project/vllm/issues/14151
                    "--timeout=90",
                    "--durations=10",
                    "-s",
                    "-vv",
                    "-rx",
                ]
            )

            if exit_code != 0:
                raise RuntimeError(f"Tests failed! {exit_code=}")

        if run_perf_profiler:
            # For now, the values are hardcoded.
            results = run_bm(
                api_url=api_url,
                api_key=api_token,
                concurrency=[1, 2, 4, 8, 16, 32],
                run_time="1m",
                prompt_tokens=256,
                max_tokens=64,
                stream=False,
                summary_file="./results.csv",
            )

            logger.info(f"Performance test results: {results}")


if __name__ == "__main__":
    main()
