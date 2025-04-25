# NOTE: This needs to get imported first because locust does not work
# well with a lot of libraries including openai, boto3, ray
# ruff: noqa: I001
from benchmark.bm import run_bm

import os
from pathlib import Path  # noqa: E402
from typing import Optional

import click
import pytest
import logging
import anyscale
from benchmark.common import parse_benchmark_args, read_from_s3
from benchmark.firehose_utils import FirehoseRecord, RecordName
from test_utils import (
    start_service,
    get_current_compute_config_name,
    get_applications,
    get_hf_token_env_var,
    setup_client_env_vars,
    get_python_version_from_image,
    append_python_version_from_image,
    get_s3_storage_path,
    namespace_to_command_args,
)


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


CLOUD = "serve_release_tests_cloud"
JOB_NAME = "rayllm_release_test_vllm_perf"
JOB_TIMEOUT_S = 1800

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

    if run_perf_profiler:
        submit_benchmark_vllm_job(image_uri)

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


def submit_benchmark_vllm_job(image_uri: str):
    py_version = get_python_version_from_image(image_uri)
    s3_storage_path = get_s3_storage_path(suffix=py_version)
    cmd_args = namespace_to_command_args(pargs, s3_path=s3_storage_path)

    working_dir = str(Path(__file__).parent)

    job_name = append_python_version_from_image(
        name=JOB_NAME,
        image_name=image_uri,
    )

    job_config = anyscale.job.JobConfig(
        name=job_name,
        entrypoint=f"python benchmark_vllm.py {cmd_args}",
        working_dir=working_dir,
        cloud=CLOUD,
        compute_config=anyscale.compute_config.ComputeConfig(
            head_node=anyscale.anyscale.compute_config.HeadNodeConfig(
                instance_type="g5.12xlarge",  # 4 GPUS,
            ),
            worker_nodes=[],  # To force running on head node only.
        ),
        image_uri=image_uri,
        env_vars={
            "BUILDKITE_BRANCH": os.environ.get("BUILDKITE_BRANCH", ""),
            "BUILDKITE_COMMIT": os.environ.get("BUILDKITE_COMMIT", ""),
        },
        timeout_s=JOB_TIMEOUT_S,
        max_retries=0,
    )

    submitted_job_id = anyscale.job.submit(config=job_config)

    anyscale.job.wait(
        id=submitted_job_id,
        state=anyscale.job.JobState.SUCCEEDED,
        timeout_s=JOB_TIMEOUT_S,
    )

    # Read data from bucket and send to anyscale-dev-product's Firehose
    # This Firehose is where databricks has access to.
    data_for_firehose = read_from_s3(s3_storage_path)

    for result in data_for_firehose:
        record = FirehoseRecord(
            record_name=RecordName.VLLM_PERF_TEST,
            record_metrics=result,
        )
    record.write(verbose=True)


if __name__ == "__main__":
    main()
