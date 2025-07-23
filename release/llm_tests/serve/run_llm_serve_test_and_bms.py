# This script is run as an Anyscale job by the release test CI pipeline.
# It starts a Ray Serve LLM service with the specified service config.
# Use CLI flags to run pytest tests from the probes directory or a performance
# benchmark against the service.
#
# It supports launching a vLLM performance benchmark job in parallel,
# using the same service config passed via CLI and identical workload
# to the Ray Serve LLM test. In particular, if the script is run with both
# --run-serve-llm-profiler and --run-vllm-profiler, the service and vLLM job
# will run at the same time.

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

from benchmark.common import read_from_s3, get_llm_config
from benchmark.firehose_utils import FirehoseRecord, RecordName
from test_utils import (
    start_service,
    get_current_compute_config_name,
    get_applications,
    get_hf_token_env_var,
    setup_client_env_vars,
    get_python_version_from_image,
    append_python_version_from_image,
    get_vllm_s3_storage_path,
)


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


CLOUD = "serve_release_tests_cloud"
JOB_NAME = "serve_llm_release_test_vllm_perf"
JOB_TIMEOUT_S = 1800
SERVICE_NAME = "serve_llm_release_test_service"


@click.command()
@click.option(
    "--image-uri",
    type=str,
    default=None,
    help="Optional image URI for service worker. Taken from ANYSCALE_JOB_CLUSTER_ENV_NAME if not present.",
)
@click.option("--serve-config-file", type=str, help="Serve config file for this test")
@click.option(
    "--run-probes",
    type=bool,
    default=True,
    help="Run pytest tests in probes directory against Ray Serve LLM service",
)
@click.option(
    "--run-serve-llm-profiler",
    is_flag=True,
    help="Run locust/gevent performance benchmark against Ray Serve LLM service",
)
@click.option(
    "--skip-hf-token",
    type=bool,
    default=False,
    help="Don't query AWS Secrets Manager for HuggingFace token",
)
@click.option(
    "--timeout", type=int, default=600, help="Ray LLM service timeout parameter."
)
@click.option(
    "--run-vllm-profiler",
    is_flag=True,
    help="Submit Anyscale job to run benchmark_vllm.py and submit results.",
)
def main(
    image_uri: Optional[str],
    serve_config_file: str,
    run_probes: bool,
    run_serve_llm_profiler: bool,
    skip_hf_token: bool,
    timeout: int,
    run_vllm_profiler: bool,
):
    if image_uri is None:
        # We expect this environment variable to be set for all release tests
        cluster_env = os.environ["ANYSCALE_JOB_CLUSTER_ENV_NAME"]
        image_uri = f"anyscale/image/{cluster_env}:1"

    applications = get_applications(serve_config_file)
    compute_config = get_current_compute_config_name()
    env_vars = get_hf_token_env_var() if not skip_hf_token else {}

    if run_vllm_profiler:

        submitted_job_id, s3_storage_path = submit_benchmark_vllm_job(
            image_uri,
            serve_config_file,
            env_vars["HUGGING_FACE_HUB_TOKEN"],
        )

    # Start Ray LLM Service while vLLM job is running
    with start_service(
        service_name=SERVICE_NAME,
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

        if run_serve_llm_profiler:
            llm_config = get_llm_config(serve_config_file)
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

            accelerator = llm_config.get("accelerator_type", "NoGpu")
            tensor_parallel_size = llm_config["engine_kwargs"].get(
                "tensor_parallel_size", 0 if accelerator == "NoGpu" else 1
            )
            tag = f"{accelerator}-TP{tensor_parallel_size}"
            for result in results:
                record = FirehoseRecord(
                    record_name=RecordName.RAYLLM_PERF_TEST,
                    record_metrics={
                        "api_url": api_url,
                        "api_token": api_token,
                        "cloud_name": CLOUD,
                        "service_name": SERVICE_NAME,
                        "py_version": get_python_version_from_image(image_uri),
                        "tag": tag,
                        "vllm_engine": "V1",
                        **result,
                    },
                )
                record.write(verbose=True)

    if run_vllm_profiler:
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


def submit_benchmark_vllm_job(image_uri: str, serve_config_file: str, hf_token: str):
    s3_storage_path = get_vllm_s3_storage_path()

    working_dir = str(Path(__file__).parent)

    job_name = append_python_version_from_image(
        name=JOB_NAME,
        image_name=image_uri,
    )

    job_config = anyscale.job.JobConfig(
        name=job_name,
        entrypoint=f"python benchmark/benchmark_vllm.py --llm-config {serve_config_file} --remote-result-path {s3_storage_path}",
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
            "HF_TOKEN": hf_token,
        },
        max_retries=0,
    )

    submitted_job_id = anyscale.job.submit(config=job_config)
    return submitted_job_id, s3_storage_path


if __name__ == "__main__":
    main()
