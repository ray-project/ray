import json
import logging
import os
import re
import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Union

import time
import anyscale
import boto3
import ray
import yaml
from anyscale import service
from anyscale.compute_config.models import ComputeConfig
from anyscale.service.models import ServiceState
from ray._common.test_utils import wait_for_condition
from ray.serve._private.utils import get_random_string

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)
REGION_NAME = "us-west-2"
SECRET_NAME = "llm_release_test_hf_token"

# This bucket is on anyscale-dev-product account and the
# anyscale-staging cloud is already configured to have write
# access to this bucket
# Buildkite is also configured to have read access to this bucket
S3_BUCKET = "rayllm-ci-results"
S3_PREFIX = "vllm_perf_results"


def check_service_state(
    service_name: str, expected_state: ServiceState, cloud: Optional[str] = None
):
    """Check if the service is in the expected state."""
    state = service.status(name=service_name, cloud=cloud).state
    logger.info(
        f"Waiting for service {service_name} to be {expected_state}, currently {state}"
    )
    assert (
        state == expected_state
    ), f"Service {service_name} is {state}, expected {expected_state}."
    return True


def terminate_service_if_running(service_name: str, cloud: Optional[str] = None):
    try:
        status = service.status(name=service_name, cloud=cloud)
    except RuntimeError:
        return

    if status.state != ServiceState.TERMINATED:
        logger.info(
            f"Service {service_name} is in state {status.state}. Terminating it before running the benchmark."
        )
        service.terminate(name=service_name, cloud=cloud)
        service.wait(name=service_name, cloud=cloud, state=ServiceState.TERMINATED)
        logger.info(f"Service {service_name} is now terminated.")


@contextmanager
def timeit(stage: str, time_metrics: Dict[str, float]):
    start = time.perf_counter()
    yield
    end = time.perf_counter()

    duration = end - start
    logger.info(f"Stage '{stage}' took {duration:.2f} seconds.")
    time_metrics[f"time_{stage}"] = duration


@contextmanager
def start_service(
    service_name: str,
    compute_config: Union[ComputeConfig, str],
    applications: List[Dict],
    image_uri: Optional[str] = None,
    working_dir: Optional[str] = None,
    add_unique_suffix: bool = True,
    cloud: Optional[str] = None,
    env_vars: Optional[Dict[str, str]] = None,
    timeout_s: int = 600,  # seconds
):
    """Starts an Anyscale Service with the specified configs.

    Args:
        service_name: Name of the Anyscale Service. The actual service
            name may be modified if `add_unique_suffix` is True.
        compute_config: The configuration for the hardware resources
            that the cluster will utilize.
        applications: The list of Ray Serve applications to run in the
            service.
        image_uri: The URI of the Docker image to use for the service.
            If None, the image URI is fetched and constructed from the env var.
        working_dir: The working directory for the service.
        add_unique_suffix: Whether to append a unique suffix to the
            service name.
        cloud: The cloud to deploy the service to.
        env_vars: The environment variables to set in the service.
        timeout_s: The maximum time to wait for the service to start
            and terminate, in seconds.
    """

    if add_unique_suffix:
        ray_commit = (
            ray.__commit__[:8] if ray.__commit__ != "{{RAY_COMMIT_SHA}}" else "nocommit"
        )
        service_name = f"{service_name}-{ray_commit}-{get_random_string()}"

    if image_uri is None:
        # We expect this environment variable to be set for all release tests
        cluster_env = os.environ["ANYSCALE_JOB_CLUSTER_ENV_NAME"]
        image_uri = f"anyscale/image/{cluster_env}:1"

    time_metrics = {}
    service_config = service.ServiceConfig(
        name=service_name,
        image_uri=image_uri,
        compute_config=compute_config,
        working_dir=working_dir,
        applications=applications,
        env_vars=env_vars,
        query_auth_token_enabled=False,
    )

    # If the service already exists, terminate and the start a new service
    # so the new service starts immediately. Otherwise, start a new service
    # without a canary_percent.
    terminate_service_if_running(service_name=service_name, cloud=cloud)

    try:
        logger.info(f"Service config: {service_config}")
        with timeit("service_startup", time_metrics):
            service.deploy(service_config)

            wait_for_condition(
                check_service_state,
                service_name=service_name,
                expected_state=ServiceState.RUNNING,
                retry_interval_ms=10000,  # 10s
                timeout=timeout_s,
                cloud=cloud,
            )

        service_status = service.status(name=service_name, cloud=cloud)

        yield {
            "api_url": service_status.query_url,
            "api_token": service_status.query_auth_token,
            **time_metrics,
        }

    finally:
        logger.info(f"Terminating service {service_name}.")
        service.terminate(name=service_name, cloud=cloud)
        wait_for_condition(
            check_service_state,
            service_name=service_name,
            expected_state="TERMINATED",
            retry_interval_ms=10000,  # 10s
            timeout=timeout_s,
            cloud=cloud,
        )
        logger.info(f"Service '{service_name}' terminated successfully.")


def get_current_compute_config_name() -> str:
    """Get the name of the current compute config."""
    cluster_id = os.environ["ANYSCALE_CLUSTER_ID"]
    sdk = anyscale.AnyscaleSDK()
    cluster = sdk.get_cluster(cluster_id)
    return anyscale.compute_config.get(
        name="", _id=cluster.result.cluster_compute_id
    ).name


def get_applications(serve_config_file: str) -> List[Any]:
    """Get the applications from the serve config file."""
    with open(serve_config_file, "r") as f:
        loaded_llm_config = yaml.safe_load(f)
    return loaded_llm_config["applications"]


def setup_client_env_vars(api_url: str, api_token: Optional[str] = None):
    """Set up the environment variables for the tests."""
    os.environ["OPENAI_API_BASE"] = f"{api_url.rstrip('/')}/v1"
    os.environ["OPENAI_API_KEY"] = api_token or "fake-key"


def get_hf_token_env_var() -> Dict[str, str]:
    """Get the environment variables for the service."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=REGION_NAME)
    secret_string = client.get_secret_value(SecretId=SECRET_NAME)["SecretString"]
    return json.loads(secret_string)


def get_python_version_from_image(image_name: str) -> str:
    """Regex to capture the python version from the image name.

    If the image name does not contain a python version, an empty string is returned.
    """
    if image_name is None:
        return ""

    image_python_version_regex_match = re.search(r"py[0-9]+", image_name)
    if image_python_version_regex_match and image_python_version_regex_match.group(0):
        return image_python_version_regex_match.group(0)

    return ""


def append_python_version_from_image(name: str, image_name: str) -> str:
    """Regex to capture the python version from the image name and append it to the
    given name.

    If the image name does not contain a python version, the name is returned as is.
    """
    python_version = get_python_version_from_image(image_name)
    if python_version:
        return f"{name}_{python_version}"

    return name


def get_vllm_s3_storage_path() -> str:
    build_number = os.environ.get(
        "BUILDKITE_BUILD_NUMBER", uuid.uuid4().hex[:5].upper()
    )
    retry_count = os.environ.get("BUILDKITE_RETRY_COUNT", "0")
    unique_id = f"build-{build_number}-{retry_count}"

    storage_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/vllm-perf-results-{unique_id}.jsonl"

    return storage_path
