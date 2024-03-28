import boto3
from contextlib import contextmanager
import logging
import os
from typing import Dict, List, Optional, Union

from anyscale.authenticate import AuthenticationBlock
from anyscale import AnyscaleSDK, service
from ray._private.test_utils import wait_for_condition


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


class DeferredEnvVar:
    def __init__(self, var: str, default: Optional[str] = None):
        self._var = var
        self._default = default

    def __str__(self):
        return os.environ.get(self._var, self._default)


RELEASE_AWS_ANYSCALE_SECRET_ARN = DeferredEnvVar(
    "RELEASE_AWS_ANYSCALE_SECRET_ARN",
    "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
    "release-automation/"
    "anyscale-token20210505220406333800000001-BcUuKB",
)


def get_anyscale_cli_token() -> str:
    try:
        token, _ = AuthenticationBlock._load_credentials()
        logger.info("Loaded anyscale credentials from local storage.")
        return token
    except Exception:
        pass  # Ignore errors

    logger.info("Missing ANYSCALE_CLI_TOKEN, retrieving from AWS secrets store")
    return boto3.client("secretsmanager", region_name="us-west-2").get_secret_value(
        SecretId=str(RELEASE_AWS_ANYSCALE_SECRET_ARN)
    )["SecretString"]


def get_current_build_id(sdk: AnyscaleSDK) -> str:
    cluster_id = os.environ["ANYSCALE_CLUSTER_ID"]
    cluster = sdk.get_cluster(cluster_id)
    return cluster.result.cluster_environment_build_id


def build_id_to_image_uri(sdk: AnyscaleSDK, build_id: str) -> str:
    build = sdk.get_cluster_environment_build(build_id).result
    cluster_env = sdk.get_cluster_environment(build.cluster_environment_id).result
    return f"anyscale/image/{cluster_env.name}:{build.revision}"


def check_service_state(sdk: AnyscaleSDK, service_id: str, expected_state: str):
    state = sdk.get_service(service_id).result.current_state
    print(f"Waiting for service {service_id} to be {expected_state}, currently {state}")
    assert (
        str(state) == expected_state
    ), f"Service {service_id} is {state}, expected {expected_state}."
    return True


@contextmanager
def start_service(
    sdk: AnyscaleSDK,
    service_name: str,
    compute_config: Union[str, Dict],
    applications: List[Dict],
):
    if not os.environ.get("ANYSCALE_CLI_TOKEN"):
        os.environ["ANYSCALE_CLI_TOKEN"] = get_anyscale_cli_token()

    current_build_id = get_current_build_id(sdk)
    image_uri = build_id_to_image_uri(sdk, current_build_id)
    service_config = service.ServiceConfig(
        name=service_name,
        image_uri=image_uri,
        compute_config=compute_config,
        applications=applications,
    )
    try:
        logger.info(f"Service config: {service_config}")
        service.deploy(service_config)

        status = service.status(name=service_name)
        wait_for_condition(
            check_service_state,
            sdk=sdk,
            service_id=status.id,
            expected_state="RUNNING",
            retry_interval_ms=10000,  # 10s
            timeout=600,
        )

        yield

    finally:
        logger.info(f"Terminating service {service_name}.")
        service.terminate(name=service_name)
        wait_for_condition(
            check_service_state,
            sdk=sdk,
            service_id=status.id,
            expected_state="TERMINATED",
            retry_interval_ms=10000,  # 10s
            timeout=600,
        )
        logger.info(f"Service '{service_name}' terminated successfully.")
