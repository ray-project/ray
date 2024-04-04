from contextlib import contextmanager
import logging
import os
from typing import Dict, List

from anyscale import service
from anyscale.service.models import ServiceState
from anyscale.compute_config.models import ComputeConfig
import ray
from ray._private.test_utils import wait_for_condition
from ray.serve._private.utils import get_random_string


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


def check_service_state(service_name: str, expected_state: ServiceState):
    state = service.status(name=service_name).state
    logger.info(
        f"Waiting for service {service_name} to be {expected_state}, currently {state}"
    )
    assert (
        state == expected_state
    ), f"Service {service_name} is {state}, expected {expected_state}."
    return True


@contextmanager
def start_service(
    service_name: str,
    compute_config: ComputeConfig,
    applications: List[Dict],
    add_unique_suffix: bool = True,
):
    """Starts an Anyscale Service with the specified configs.

    Args:
        service_name: Name of the Anyscale Service. The actual service
            name may be modified if `add_unique_suffix` is True.
        compute_config: The configuration for the hardware resources
            that the cluster will utilize.
        applications: The list of Ray Serve applications to run in the
            service.
        add_unique_suffix: Whether to append a unique suffix to the
            service name.
    """

    cluster_env = os.environ.get("ANYSCALE_JOB_CLUSTER_ENV_NAME", None)
    if add_unique_suffix:
        ray_commit = (
            ray.__commit__[:8] if ray.__commit__ != "{{RAY_COMMIT_SHA}}" else "nocommit"
        )
        service_name = f"{service_name}-{ray_commit}-{get_random_string()}"

    service_config = service.ServiceConfig(
        name=service_name,
        image_uri=f"anyscale/image/{cluster_env}:1" if cluster_env else None,
        compute_config=compute_config,
        working_dir="workloads",
        applications=applications,
    )
    try:
        logger.info(f"Service config: {service_config}")
        service.deploy(service_config)

        wait_for_condition(
            check_service_state,
            service_name=service_name,
            expected_state="RUNNING",
            retry_interval_ms=10000,  # 10s
            timeout=600,
        )

        yield service_name

    finally:
        logger.info(f"Terminating service {service_name}.")
        service.terminate(name=service_name)
        wait_for_condition(
            check_service_state,
            service_name=service_name,
            expected_state="TERMINATED",
            retry_interval_ms=10000,  # 10s
            timeout=600,
        )
        logger.info(f"Service '{service_name}' terminated successfully.")
