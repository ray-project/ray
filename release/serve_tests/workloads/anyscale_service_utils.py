from contextlib import contextmanager
import logging
import os
from typing import Dict, List, Union

from anyscale import service
from ray._private.test_utils import wait_for_condition
from ray_release.aws import maybe_fetch_api_token


logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


def check_service_state(service_name: str, expected_state: str):
    state = service.status(name=service_name).state
    logger.info(
        f"Waiting for service {service_name} to be {expected_state}, currently {state}"
    )
    assert (
        str(state) == expected_state
    ), f"Service {service_name} is {state}, expected {expected_state}."
    return True


IMAGE_URI = (
    "anyscale/image/029272617770_dkr_ecr_us-west-2_amazonaws_com_anyscale_ray-ml_pr-"
    "42421_56a0db-py39-gpu-59a4f3f3d77a575e353cad97e5b133f9f8ed0fa5a9548c934ccbb904754877ac"  # noqa
    "__env__44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a:1"
)


@contextmanager
def start_service(
    service_name: str,
    compute_config: Union[str, Dict],
    applications: List[Dict],
):
    maybe_fetch_api_token()
    print("os.environ", os.environ)

    service_config = service.ServiceConfig(
        name=service_name,
        image_uri=IMAGE_URI,
        compute_config=compute_config,
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

        yield

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
