import os

import ray
from ray._private.gcs_utils import GcsAioClient
from ray.dashboard.modules.job.job_manager import JobManager

TEST_NAMESPACE = "jobs_test_namespace"


def create_ray_cluster(_tracing_startup_hook=None):
    return ray.init(
        num_cpus=16,
        num_gpus=1,
        resources={"Custom": 1},
        namespace=TEST_NAMESPACE,
        log_to_driver=True,
        _tracing_startup_hook=_tracing_startup_hook,
    )


def create_job_manager(ray_cluster, tmp_path):
    address_info = ray_cluster
    gcs_aio_client = GcsAioClient(address=address_info["gcs_address"])
    return JobManager(gcs_aio_client, tmp_path)


def _driver_script_path(file_name: str) -> str:
    return os.path.join(
        os.path.dirname(__file__), "subprocess_driver_scripts", file_name
    )
