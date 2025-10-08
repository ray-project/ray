from typing import TYPE_CHECKING, Optional, Dict, Any

from ray_release.logger import logger
from ray_release.util import get_anyscale_sdk

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

from anyscale.compute_config.models import (
    ComputeConfig,
    HeadNodeConfig,
    WorkerNodeGroupConfig,
    MarketType,
)
from ray_release.test import Test

LAST_LOGS_LENGTH = 100


def find_cloud_by_name(
    cloud_name: str, sdk: Optional["AnyscaleSDK"] = None
) -> Optional[str]:
    sdk = sdk or get_anyscale_sdk()

    cloud_id = None
    logger.info(f"Looking up cloud with name `{cloud_name}`. ")

    paging_token = None
    while not cloud_id:
        result = sdk.search_clouds(
            clouds_query=dict(paging=dict(count=50, paging_token=paging_token))
        )

        paging_token = result.metadata.next_paging_token

        for res in result.results:
            if res.name == cloud_name:
                cloud_id = res.id
                logger.info(f"Found cloud with name `{cloud_name}` as `{cloud_id}`")
                break

        if not paging_token or cloud_id or not len(result.results):
            break

    return cloud_id


def get_project_name(project_id: str, sdk: Optional["AnyscaleSDK"] = None) -> str:
    sdk = sdk or get_anyscale_sdk()

    result = sdk.get_project(project_id)
    return result.result.name


def get_cluster_name(cluster_id: str, sdk: Optional["AnyscaleSDK"] = None) -> str:
    sdk = sdk or get_anyscale_sdk()

    result = sdk.get_cluster(cluster_id)
    return result.result.name


def convert_cluster_compute_to_anyscale_compute_config(
    cluster_compute: Dict[str, Any]
) -> ComputeConfig:
    head_node = cluster_compute["head_node_type"]
    worker_node_types = cluster_compute["worker_node_types"]
    cloud_id = cluster_compute["cloud_id"]

    head_node_config = HeadNodeConfig(
        instance_type=head_node["instance_type"],
    )
    worker_node_configs = []
    for worker_node_type in worker_node_types:
        worker_node_config = WorkerNodeGroupConfig(
            instance_type=worker_node_type["instance_type"],
            min_workers=worker_node_type["min_workers"],
            max_workers=worker_node_type["max_workers"],
        )
        if worker_node_type.get("use_spot", False):
            worker_node_config.market_type = MarketType.SPOT
        worker_node_configs.append(worker_node_config)

    compute_config = ComputeConfig(
        cloud_id=cloud_id,
        head_node=head_node_config,
        worker_node_groups=worker_node_configs,
    )
    return compute_config


def get_entrypoint_command(
    test: Test,
    timeout: int,
    smoke_test: bool,
):
    command = test["run"]["script"]
    command_env = test.get_byod_runtime_env()

    raise_on_timeout = not test["run"].get("long_running", False)
    no_raise_on_timeout_str = "--test-no-raise-on-timeout" if raise_on_timeout else ""
    if smoke_test:
        command = f"{command} --smoke-test"
        command_env["IS_SMOKE_TEST"] = "1"

    full_command = (
        f"python anyscale_job_wrapper.py '{command}'"
        f"--test-workload-timeout {timeout}{no_raise_on_timeout_str} "
    )

    return full_command


"""
cloud_id: {{env["ANYSCALE_CLOUD_ID"]}}
region: us-west-2

head_node_type:
    name: head_node
    instance_type: m5.xlarge  # 4 CPUs

worker_node_types:
    - name: worker_node
      instance_type: m5.xlarge
      min_workers: 0
      max_workers: 2
      use_spot: false
"""
