from typing import TYPE_CHECKING, Optional

from ray_release.logger import logger
from ray_release.util import get_anyscale_sdk

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


LAST_LOGS_LENGTH = 10


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
