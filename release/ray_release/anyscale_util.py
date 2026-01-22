from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ray_release.exception import ClusterEnvCreateError
from ray_release.logger import logger
from ray_release.util import get_anyscale_sdk

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


LAST_LOGS_LENGTH = 100


class Anyscale:
    """
    A wrapper class for latest version of Anyscale SDK.

    Methods of this class can be overwritten for testing.
    """

    def __init__(self):
        # We need to late-import Anyscale SDK as merely importing it
        # will trigger credential lookup on the system.
        self._anyscale_pkg = None

    def _anyscale(self) -> Any:
        if self._anyscale_pkg is None:
            import anyscale

            self._anyscale_pkg = anyscale

        return self._anyscale_pkg

    def project_name_by_id(self, project_id: str) -> str:
        return self._anyscale().project.get(project_id).name


# Global singleton for the V2 Anyscale SDK.
the_v2_sdk = Anyscale()


def get_project_name(
    project_id: str, sdk: Optional[Union[Anyscale, "AnyscaleSDK"]] = None
) -> str:
    sdk = sdk or the_v2_sdk

    if not isinstance(sdk, Anyscale):
        # Fallback to old SDK if provided.
        # TODO(aslonnie): remove this once we fully migrate to new SDK.
        return sdk.get_project(project_id).result.name

    return sdk.project_name_by_id(project_id)


def get_custom_cluster_env_name(image: str, test_name: str) -> str:
    # TODO(aslonnie): remove this; new SDK does not need creating cluster envs anymore.
    image_normalized = image.replace("/", "_").replace(":", "_").replace(".", "_")
    return f"test_env_{image_normalized}_{test_name}"


def create_cluster_env_from_image(
    image: str,
    test_name: str,
    runtime_env: Dict[str, Any],
    sdk: Optional["AnyscaleSDK"] = None,
    cluster_env_id: Optional[str] = None,
    cluster_env_name: Optional[str] = None,
) -> str:
    # TODO(aslonnie): remove this; new SDK does not need creating cluster envs anymore.
    anyscale_sdk = sdk or get_anyscale_sdk()
    if not cluster_env_name:
        cluster_env_name = get_custom_cluster_env_name(image, test_name)

    # Find whether there is identical cluster env
    paging_token = None
    while not cluster_env_id:
        result = anyscale_sdk.search_cluster_environments(
            dict(
                name=dict(equals=cluster_env_name),
                paging=dict(count=50, paging_token=paging_token),
                project_id=None,
            )
        )
        paging_token = result.metadata.next_paging_token

        for res in result.results:
            if res.name == cluster_env_name:
                cluster_env_id = res.id
                logger.info(f"Cluster env already exists with ID " f"{cluster_env_id}")
                break

        if not paging_token or cluster_env_id:
            break

    if not cluster_env_id:
        logger.info("Cluster env not found. Creating new one.")
        try:
            result = anyscale_sdk.create_byod_cluster_environment(
                dict(
                    name=cluster_env_name,
                    config_json=dict(
                        docker_image=image,
                        ray_version="nightly",
                        env_vars=runtime_env,
                    ),
                )
            )
            cluster_env_id = result.result.id
        except Exception as e:
            logger.warning(
                f"Got exception when trying to create cluster "
                f"env: {e}. Sleeping for 10 seconds with jitter and then "
                f"try again..."
            )
            raise ClusterEnvCreateError("Could not create cluster env.") from e

        logger.info(f"Cluster env created with ID {cluster_env_id}")

    return cluster_env_id
