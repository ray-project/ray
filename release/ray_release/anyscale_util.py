from typing import Any, Optional

from ray_release.exception import ClusterEnvCreateError
from ray_release.logger import logger

LAST_LOGS_LENGTH = 100


class Anyscale:
    """
    A wrapper class for latest version of Anyscale SDK.

    Provides lazy-imported access to anyscale.image, anyscale.compute_config,
    and anyscale.project, as well as the legacy AnyscaleSDK for DefaultApi calls.
    All imports are deferred to avoid triggering credential lookup at import time.

    Methods of this class can be overwritten for testing.
    """

    def __init__(self):
        self._anyscale_pkg = None
        self._legacy_sdk = None

    def _anyscale(self) -> Any:
        if self._anyscale_pkg is None:
            import anyscale

            self._anyscale_pkg = anyscale

        return self._anyscale_pkg

    @property
    def image(self) -> Any:
        return self._anyscale().image

    @property
    def compute_config(self) -> Any:
        return self._anyscale().compute_config

    @property
    def legacy_sdk(self) -> Any:
        """Return a cached AnyscaleSDK instance for legacy DefaultApi calls."""
        if self._legacy_sdk is None:
            from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

            from ray_release.util import ANYSCALE_HOST

            self._legacy_sdk = AnyscaleSDK(host=str(ANYSCALE_HOST))
        return self._legacy_sdk

    def project_name_by_id(self, project_id: str) -> str:
        return self._anyscale().project.get(project_id).name


# Global singleton for the V2 Anyscale SDK.
the_v2_sdk = Anyscale()


def get_project_name(project_id: str, sdk: Optional[Anyscale] = None) -> str:
    sdk = sdk or the_v2_sdk
    return sdk.project_name_by_id(project_id)


def get_custom_cluster_env_name(image: str, test_name: str) -> str:
    image_normalized = image.replace("/", "_").replace(":", "_").replace(".", "_")
    return f"test_env_{image_normalized}_{test_name}"


def create_cluster_env_from_image(
    image: str,
    test_name: str,
    cluster_env_id: Optional[str] = None,
    cluster_env_name: Optional[str] = None,
    sdk: Optional["Anyscale"] = None,
) -> str:
    """Register image with anyscale.image APIs and return the cluster env ID."""
    anyscale_image = (sdk or the_v2_sdk).image

    if not cluster_env_name:
        cluster_env_name = get_custom_cluster_env_name(image, test_name)

    if not cluster_env_id:
        for img in anyscale_image.list(name=cluster_env_name):
            if img.name == cluster_env_name:
                cluster_env_id = img.id
                logger.info(f"Cluster env already exists with ID {cluster_env_id}")
                break

    if not cluster_env_id:
        logger.info("Cluster env not found. Creating new one.")
        try:
            anyscale_image.register(image, name=cluster_env_name, ray_version="nightly")
            img = anyscale_image.get(name=cluster_env_name)
            cluster_env_id = img.id
        except Exception as e:
            raise ClusterEnvCreateError("Could not create cluster env.") from e

        logger.info(f"Cluster env created with ID {cluster_env_id}")

    return cluster_env_id
