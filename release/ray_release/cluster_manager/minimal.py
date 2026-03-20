import time

from anyscale.compute_config.models import ComputeConfig
from anyscale.sdk.anyscale_client.api.default_api import DefaultApi

from ray_release.anyscale_util import create_cluster_env_from_image
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import (
    ClusterComputeCreateError,
    ClusterEnvBuildError,
    ClusterEnvBuildTimeout,
    ClusterEnvCreateError,
)
from ray_release.logger import logger
from ray_release.retry import retry
from ray_release.util import (
    anyscale_cluster_env_build_url,
    format_link,
)

REPORT_S = 30.0

# Fields accepted by anyscale.compute_config.models.ComputeConfig.
COMPUTE_CONFIG_FIELDS = {
    "cloud",
    "cloud_resource",
    "head_node",
    "worker_nodes",
    "min_resources",
    "max_resources",
    "zones",
    "enable_cross_zone_scaling",
    "advanced_instance_config",
    "flags",
    "auto_select_worker_config",
}


class MinimalClusterManager(ClusterManager):
    """Minimal manager.

    Builds app config and compute template but does not start or stop session.
    """

    @retry(
        init_delay_sec=10,
        jitter_sec=5,
        max_retry_count=2,
        exceptions=(ClusterEnvCreateError,),
    )
    def create_cluster_env(self):
        assert self.cluster_env_id is None
        assert self.cluster_env_name

        logger.info(
            f"Test uses a cluster env with name "
            f"{self.cluster_env_name}. Looking up existing "
            f"cluster envs with this name."
        )

        self.cluster_env_id = create_cluster_env_from_image(
            image=self.test.get_anyscale_byod_image(),
            test_name=self.cluster_env_name,
            cluster_env_id=self.cluster_env_id,
            cluster_env_name=self.cluster_env_name,
            sdk=self.sdk,
        )

    def build_cluster_env(self, timeout: float = 600.0):
        assert self.cluster_env_id
        assert self.cluster_env_build_id is None

        """Poll build status using anyscale.image.get()."""
        img = self.sdk.image.get(name=self.cluster_env_name)
        build_id = img.latest_build_id
        status = (
            str(img.latest_build_status).upper() if img.latest_build_status else None
        )

        if not build_id:
            raise ClusterEnvBuildError(
                f"No build found for cluster env: {self.cluster_env_name}"
            )

        if status == "SUCCEEDED":
            logger.info(
                f"Link to succeeded cluster env build: "
                f"{format_link(anyscale_cluster_env_build_url(build_id))}"
            )
            if img.latest_build_revision is None:
                raise ClusterEnvBuildError(
                    f"Build succeeded but revision is missing for "
                    f"{self.cluster_env_name}"
                )
            # image_uri for JobConfig: "anyscale/image/{name}:{revision}"
            self.cluster_env_build_id = (
                f"anyscale/image/{img.name}:{img.latest_build_revision}"
            )
            return

        if status == "FAILED":
            raise ClusterEnvBuildError(
                f"Cluster env build failed. Please see "
                f"{anyscale_cluster_env_build_url(build_id)} for details."
            )

        # Build in progress — poll until done
        start_wait = time.time()
        next_report = start_wait + REPORT_S
        timeout_at = time.monotonic() + timeout
        logger.info(f"Waiting for build {build_id} to finish...")
        logger.info(
            f"Track progress here: "
            f"{format_link(anyscale_cluster_env_build_url(build_id))}"
        )
        while True:
            now = time.time()
            if now > next_report:
                logger.info(
                    f"... still waiting for build {build_id} to finish "
                    f"({int(now - start_wait)} seconds) ..."
                )
                next_report = next_report + REPORT_S

            img = self.sdk.image.get(name=self.cluster_env_name)
            status = (
                str(img.latest_build_status).upper()
                if img.latest_build_status
                else None
            )

            if status == "FAILED":
                raise ClusterEnvBuildError(
                    f"Cluster env build failed. Please see "
                    f"{anyscale_cluster_env_build_url(build_id)} for details."
                )

            if status == "SUCCEEDED":
                logger.info("Build succeeded.")
                if img.latest_build_revision is None:
                    raise ClusterEnvBuildError(
                        f"Build succeeded but revision is missing for "
                        f"{self.cluster_env_name}"
                    )
                # image_uri for JobConfig: "anyscale/image/{name}:{revision}"
                self.cluster_env_build_id = (
                    f"anyscale/image/{img.name}:{img.latest_build_revision}"
                )
                return

            if status not in ("IN_PROGRESS", None):
                raise ClusterEnvBuildError(
                    f"Unknown build status: {status}. Please see "
                    f"{anyscale_cluster_env_build_url(build_id)} for details"
                )

            if time.monotonic() > timeout_at:
                raise ClusterEnvBuildTimeout(
                    f"Time out when building cluster env {self.cluster_env_name}"
                )

            time.sleep(1)

    def create_cluster_compute(self, _repeat: bool = True):
        assert self.cluster_compute_id is None

        if self.cluster_compute:
            logger.info(
                f"Tests uses compute template "
                f"with name {self.cluster_compute_name}. "
                f"Looking up existing cluster computes."
            )

            if self.test.uses_anyscale_sdk_2026():
                self._create_cluster_compute_new_sdk(_repeat=_repeat)
            else:
                self._create_cluster_compute_legacy(_repeat=_repeat)

    def _create_cluster_compute_legacy(self, _repeat: bool = True):
        """Create or find a cluster compute using the legacy DefaultApi SDK."""
        legacy_sdk = self.sdk.legacy_sdk
        paging_token = None
        while not self.cluster_compute_id:
            result = DefaultApi.search_cluster_computes(
                legacy_sdk,
                dict(
                    project_id=self.project_id,
                    name=dict(equals=self.cluster_compute_name),
                    include_anonymous=True,
                    paging=dict(paging_token=paging_token),
                ),
            )
            paging_token = result.metadata.next_paging_token

            for res in result.results:
                if res.name == self.cluster_compute_name:
                    self.cluster_compute_id = res.id
                    logger.info(
                        f"Cluster compute already exists "
                        f"with ID {self.cluster_compute_id}"
                    )
                    break

            if not paging_token:
                break

        if not self.cluster_compute_id:
            logger.info(
                f"Cluster compute not found. "
                f"Creating with name {self.cluster_compute_name}."
            )
            try:
                result = DefaultApi.create_cluster_compute(
                    legacy_sdk,
                    dict(
                        name=self.cluster_compute_name,
                        project_id=self.project_id,
                        config=self.cluster_compute,
                    ),
                )
                self.cluster_compute_id = result.result.id
            except Exception as e:
                if _repeat:
                    logger.warning(
                        f"Got exception when trying to create cluster "
                        f"compute: {e}. Sleeping for 10 seconds and then "
                        f"try again once..."
                    )
                    time.sleep(10)
                    return self._create_cluster_compute_legacy(_repeat=False)

                raise ClusterComputeCreateError(
                    "Could not create cluster compute"
                ) from e

            logger.info(
                f"Cluster compute template created with "
                f"name {self.cluster_compute_name} and "
                f"ID {self.cluster_compute_id}"
            )

    def _create_cluster_compute_new_sdk(self, _repeat: bool = True):
        """Create or find a cluster compute using the new anyscale.compute_config API."""
        # Pagination not needed: names include a content hash (dict_hash),
        # so >20 versions of the same name is extremely unlikely.
        result = self.sdk.compute_config.list(name=self.cluster_compute_name)
        for cc in result.results:
            if cc.name.rsplit(":", 1)[0] == self.cluster_compute_name:
                self.cluster_compute_id = cc.id
                logger.info(
                    f"Cluster compute already exists "
                    f"with ID {self.cluster_compute_id}"
                )
                return

        logger.info(
            f"Cluster compute not found. "
            f"Creating with name {self.cluster_compute_name}."
        )
        try:
            # Filter to only fields accepted by ComputeConfig; keys like
            # idle_termination_minutes/maximum_uptime_minutes are cluster-level
            # settings not part of the ComputeConfig model.
            compute_kwargs = {
                k: v
                for k, v in self.cluster_compute.items()
                if k in COMPUTE_CONFIG_FIELDS
            }
            config = ComputeConfig(**compute_kwargs)
            full_name = self.sdk.compute_config.create(
                config, name=self.cluster_compute_name
            )
            version = self.sdk.compute_config.get(full_name)
            self.cluster_compute_id = version.id
        except Exception as e:
            if _repeat:
                logger.warning(
                    f"Got exception when trying to create cluster "
                    f"compute: {e}. Sleeping for 10 seconds and then "
                    f"try again once..."
                )
                time.sleep(10)
                return self._create_cluster_compute_new_sdk(_repeat=False)

            raise ClusterComputeCreateError("Could not create cluster compute") from e

        logger.info(
            f"Cluster compute template created with "
            f"name {self.cluster_compute_name} and "
            f"ID {self.cluster_compute_id}"
        )

    def build_configs(self, timeout: float = 30.0):
        try:
            self.create_cluster_compute()
        except AssertionError as e:
            # If already exists, ignore
            logger.warning(str(e))
        except ClusterComputeCreateError as e:
            raise e
        except Exception as e:
            raise ClusterComputeCreateError(
                f"Unexpected cluster compute build error: {e}"
            ) from e

        try:
            self.create_cluster_env()
        except AssertionError as e:
            # If already exists, ignore
            logger.warning(str(e))
        except ClusterEnvCreateError as e:
            raise e
        except Exception as e:
            raise ClusterEnvCreateError(
                f"Unexpected cluster env create error: {e}"
            ) from e

        try:
            self.build_cluster_env(timeout=timeout)
        except AssertionError as e:
            # If already exists, ignore
            logger.warning(str(e))
        except (ClusterEnvBuildError, ClusterEnvBuildTimeout) as e:
            raise e
        except Exception as e:
            raise ClusterEnvBuildError(
                f"Unexpected cluster env build error: {e}"
            ) from e
