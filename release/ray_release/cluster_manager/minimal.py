import time
from copy import deepcopy

from ray_release.exception import (
    ClusterEnvBuildError,
    ClusterEnvBuildTimeout,
    ClusterEnvCreateError,
    ClusterComputeCreateError,
)
from ray_release.logger import logger
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.util import format_link, anyscale_cluster_env_build_url
from retry import retry

REPORT_S = 30.0


class MinimalClusterManager(ClusterManager):
    """Minimal manager.

    Builds app config and compute template but does not start or stop session.
    """

    extra_tags_resource_types = ["instance"]

    @retry((ClusterEnvCreateError), delay=10, jitter=5, tries=2)
    def create_cluster_env(self):
        assert self.cluster_env_id is None

        if self.cluster_env:
            assert self.cluster_env_name

            logger.info(
                f"Test uses a cluster env with name "
                f"{self.cluster_env_name}. Looking up existing "
                f"cluster envs with this name."
            )

            paging_token = None
            while not self.cluster_env_id:
                result = self.sdk.search_cluster_environments(
                    dict(
                        project_id=self.project_id,
                        name=dict(equals=self.cluster_env_name),
                        paging=dict(count=50, token=paging_token),
                    )
                )
                paging_token = result.metadata.next_paging_token

                for res in result.results:
                    if res.name == self.cluster_env_name:
                        self.cluster_env_id = res.id
                        logger.info(
                            f"Cluster env already exists with ID "
                            f"{self.cluster_env_id}"
                        )
                        break

                if not paging_token or self.cluster_env_id:
                    break

            if not self.cluster_env_id:
                logger.info("Cluster env not found. Creating new one.")
                try:
                    result = self.sdk.create_cluster_environment(
                        dict(
                            name=self.cluster_env_name,
                            project_id=self.project_id,
                            config_json=self.cluster_env,
                        )
                    )
                    self.cluster_env_id = result.result.id
                except Exception as e:
                    logger.warning(
                        f"Got exception when trying to create cluster "
                        f"env: {e}. Sleeping for 10 seconds with jitter and then "
                        f"try again..."
                    )
                    raise ClusterEnvCreateError("Could not create cluster env.") from e

                logger.info(f"Cluster env created with ID {self.cluster_env_id}")

    def build_cluster_env(self, timeout: float = 600.0):
        assert self.cluster_env_id
        assert self.cluster_env_build_id is None

        # Fetch build
        build_id = None
        last_status = None
        error_message = None
        config_json = None
        result = self.sdk.list_cluster_environment_builds(self.cluster_env_id)
        if not result or not result.results:
            raise ClusterEnvBuildError(f"No build found for cluster env: {result}")

        build = sorted(result.results, key=lambda b: b.created_at)[-1]
        build_id = build.id
        last_status = build.status
        error_message = build.error_message
        config_json = build.config_json

        if last_status == "succeeded":
            logger.info(
                f"Link to succeeded cluster env build: "
                f"{format_link(anyscale_cluster_env_build_url(build_id))}"
            )
            self.cluster_env_build_id = build_id
            return

        if last_status == "failed":
            logger.info(f"Previous cluster env build failed: {error_message}")
            logger.info("Starting new cluster env build...")

            # Retry build
            result = self.sdk.create_cluster_environment_build(
                dict(
                    cluster_environment_id=self.cluster_env_id, config_json=config_json
                )
            )
            build_id = result.result.id

            logger.info(
                f"Link to created cluster env build: "
                f"{format_link(anyscale_cluster_env_build_url(build_id))}"
            )

        # Build found but not failed/finished yet
        completed = False
        start_wait = time.time()
        next_report = start_wait + REPORT_S
        timeout_at = time.monotonic() + timeout
        logger.info(f"Waiting for build {build_id} to finish...")
        logger.info(
            f"Track progress here: "
            f"{format_link(anyscale_cluster_env_build_url(build_id))}"
        )
        while not completed:
            now = time.time()
            if now > next_report:
                logger.info(
                    f"... still waiting for build {build_id} to finish "
                    f"({int(now - start_wait)} seconds) ..."
                )
                next_report = next_report + REPORT_S

            result = self.sdk.get_build(build_id)
            build = result.result

            if build.status == "failed":
                raise ClusterEnvBuildError(
                    f"Cluster env build failed. Please see "
                    f"{anyscale_cluster_env_build_url(build_id)} for details. "
                    f"Error message: {build.error_message}"
                )

            if build.status == "succeeded":
                logger.info("Build succeeded.")
                self.cluster_env_build_id = build_id
                return

            completed = build.status not in ["in_progress", "pending"]

            if completed:
                raise ClusterEnvBuildError(
                    f"Unknown build status: {build.status}. Please see "
                    f"{anyscale_cluster_env_build_url(build_id)} for details"
                )

            if time.monotonic() > timeout_at:
                raise ClusterEnvBuildTimeout(
                    f"Time out when building cluster env {self.cluster_env_name}"
                )

            time.sleep(1)

        self.cluster_env_build_id = build_id

    def fetch_build_info(self):
        assert self.cluster_env_build_id

        result = self.sdk.get_cluster_environment_build(self.cluster_env_build_id)
        self.cluster_env = result.result.config_json

    def create_cluster_compute(self, _repeat: bool = True):
        assert self.cluster_compute_id is None

        if self.cluster_compute:
            assert self.cluster_compute

            logger.info(
                f"Tests uses compute template "
                f"with name {self.cluster_compute_name}. "
                f"Looking up existing cluster computes."
            )

            paging_token = None
            while not self.cluster_compute_id:
                result = self.sdk.search_cluster_computes(
                    dict(
                        project_id=self.project_id,
                        name=dict(equals=self.cluster_compute_name),
                        include_anonymous=True,
                        paging=dict(token=paging_token),
                    )
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
                    result = self.sdk.create_cluster_compute(
                        dict(
                            name=self.cluster_compute_name,
                            project_id=self.project_id,
                            config=self.cluster_compute_with_extra_tags,
                        )
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
                        return self.create_cluster_compute(_repeat=False)

                    raise ClusterComputeCreateError(
                        "Could not create cluster compute"
                    ) from e

                logger.info(
                    f"Cluster compute template created with "
                    f"name {self.cluster_compute_name} and "
                    f"ID {self.cluster_compute_id}"
                )

    @property
    def cluster_compute_with_extra_tags(self) -> dict:
        if self.extra_tags and self._get_cloud_provider() == "AWS":
            cluster_compute = self.cluster_compute.copy()
            if "aws" in cluster_compute:
                cluster_compute["aws"] = deepcopy(cluster_compute["aws"])
            else:
                cluster_compute["aws"] = {}
            cluster_compute["aws"].setdefault("TagSpecifications", [])
            tag_specifications = cluster_compute["aws"]["TagSpecifications"]
            for resource in self.extra_tags_resource_types:
                resource_tags: dict = next(
                    (
                        x
                        for x in tag_specifications
                        if x.get("ResourceType", "") == resource
                    ),
                    None,
                )
                if resource_tags is None:
                    resource_tags = {"ResourceType": resource, "Tags": []}
                    tag_specifications.append(resource_tags)
                tags = resource_tags["Tags"]
                for key, value in self.extra_tags.items():
                    tags.append({"Key": key, "Value": value})
            return cluster_compute
        else:
            return self.cluster_compute

    def _get_cloud_provider(self) -> str:
        assert self.cluster_compute and "cloud_id" in self.cluster_compute
        return self.sdk.get_cloud(self.cluster_compute["cloud_id"]).result.provider

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

    def delete_configs(self):
        if self.cluster_id:
            self.sdk.delete_cluster(self.cluster_id)
        if self.cluster_env_build_id:
            self.sdk.delete_cluster_environment_build(self.cluster_env_build_id)
        if self.cluster_env_id:
            self.sdk.delete_cluster_environment(self.cluster_env_id)
        if self.cluster_compute_id:
            self.sdk.delete_cluster_compute(self.cluster_compute_id)

    def start_cluster(self, timeout: float = 600.0):
        pass

    def terminate_cluster(self):
        pass

    def get_cluster_address(self) -> str:
        return f"anyscale://{self.project_name}/{self.cluster_name}"
