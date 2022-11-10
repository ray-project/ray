import time

from ray_release.exception import (
    ClusterCreationError,
    ClusterStartupError,
    ClusterStartupTimeout,
    ClusterStartupFailed,
)
from ray_release.logger import logger
from ray_release.cluster_manager.minimal import MinimalClusterManager
from ray_release.util import (
    format_link,
    anyscale_cluster_url,
    exponential_backoff_retry,
)

REPORT_S = 30.0


class FullClusterManager(MinimalClusterManager):
    """Full manager.

    Builds app config and compute template and starts/terminated session
    using SDK.
    """

    def start_cluster(self, timeout: float = 600.0):
        logger.info(f"Creating cluster {self.cluster_name}")
        logger.info(f"Autosuspend time: {self.autosuspend_minutes} minutes")
        logger.info(f"Auto terminate after: {self.maximum_uptime_minutes} minutes")
        try:
            result = self.sdk.create_cluster(
                dict(
                    name=self.cluster_name,
                    project_id=self.project_id,
                    cluster_environment_build_id=self.cluster_env_build_id,
                    cluster_compute_id=self.cluster_compute_id,
                    idle_timeout_minutes=self.autosuspend_minutes,
                )
            )
            self.cluster_id = result.result.id
        except Exception as e:
            raise ClusterCreationError(f"Error creating cluster: {e}") from e

        # Trigger session start
        logger.info(f"Starting cluster {self.cluster_name} ({self.cluster_id})")
        cluster_url = anyscale_cluster_url(
            project_id=self.project_id, session_id=self.cluster_id
        )
        logger.info(f"Link to cluster: {format_link(cluster_url)}")

        try:
            result = self.sdk.start_cluster(self.cluster_id, start_cluster_options={})
            cop_id = result.result.id
            completed = result.result.completed
        except Exception as e:
            raise ClusterStartupError(
                f"Error starting cluster with name "
                f"{self.cluster_name} and {self.cluster_id} ({cluster_url}): "
                f"{e}"
            ) from e

        # Wait for session
        logger.info(f"Waiting for cluster {self.cluster_name}...")

        start_time = time.monotonic()
        timeout_at = start_time + timeout
        next_status = start_time + 30
        while not completed:
            now = time.monotonic()
            if now >= timeout_at:
                raise ClusterStartupTimeout(
                    f"Time out when creating cluster {self.cluster_name}"
                )

            if now >= next_status:
                logger.info(
                    f"... still waiting for cluster {self.cluster_name} "
                    f"({int(now - start_time)} seconds) ..."
                )
                next_status += 30

            # Sleep 1 sec before next check.
            time.sleep(1)

            result = exponential_backoff_retry(
                lambda: self.sdk.get_cluster_operation(cop_id, _request_timeout=30),
                retry_exceptions=Exception,
                initial_retry_delay_s=2,
                max_retries=3,
            )
            completed = result.result.completed

        result = self.sdk.get_cluster(self.cluster_id)
        if result.result.state != "Running":
            raise ClusterStartupFailed(
                f"Cluster did not come up - most likely the nodes are currently "
                f"not available. Please check the cluster startup logs: "
                f"{cluster_url} (cluster state: {result.result.state})"
            )

    def terminate_cluster(self, wait: bool = False):
        if self.cluster_id:
            # Just trigger a request. No need to wait until session shutdown.
            result = self.sdk.terminate_cluster(
                cluster_id=self.cluster_id, terminate_cluster_options={}
            )

            if not wait:
                return

            # Only do this when waiting
            cop_id = result.result.id
            completed = result.result.completed
            while not completed:
                # Sleep 1 sec before next check.
                time.sleep(1)

                cluster_operation_response = self.sdk.get_cluster_operation(
                    cop_id, _request_timeout=30
                )
                cluster_operation = cluster_operation_response.result
                completed = cluster_operation.completed

            result = self.sdk.get_cluster(self.cluster_id)
            while result.result.state != "Terminated":
                time.sleep(1)
                result = self.sdk.get_cluster(self.cluster_id)
