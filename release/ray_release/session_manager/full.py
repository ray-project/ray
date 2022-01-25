import time

from e2e import SessionTimeoutError
from ray_release.exception import ReleaseTestInfraError
from ray_release.logger import logger
from ray_release.session_manager.minimal import MinimalSessionManager
from ray_release.util import (format_link, anyscale_session_url,
                              run_with_timeout)

REPORT_S = 30.


class FullSessionManager(MinimalSessionManager):
    """Full manager.

    Builds app config and compute template and starts/terminated session
    using SDK.
    """

    def start_cluster(self, timeout: float = 600.):
        logger.info(f"Creating cluster {self.cluster_name}")
        result = self.sdk.create_cluster(
            dict(
                name=self.cluster_name,
                project_id=self.project_id,
                cluster_environment_build_id=self.cluster_env_build_id,
                cluster_compute_id=self.cluster_compute_id,
                idle_timeout_minutes=30))
        self.cluster_id = result.result.id

        # Trigger session start
        logger.info(
            f"Starting cluster {self.cluster_name} ({self.cluster_id})")
        session_url = anyscale_session_url(
            project_id=self.project_id, session_id=self.cluster_id)
        logger.info(f"Link to cluster: {format_link(session_url)}")

        result = self.sdk.start_cluster(
            self.cluster_id, start_cluster_options={})
        cop_id = result.result.id
        completed = result.result.completed

        # Wait for session
        logger.info(f"Waiting for cluster {self.cluster_name}...")

        def _start_cluster():
            completed = False
            while not completed:
                # Sleep 1 sec before next check.
                time.sleep(1)

                cluster_operation_response = self.sdk.get_cluster_operation(
                    cop_id, _request_timeout=30)
                cluster_operation = cluster_operation_response.result
                completed = cluster_operation.completed

        def _status_fn(time_elapsed: float):
            logger.info(f"... still waiting for cluster {self.cluster_name} "
                        f"({int(time_elapsed)} seconds) ...")

        def _error_fn():
            raise SessionTimeoutError(
                f"Time out when creating cluster {self.cluster_name}")

        if not completed:
            run_with_timeout(
                _start_cluster,
                timeout=timeout,
                status_fn=_status_fn,
                error_fn=_error_fn)

        result = self.sdk.get_cluster(self.cluster_id)
        if not result.result.state != "Active":
            raise ReleaseTestInfraError(
                f"Cluster did not come up - most likely the nodes are currently "
                f"not available. Please check the cluster startup logs: "
                f"{anyscale_session_url(self.project_id, self.cluster_id)}")

    def terminate_cluster(self, wait: bool = False):
        if self.cluster_id:
            # Just trigger a request. No need to wait until session shutdown.
            result = self.sdk.terminate_cluster(
                cluster_id=self.cluster_id, terminate_cluster_options={})

            if not wait:
                return

            # Only do this when waiting
            cop_id = result.result.id
            completed = result.result.completed
            while not completed:
                # Sleep 1 sec before next check.
                time.sleep(1)

                cluster_operation_response = self.sdk.get_cluster_operation(
                    cop_id, _request_timeout=30)
                cluster_operation = cluster_operation_response.result
                completed = cluster_operation.completed

            result = self.sdk.get_cluster(self.cluster_id)
            while result.result.state != "Terminated":
                time.sleep(1)
                result = self.sdk.get_cluster(self.cluster_id)
