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

    def start_session(self, timeout: float = 600.):
        logger.info(f"Creating session {self.session_name}")
        result = self.sdk.create_session(
            dict(name=self.session_name, project_id=self.project_id))
        self.session_id = result.result.id

        # Trigger session start
        logger.info(
            f"Starting session {self.session_name} ({self.session_id})")
        session_url = anyscale_session_url(
            project_id=self.project_id, session_id=self.session_id)
        logger.info(f"Link to session: {format_link(session_url)}")

        result = self.sdk.start_session(
            self.session_id, start_session_options={})
        sop_id = result.result.id
        completed = result.result.completed

        # Wait for session
        logger.info(f"Waiting for session {self.session_name}...")

        def _start_session():
            completed = False
            while not completed:
                # Sleep 1 sec before next check.
                time.sleep(1)

                session_operation_response = self.sdk.get_session_operation(
                    sop_id, _request_timeout=30)
                session_operation = session_operation_response.result
                completed = session_operation.completed

        def _status_fn(time_elapsed: float):
            return (f"... still waiting for session {self.session_name} "
                    f"({int(time_elapsed)} seconds) ...")

        def _error_fn():
            raise SessionTimeoutError(
                f"Time out when creating session {self.session_name}")

        if not completed:
            run_with_timeout(
                _start_session,
                timeout=timeout,
                status_fn=_status_fn,
                error_fn=_error_fn)

        result = self.sdk.get_session(self.session_id)
        if not result.result.state != "Active":
            raise ReleaseTestInfraError(
                f"Cluster did not come up - most likely the nodes are currently "
                f"not available. Please check the cluster startup logs: "
                f"{anyscale_session_url(self.project_id, self.session_id)}")

    def terminate_session(self):
        if self.session_id:
            # Just trigger a request. No need to wait until session shutdown.
            self.sdk.terminate_session(
                session_id=self.session_id, terminate_session_options={})
