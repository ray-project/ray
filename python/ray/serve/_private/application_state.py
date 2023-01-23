import ray
import traceback
from typing import Dict, List
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.common import (
    DeploymentStatus,
    DeploymentStatusInfo,
    ApplicationStatusInfo,
)
import time
from ray.exceptions import RayTaskError, RuntimeEnvSetupError
import logging
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.types import ObjectRef

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        app_name: str,
        deployment_state_manager: DeploymentStateManager,
        deployment_params: List[Dict] = None,
        deploy_obj_ref: ObjectRef = None,
        deployment_time: float = 0,
    ):
        """
        Args:
            app_name: application name
            deployment_state_manager: deployment state manager which is used for
                fetching deployment information
            deployment_params: all deployment parameters to deploy.
            deploy_obj_ref: Task ObjRef of deploying application.
            deployment_time: Deployment timestamp
        """
        if deploy_obj_ref:
            self.status: ApplicationStatus = ApplicationStatus.DEPLOYING
        else:
            self.status: ApplicationStatus = ApplicationStatus.NOT_STARTED
        self.app_name = app_name
        self.deployment_params = deployment_params
        self.to_be_deleted = False
        self.deployment_state_manager = deployment_state_manager
        if deployment_time:
            self.deployment_timestamp = deployment_time
        else:
            self.deployment_timestamp = time.time()
        self.deploy_obj_ref = deploy_obj_ref
        self.app_msg = ""

    def delete(self):
        """Delete the application"""
        self.status = ApplicationStatus.DELETING

    def deploy(self):
        """Deploy the application"""
        self.status = ApplicationStatus.DEPLOYING

    def update(self):
        """Update the application status, maintain the ApplicationStatus.
        This method should be idempotent.

        Status:
            DEPLOYING -> RUNNING: All deployments are healthy.
            DEPLOYING -> DEPLOY_FAILED: Not all deployments are healthy.
            DELETING: Mark to_be_deleted as True when all deployments are gone.
        """

        if self.to_be_deleted:
            return

        if self.status == ApplicationStatus.DELETING:
            mark_delete = True
            for name in self.get_all_deployments():
                if self.deployment_state_manager.get_deployment(name):
                    mark_delete = False
                    break
            self.to_be_deleted = mark_delete
            return

        if self.status == ApplicationStatus.DEPLOYING:
            if self.deploy_obj_ref:
                finished, pending = ray.wait([self.deploy_obj_ref], timeout=0)
                if pending:
                    return
                try:
                    ray.get(finished[0])
                except RayTaskError:
                    self.status = ApplicationStatus.DEPLOY_FAILED
                    self.app_msg = f"Deployment failed:\n{traceback.format_exc()}"
                    self.deploy_obj_ref = None
                    return
                except RuntimeEnvSetupError:
                    self.status = ApplicationStatus.DEPLOY_FAILED
                    self.app_msg = (
                        f"Runtime env setup failed:\n{traceback.format_exc()}"
                    )
                    self.deploy_obj_ref = None
                    return
            deployments_statuses = (
                self.deployment_state_manager.get_deployment_statuses(
                    self.get_all_deployments()
                )
            )
            num_health_deployments = 0
            for deployment_status in deployments_statuses:
                if deployment_status.status == DeploymentStatus.UNHEALTHY:
                    self.status = ApplicationStatus.DEPLOY_FAILED
                    return
                if deployment_status.status == DeploymentStatus.HEALTHY:
                    num_health_deployments += 1
            if num_health_deployments == len(deployments_statuses):
                self.status = ApplicationStatus.RUNNING

    def get_all_deployments(self) -> List[str]:
        """Return all deployments name from the application"""
        if self.deployment_params is None:
            return []
        return [params["name"] for params in self.deployment_params]

    def get_deployments_statuses(self) -> List[DeploymentStatusInfo]:
        """Return all deployment status information"""
        return self.deployment_state_manager.get_deployment_statuses(
            self.get_all_deployments()
        )

    def get_application_status_info(self) -> ApplicationStatusInfo:
        """Return the application status information"""
        return ApplicationStatusInfo(
            self.status,
            message=self.app_msg,
            deployment_timestamp=self.deployment_timestamp,
        )


class ApplicationStateManager:
    def __init__(self, deployment_state_manager):
        self.deployment_state_manager = deployment_state_manager
        self._application_states: Dict[str, ApplicationState] = {}

    def delete_application(self, name: str):
        """Delete application by name"""
        if name not in self._application_states:
            return
        self._application_states[name].delete()

    def deploy_application(self, name: str, deployment_args: List[Dict]):
        """Deploy single application

        Args:
            name: application name
            deployment_args: deployment args
        """
        if name in self._application_states:
            self._application_states[name].deployment_params = deployment_args
        else:
            self._application_states[name] = ApplicationState(
                name,
                self.deployment_state_manager,
                deployment_args,
            )
        self._application_states[name].deploy()

    def get_deployments(self, app_name: str) -> List[str]:
        """Return all deployment names by app name"""
        if app_name not in self._application_states:
            return []
        return self._application_states[app_name].get_all_deployments()

    def get_deployments_statuses(self, app_name: str) -> List[DeploymentStatusInfo]:
        """Return all deployment statuses by app name"""
        if app_name not in self._application_states:
            return []
        return self._application_states[app_name].get_deployments_statuses()

    def get_app_status(self, name: str) -> ApplicationStatusInfo:
        if name not in self._application_states:
            return ApplicationStatusInfo(
                ApplicationStatus.NOT_STARTED,
                message=f"Application {name} doesn't exist",
                deployment_timestamp=0,
            )
        return self._application_states[name].get_application_status_info()

    def create_application_state(
        self, name: str, deploy_obj_ref: ObjectRef, deployment_time: float = 0
    ):
        """Create application state
        This is used for holding the deploy_obj_ref which is created by run_graph method
        """
        if (
            name in self._application_states
            and self._application_states[name].deploy_obj_ref
        ):
            logger.info(
                f"Received new config deployment for {name} request. Cancelling "
                "previous request."
            )
            ray.cancel(self._application_states[name].deploy_obj_ref)
        self._application_states[name] = ApplicationState(
            name,
            self.deployment_state_manager,
            deploy_obj_ref=deploy_obj_ref,
            deployment_time=deployment_time,
        )

    def get_deployment_timestamp(self, name: str) -> float:
        if name not in self._application_states:
            return -1
        return self._application_states[name].deployment_timestamp

    def update(self):
        """Update each application state"""
        apps_to_be_deleted = []
        for name, app in self._application_states.items():
            app.update()
            if app.to_be_deleted:
                apps_to_be_deleted.append(name)
        for app_name in apps_to_be_deleted:
            del self._application_states[app_name]
