import traceback
from typing import Dict, List, Optional
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.common import (
    DeploymentStatus,
    DeploymentStatusInfo,
    ApplicationStatusInfo,
)
from ray.serve.schema import DeploymentDetails
import time
from ray.exceptions import RayTaskError, RuntimeEnvSetupError
import logging
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.types import ObjectRef
import ray
from ray.serve.exceptions import RayServeException

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        name: str,
        deployment_state_manager: DeploymentStateManager,
        deploy_obj_ref: ObjectRef = None,
        deployment_time: float = 0,
    ):
        """
        Args:
            name: application name
            deployment_state_manager: deployment state manager which is used for
                fetching deployment information
            deploy_obj_ref: Task ObjRef of deploying application.
            deployment_time: Deployment timestamp
        """

        self._name = name
        self._deploy_obj_ref = deploy_obj_ref
        self._app_msg = ""
        self._deployment_state_manager = deployment_state_manager
        self._deployment_params: List[Dict] = []
        # This set tracks old deployments that are being deleted
        self._deployments_to_delete = set()
        self._ready_to_be_deleted = False
        self._route_prefix = None
        self._docs_path = None

        if deploy_obj_ref:
            self._status: ApplicationStatus = ApplicationStatus.DEPLOYING
        else:
            self._status: ApplicationStatus = ApplicationStatus.NOT_STARTED
        if deployment_time:
            self._deployment_timestamp = deployment_time
        else:
            self._deployment_timestamp = time.time()

    @property
    def ready_to_be_deleted(self) -> bool:
        return self._ready_to_be_deleted

    @property
    def route_prefix(self) -> Optional[str]:
        return self._route_prefix

    @property
    def docs_path(self) -> Optional[str]:
        return self._docs_path

    @property
    def status(self) -> ApplicationStatus:
        return self._status

    @property
    def deployment_timestamp(self) -> int:
        return self._deployment_timestamp

    @property
    def deploy_obj_ref(self) -> Optional[ObjectRef]:
        return self._deploy_obj_ref

    @property
    def deployments(self) -> List[str]:
        """Return all deployments name from the application"""
        if self._deployment_params is None:
            return []
        return [params["name"] for params in self._deployment_params]

    def delete(self):
        """Delete the application"""
        self._status = ApplicationStatus.DELETING

    def deploy(self, deployment_params: List[Dict]) -> List[str]:
        """Deploy the application.

        Args:
            deployment_params: list of deployment parameters, including all deployment
            information.

        Returns: list of deployments which should be deleted.
        """

        # Filter deployments from current deployment_params
        # that are not used in the new deployment_params
        to_be_deployed_deployments = {params["name"] for params in deployment_params}
        cur_deployments_to_delete = []
        for deployment_name in self.deployments:
            if deployment_name not in to_be_deployed_deployments:
                cur_deployments_to_delete.append(deployment_name)
                self._deployments_to_delete.add(deployment_name)
        self._deployment_params = deployment_params

        # Update route prefix for application
        num_route_prefixes = 0
        num_docs_paths = 0
        for deploy_param in deployment_params:
            if deploy_param.get("route_prefix") is not None:
                self._route_prefix = deploy_param["route_prefix"]
                num_route_prefixes += 1

            if deploy_param.get("docs_path") is not None:
                self._docs_path = deploy_param["docs_path"]
                num_docs_paths += 1
        if num_route_prefixes > 1:
            raise RayServeException(
                f'Found multiple route prefix from application "{self._name}",'
                " Please specify only one route prefix for the application "
                "to avoid this issue."
            )
        # NOTE(zcin) This will not catch multiple FastAPI deployments in the application
        # if user sets the docs path to None in their FastAPI app.
        if num_docs_paths > 1:
            raise RayServeException(
                f'Found multiple deployments in application "{self._name}" that have '
                "a docs path. This may be due to using multiple FastAPI deployments "
                "in your application. Please only include one deployment with a docs "
                "path in your application to avoid this issue."
            )

        self._status = ApplicationStatus.DEPLOYING
        return cur_deployments_to_delete

    def update_obj_ref(self, deploy_obj_ref: ObjectRef, deployment_time: int):
        self._deploy_obj_ref = deploy_obj_ref
        self._deployment_timestamp = deployment_time
        self._status = ApplicationStatus.DEPLOYING

    def _process_terminating_deployments(self):
        """Update the tracking for all deployments being deleted

        When a deployment's status is None, the deployment will be
        removed from application.
        """
        for name in list(self._deployments_to_delete):
            if self._deployment_state_manager.get_deployment(name):
                logger.warning(
                    f"Deleting deployment {name} from application {self._name}."
                )
            else:
                self._deployments_to_delete.remove(name)

    def update(self):
        """Update the application status, maintain the ApplicationStatus.
        This method should be idempotent.

        Status:
            DEPLOYING -> RUNNING: All deployments are healthy.
            DEPLOYING -> DEPLOY_FAILED: Not all deployments are healthy.
            DELETING: Mark ready_to_be_deleted as True when all deployments are gone.
        """

        if self._ready_to_be_deleted:
            return

        if self._status == ApplicationStatus.DELETING:
            mark_delete = True
            # Application won't be deleted until all deployments get cleaned up
            for name in self.deployments:
                if self._deployment_state_manager.get_deployment(name):
                    logger.debug(
                        f"Deleting deployment {name} from application {self._name}."
                    )
                    mark_delete = False
                    break
            if self._deployments_to_delete:
                mark_delete = False
            self._ready_to_be_deleted = mark_delete
            self._process_terminating_deployments()
            return

        if self._status == ApplicationStatus.DEPLOYING:
            if self._deploy_obj_ref:
                finished, pending = ray.wait([self._deploy_obj_ref], timeout=0)
                if pending:
                    return
                try:
                    ray.get(finished[0])
                    logger.info(f"Deploy task for app '{self._name}' ran successfully.")
                except RayTaskError as e:
                    self._status = ApplicationStatus.DEPLOY_FAILED
                    # NOTE(zcin): we should use str(e) instead of traceback.format_exc()
                    # here because the full details of the error is not displayed
                    # properly with traceback.format_exc(). RayTaskError has its own
                    # custom __str__ function.
                    self._app_msg = f"Deploying app '{self._name}' failed:\n{str(e)}"
                    self._deploy_obj_ref = None
                    logger.warning(self._app_msg)
                    return
                except RuntimeEnvSetupError:
                    self._status = ApplicationStatus.DEPLOY_FAILED
                    self._app_msg = (
                        f"Runtime env setup for app '{self._name}' "
                        f"failed:\n{traceback.format_exc()}"
                    )
                    self._deploy_obj_ref = None
                    logger.warning(self._app_msg)
                    return
            deployments_statuses = (
                self._deployment_state_manager.get_deployment_statuses(self.deployments)
            )
            num_health_deployments = 0
            for deployment_status in deployments_statuses:
                if deployment_status.status == DeploymentStatus.UNHEALTHY:
                    self._status = ApplicationStatus.DEPLOY_FAILED
                    return
                if deployment_status.status == DeploymentStatus.HEALTHY:
                    num_health_deployments += 1
            if num_health_deployments == len(deployments_statuses):
                self._status = ApplicationStatus.RUNNING

            self._process_terminating_deployments()

    def get_deployments_statuses(self) -> List[DeploymentStatusInfo]:
        """Return all deployment status information"""
        return self._deployment_state_manager.get_deployment_statuses(self.deployments)

    def get_application_status_info(self) -> ApplicationStatusInfo:
        """Return the application status information"""
        return ApplicationStatusInfo(
            self._status,
            message=self._app_msg,
            deployment_timestamp=self._deployment_timestamp,
        )

    def list_deployment_details(self) -> Dict[str, DeploymentDetails]:
        """Gets detailed info on all live deployments in this application.
        (Does not include deleted deployments.)

        Returns: a dictionary of deployment info. The set of deployment info returned
            may not be the full list of deployments that are part of the application.
            This can happen when the application is still deploying and bringing up
            deployments, or when the application is deleting and some deployments have
            been deleted.
        """
        details = {
            name: self._deployment_state_manager.get_deployment_details(name)
            for name in self.deployments
        }
        return {k: v for k, v in details.items() if v is not None}


class ApplicationStateManager:
    def __init__(self, deployment_state_manager):
        self._deployment_state_manager = deployment_state_manager
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
            deployment_args: deployment arguments needed for
                deploying a list of Deployments.
        Returns:
            Return a list of deployments to be deleted.
        """

        # Make sure route_prefix is not being used by other application.
        live_route_prefixes: Dict[str, str] = {
            self._application_states[app_name].route_prefix: app_name
            for app_name, app_state in self._application_states.items()
            if app_state.route_prefix is not None
            and not app_state.status == ApplicationStatus.DELETING
        }
        for deploy_param in deployment_args:
            if "route_prefix" in deploy_param:
                deploy_app_prefix = deploy_param["route_prefix"]
                if (
                    deploy_app_prefix
                    and deploy_app_prefix in live_route_prefixes
                    and name != live_route_prefixes[deploy_app_prefix]
                ):
                    raise RayServeException(
                        f"Prefix {deploy_app_prefix} is being used by application "
                        f'"{live_route_prefixes[deploy_app_prefix]}".'
                        f' Failed to deploy application "{name}".'
                    )

        if name not in self._application_states:
            self._application_states[name] = ApplicationState(
                name,
                self._deployment_state_manager,
            )
        record_extra_usage_tag(
            TagKey.SERVE_NUM_APPS, str(len(self._application_states))
        )
        return self._application_states[name].deploy(deployment_args)

    def get_deployments(self, app_name: str) -> List[str]:
        """Return all deployment names by app name"""
        if app_name not in self._application_states:
            return []
        return self._application_states[app_name].deployments

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

    def get_deployment_timestamp(self, name: str) -> float:
        if name not in self._application_states:
            return -1
        return self._application_states[name].deployment_timestamp

    def get_docs_path(self, app_name: str):
        return self._application_states[app_name].docs_path

    def get_route_prefix(self, name: str) -> str:
        return self._application_states[name].route_prefix

    def list_app_statuses(self) -> Dict[str, ApplicationStatusInfo]:
        """Return a dictionary with {app name: application info}"""
        return {
            name: self._application_states[name].get_application_status_info()
            for name in self._application_states
        }

    def list_deployment_details(self, name: str) -> Dict[str, DeploymentDetails]:
        """Gets detailed info on all deployments in specified application."""
        if name not in self._application_states:
            return {}
        return self._application_states[name].list_deployment_details()

    def create_application_state(
        self,
        name: str,
        deploy_obj_ref: ObjectRef,
        deployment_time: float = 0,
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
        if name in self._application_states:
            self._application_states[name].update_obj_ref(
                deploy_obj_ref,
                deployment_time,
            )
        else:
            self._application_states[name] = ApplicationState(
                name,
                self._deployment_state_manager,
                deploy_obj_ref=deploy_obj_ref,
                deployment_time=deployment_time,
            )

    def update(self):
        """Update each application state"""
        apps_to_be_deleted = []
        for name, app in self._application_states.items():
            app.update()
            if app.ready_to_be_deleted:
                apps_to_be_deleted.append(name)

        if len(apps_to_be_deleted) > 0:
            for app_name in apps_to_be_deleted:
                del self._application_states[app_name]
            record_extra_usage_tag(
                TagKey.SERVE_NUM_APPS, str(len(self._application_states))
            )
