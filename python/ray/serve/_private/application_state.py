import traceback
import logging
import time
from typing import Dict, List, Optional, Union

import ray
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.types import ObjectRef
from ray.exceptions import RayTaskError, RuntimeEnvSetupError
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.common import (
    EndpointInfo,
    DeploymentStatus,
    ApplicationStatus,
    DeploymentStatusInfo,
    ApplicationStatusInfo,
)
from ray.serve.schema import DeploymentDetails
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.exceptions import RayServeException

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        name: str,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
        deployment_time: float = 0,
    ):
        """
        Args:
            name: application name
            deployment_state_manager: deployment state manager which is used for
                fetching deployment information
            deployment_time: Deployment timestamp
        """
        self.status: ApplicationStatus = ApplicationStatus.NOT_STARTED
        self.name = name
        self.deployment_params: List[Dict] = []
        self.ready_to_be_deleted = False
        self.deployment_state_manager = deployment_state_manager
        self.endpoint_state = endpoint_state
        if deployment_time:
            self.deployment_timestamp = deployment_time
        else:
            self.deployment_timestamp = time.time()
        self.app_msg = ""
        self.route_prefix = None
        self.docs_path = None

        # This set tracks old deployments that are being deleted
        self.deployments_to_delete = set()

    def delete_deployment(self, name):
        self.endpoint_state.delete_endpoint(name)
        self.deployment_state_manager.delete_deployment(name)

    def delete(self):
        """Delete the application"""
        self.status = ApplicationStatus.DELETING
        for deployment in self.deployments:
            self.delete_deployment(deployment)

    def set_deploy_failed(self, message):
        self.status = ApplicationStatus.DEPLOY_FAILED
        self.app_msg = message

    def deploy_deployment(
        self,
        name: str,
        deployment_config_proto_bytes: bytes,
        replica_config_proto_bytes: bytes,
        route_prefix: Optional[str],
        deployer_job_id: Union[str, bytes],
        docs_path: Optional[str] = None,
        is_driver_deployment: Optional[bool] = False,
    ) -> bool:
        """
        Deploys a deployment in the application.

        Returns: Whether the deployment is being updated.
        """

        deployment_info = deploy_args_to_deployment_info(
            deployment_name=name,
            deployment_config_proto_bytes=deployment_config_proto_bytes,
            replica_config_proto_bytes=replica_config_proto_bytes,
            deployer_job_id=deployer_job_id,
            previous_deployment=self.deployment_state_manager.get_deployment(name),
            is_driver_deployment=is_driver_deployment,
        )

        updating = self.deployment_state_manager.deploy(name, deployment_info)

        if route_prefix is not None:
            self.route_prefix = route_prefix
            endpoint_info = EndpointInfo(route=route_prefix)
            self.endpoint_state.update_endpoint(name, endpoint_info)
        else:
            self.endpoint_state.delete_endpoint(name)

        return updating

    def deploy(self, deployment_params: List[Dict]) -> List[bool]:
        """Deploy the application.

        Args:
            deployment_params: list of deployment parameters, including all deployment
            information.

        Returns: a list of booleans indicating whether each deployment is being updated
            or not.
        """
        # Update route prefix for application
        num_route_prefixes = 0
        num_docs_paths = 0
        for deploy_param in deployment_params:
            if deploy_param.get("route_prefix") is not None:
                self.route_prefix = deploy_param["route_prefix"]
                num_route_prefixes += 1

            if deploy_param.get("docs_path") is not None:
                self.docs_path = deploy_param["docs_path"]
                num_docs_paths += 1
        if num_route_prefixes > 1:
            raise RayServeException(
                f'Found multiple route prefix from application "{self.name}",'
                " Please specify only one route prefix for the application "
                "to avoid this issue."
            )
        # NOTE(zcin) This will not catch multiple FastAPI deployments in the application
        # if user sets the docs path to None in their FastAPI app.
        if num_docs_paths > 1:
            raise RayServeException(
                f'Found multiple deployments in application "{self.name}" that have '
                "a docs path. This may be due to using multiple FastAPI deployments "
                "in your application. Please only include one deployment with a docs "
                "path in your application to avoid this issue."
            )

        # Delete old deployments
        self.deployments_to_delete = set(self.deployments).difference(
            {params["name"] for params in deployment_params}
        )
        for deployment_name in self.deployments_to_delete:
            self.delete_deployment(deployment_name)
        self.deployment_params = deployment_params

        # Deploy new deployments
        updating = []
        for deployment in deployment_params:
            updating.append(self.deploy_deployment(**deployment))

        self.status = ApplicationStatus.DEPLOYING
        return updating

    def _process_terminating_deployments(self):
        """Update the tracking for all deployments being deleted

        When a deployment's status is None, the deployment will be
        removed from application.
        """
        for name in list(self.deployments_to_delete):
            if self.deployment_state_manager.get_deployment(name):
                logger.warning(
                    f"Deleting deployment {name} from application {self.name}."
                )
            else:
                self.deployments_to_delete.remove(name)

    def update(self):
        """Update the application status, maintain the ApplicationStatus.
        This method should be idempotent.

        Status:
            DEPLOYING -> RUNNING: All deployments are healthy.
            DEPLOYING -> DEPLOY_FAILED: Not all deployments are healthy.
            DELETING: Mark ready_to_be_deleted as True when all deployments are gone.
        """

        if self.ready_to_be_deleted:
            return

        if self.status == ApplicationStatus.DELETING:
            mark_delete = True
            # Application won't be deleted until all deployments get cleaned up
            for name in self.deployments:
                if self.deployment_state_manager.get_deployment(name):
                    logger.debug(
                        f"Deleting deployment {name} from application {self.name}."
                    )
                    mark_delete = False
                    break
            if self.deployments_to_delete:
                mark_delete = False
            self.ready_to_be_deleted = mark_delete
            self._process_terminating_deployments()
            return

        if self.status == ApplicationStatus.DEPLOYING:
            deployments_statuses = (
                self.deployment_state_manager.get_deployment_statuses(self.deployments)
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

            self._process_terminating_deployments()

    @property
    def deployments(self) -> List[str]:
        """Names of all deployments in application."""
        if self.deployment_params is None:
            return []
        return [params["name"] for params in self.deployment_params]

    def get_application_status_info(self) -> ApplicationStatusInfo:
        """Return the application status information"""
        return ApplicationStatusInfo(
            self.status,
            message=self.app_msg,
            deployment_timestamp=self.deployment_timestamp,
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
            name: self.deployment_state_manager.get_deployment_details(name)
            for name in self.deployments
        }
        return {k: v for k, v in details.items() if v is not None}


class ApplicationStateManager:
    def __init__(
        self,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
    ):
        self.deployment_state_manager = deployment_state_manager
        self.endpoint_state = endpoint_state
        self._application_states: Dict[str, ApplicationState] = {}
        self._build_app_obj_refs: Dict[str, ObjectRef] = {}

    def get_deployments(self, app_name: str) -> List[str]:
        """Return all deployment names by app name"""
        if app_name not in self._application_states:
            return []
        return self._application_states[app_name].deployments

    def get_deployments_statuses(self, app_name: str) -> List[DeploymentStatusInfo]:
        """Return all deployment statuses by app name"""
        if app_name not in self._application_states:
            return []
        return self.deployment_state_manager.get_deployment_statuses(
            self._application_states[app_name].deployments
        )

    def get_app_status(self, name: str) -> ApplicationStatus:
        if name not in self._application_states:
            return ApplicationStatus.NOT_STARTED

        return self._application_states[name].status

    def get_app_status_info(self, name: str) -> ApplicationStatusInfo:
        if name not in self._application_states:
            return ApplicationStatusInfo(
                ApplicationStatus.NOT_STARTED,
                message=f"Application {name} doesn't exist",
                deployment_timestamp=0,
            )
        return self._application_states[name].get_application_status_info()

    def get_docs_path(self, app_name: str):
        return self._application_states[app_name].docs_path

    def get_route_prefix(self, name: str) -> str:
        return self._application_states[name].route_prefix

    def get_deployment_timestamp(self, name: str) -> float:
        if name not in self._application_states:
            return -1
        return self._application_states[name].deployment_timestamp

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
        build_app_obj_ref: ObjectRef,
        deployment_time: float = 0,
    ):
        """
        Create application state.

        This is used for holding the build_app_obj_ref, which is the object ref for the
        build_application task.
        """
        if name in self._application_states and name in self._build_app_obj_refs:
            logger.info(
                f"Received new config deployment for {name} request. Cancelling "
                "previous request."
            )
            ray.cancel(self._build_app_obj_refs[name])

        self._build_app_obj_refs[name] = build_app_obj_ref
        self._application_states[name] = ApplicationState(
            name,
            self.deployment_state_manager,
            self.endpoint_state,
            deployment_time=deployment_time,
        )

    def deploy_application(
        self, name: str, deployment_args_list: List[Dict]
    ) -> List[bool]:
        """Deploy single application

        Args:
            name: application name
            deployment_args: deployment arguments needed for deploying a list of d
                deployments.
        Returns:
            Returns a list indicating whether each deployment is being updated.
        """

        # Make sure route_prefix is not being used by other application.
        live_route_prefixes: Dict[str, str] = {
            self._application_states[app_name].route_prefix: app_name
            for app_name, app_state in self._application_states.items()
            if app_state.route_prefix is not None
            and not app_state.status == ApplicationStatus.DELETING
            and name != app_name
        }

        for deploy_param in deployment_args_list:
            deploy_app_prefix = deploy_param.get("route_prefix")
            if deploy_app_prefix in live_route_prefixes:
                raise RayServeException(
                    f"Prefix {deploy_app_prefix} is being used by application "
                    f'"{live_route_prefixes[deploy_app_prefix]}".'
                    f' Failed to deploy application "{name}".'
                )

        if name not in self._application_states:
            self._application_states[name] = ApplicationState(
                name,
                self.deployment_state_manager,
                self.endpoint_state,
            )

        record_extra_usage_tag(
            TagKey.SERVE_NUM_APPS, str(len(self._application_states))
        )

        return self._application_states[name].deploy(deployment_args_list)

    def delete_application(self, name: str):
        """Delete application by name"""
        if name not in self._application_states:
            return
        self._application_states[name].delete()

    def update(self):
        """Update each application state"""
        apps_to_be_deleted = []
        for name, app in self._application_states.items():
            app.update()
            if app.ready_to_be_deleted:
                apps_to_be_deleted.append(name)

        for name, build_app_obj_ref in list(self._build_app_obj_refs.items()):
            finished, _ = ray.wait([build_app_obj_ref], timeout=0)
            if finished:
                try:
                    deployment_args_list = ray.get(finished[0])
                    logger.info(f"Build task for app '{name}' ran successfully.")

                    try:
                        self.deploy_application(name, deployment_args_list)
                    except RayServeException:
                        message = (
                            f"Deploying app '{name}' failed:\n{traceback.format_exc()}"
                        )
                        self._application_states[name].set_deploy_failed(message=message)
                        logger.warning(message)
                    except Exception:
                        message = (
                            f"Deploying app '{name}' failed with an unexpected error:\n"
                            f"{traceback.format_exc()}"
                        )
                        self._application_states[name].set_deploy_failed(message=message)
                        logger.warning(message)
                except RayTaskError as e:
                    # NOTE(zcin): we should use str(e) instead of traceback.format_exc()
                    # here because the full details of the error is not displayed
                    # properly with traceback.format_exc(). RayTaskError has its own
                    # custom __str__ function.
                    message = f"Deploying app '{name}' failed:\n{str(e)}"
                    self._application_states[name].set_deploy_failed(message=message)
                    logger.warning(message)
                except RuntimeEnvSetupError:
                    message = (
                        f"Runtime env setup for app '{name}' "
                        f"failed:\n{traceback.format_exc()}"
                    )
                    self._application_states[name].set_deploy_failed(message=message)
                    logger.warning(message)

                del self._build_app_obj_refs[name]

        if len(apps_to_be_deleted) > 0:
            for app_name in apps_to_be_deleted:
                del self._application_states[app_name]
            record_extra_usage_tag(
                TagKey.SERVE_NUM_APPS, str(len(self._application_states))
            )
