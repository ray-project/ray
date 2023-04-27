from dataclasses import dataclass
import traceback
import logging
import time
from typing import Dict, List, Optional
from collections import Counter

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
    DeploymentInfo,
)
from ray.serve.schema import DeploymentDetails
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.exceptions import RayServeException

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class ApplicationTargetState:
    deployment_params: Optional[Dict]
    deleting: bool


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        name: str,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
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
        if deploy_obj_ref:
            self.status: ApplicationStatus = ApplicationStatus.DEPLOYING
        else:
            self.status: ApplicationStatus = ApplicationStatus.NOT_STARTED
        self._name = name
        self.ready_to_be_deleted = False
        self.deployment_state_manager = deployment_state_manager
        self.endpoint_state = endpoint_state
        if deployment_time:
            self.deployment_timestamp = deployment_time
        else:
            self.deployment_timestamp = time.time()
        self.deploy_obj_ref = deploy_obj_ref
        self.app_msg = ""
        self.route_prefix = None
        self.docs_path = None

        self._target_state: ApplicationTargetState = ApplicationTargetState(
            deployment_params=None, deleting=False
        )

    def _delete_deployment(self, name):
        self.endpoint_state.delete_endpoint(name)
        self.deployment_state_manager.delete_deployment(name)

    def delete(self):
        """Delete the application"""
        logger.info(f"Deleting application '{self._name}'")
        self._target_state = ApplicationTargetState(deployment_params=[], deleting=True)
        self.status = ApplicationStatus.DELETING

    def apply_deployment(
        self,
        deployment_name: str,
        route_prefix: Optional[str],
        deployment_info: DeploymentInfo,
    ) -> bool:
        """
        Deploys a deployment in the application.

        Returns: Whether the deployment is being updated.
        """
        if route_prefix is not None:
            assert route_prefix.startswith("/")

        updating = self.deployment_state_manager.deploy(
            deployment_name, deployment_info
        )

        if route_prefix is not None:
            self.route_prefix = route_prefix
            endpoint_info = EndpointInfo(route=route_prefix)
            self.endpoint_state.update_endpoint(deployment_name, endpoint_info)
        else:
            self.endpoint_state.delete_endpoint(deployment_name)

        return updating

    def apply_deployment_args(self, deployment_params: List[Dict]) -> List[bool]:
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

        self._target_state = ApplicationTargetState(
            deployment_params=deployment_params, deleting=False
        )
        return [True for _ in deployment_params]

    def update_obj_ref(self, deploy_obj_ref: ObjectRef, deployment_time: int):
        self.deploy_obj_ref = deploy_obj_ref
        self.deployment_timestamp = deployment_time
        logger.info(f"application status is now deploying {time.time()}")

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

        # Deal with deploy obj ref (should only be done once)
        if self.status == ApplicationStatus.DEPLOYING:
            if self.deploy_obj_ref:
                finished, _ = ray.wait([self.deploy_obj_ref], timeout=0)
                if finished:
                    try:
                        ray.get(finished[0])
                        logger.info(
                            f"Deploy task for app '{self._name}' ran successfully."
                        )
                        self.deploy_obj_ref = None
                    except RayTaskError as e:
                        self.status = ApplicationStatus.DEPLOY_FAILED
                        # NOTE(zcin): we should use str(e) not traceback.format_exc()
                        # here because the full details of the error is not displayed
                        # properly with traceback.format_exc(). RayTaskError has its own
                        # custom __str__ function.
                        self.app_msg = f"Deploying app '{self._name}' failed:\n{str(e)}"
                        self.deploy_obj_ref = None
                        logger.warning(self.app_msg)
                        return
                    except RuntimeEnvSetupError:
                        self.status = ApplicationStatus.DEPLOY_FAILED
                        self.app_msg = (
                            f"Runtime env setup for app '{self._name}' "
                            f"failed:\n{traceback.format_exc()}"
                        )
                        self.deploy_obj_ref = None
                        logger.warning(self.app_msg)
                        return

        if self._target_state.deployment_params is None:
            return

        # === Reconciliation Loop ================================
        # Deploy/update deployments
        for params in self._target_state.deployment_params:
            deployment_name = params["name"]
            previous_deployment = self.get_deployment(deployment_name)
            deployment_info = deploy_args_to_deployment_info(
                app_name=self._name,
                previous_deployment=previous_deployment,
                **params,
            )
            self.apply_deployment(
                deployment_name, params["route_prefix"], deployment_info
            )

        # Delete deployments
        live_deployments = self.deployment_state_manager.get_deployments_in_application(
            self._name
        )
        for deployment_name in live_deployments:
            if deployment_name not in self.deployments:
                self._delete_deployment(deployment_name)
        if self._target_state.deleting:
            if len(live_deployments) == 0:
                self.ready_to_be_deleted = True

        # Check current status
        if self.status != ApplicationStatus.DELETING:
            deployment_statuses = Counter(
                info.status
                for info in self.deployment_state_manager.get_deployment_statuses(
                    self.deployments
                )
            )
            # print("deployment_statuses:", deployment_statuses)
            if deployment_statuses[DeploymentStatus.UNHEALTHY]:
                if self.status == ApplicationStatus.DEPLOYING:
                    self.status = ApplicationStatus.DEPLOY_FAILED
                else:
                    self.status = ApplicationStatus.ERRORED
            elif deployment_statuses[DeploymentStatus.UPDATING]:
                # print("application status is now DEPLOYING", time.time())
                self.status = ApplicationStatus.DEPLOYING
            elif deployment_statuses[DeploymentStatus.HEALTHY] == len(self.deployments):
                # print("application status is now RUNNING", time.time())
                self.status = ApplicationStatus.RUNNING

    @property
    def deployments(self) -> List[str]:
        """Names of all deployments in application."""
        return [params["name"] for params in self._target_state.deployment_params]

    def get_deployment(self, name: str) -> DeploymentInfo:
        """Get deployment info for deployment by name."""
        return self.deployment_state_manager.get_deployment(name)

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

    def delete_application(self, name: str):
        """Delete application by name"""
        if name not in self._application_states:
            return
        self._application_states[name].delete()

    def deploy_application(
        self, name: str, deployment_args_list: List[Dict]
    ) -> List[bool]:
        """Deploy single application

        Args:
            name: application name
            deployment_args_list: deployment arguments needed for deploying a list of
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
        return self._application_states[name].apply_deployment_args(
            deployment_args_list
        )

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
                self.deployment_state_manager,
                endpoint_state=self.endpoint_state,
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

    def shutdown(self):
        for app_state in self._application_states.values():
            app_state.delete()
