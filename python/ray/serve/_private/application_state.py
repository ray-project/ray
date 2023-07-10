from dataclasses import dataclass
from enum import Enum
import traceback
from typing import Dict, List, Optional, Callable, Tuple
import logging

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.storage.kv_store import KVStoreBase
from ray.serve._private.common import (
    DeploymentStatus,
    DeploymentStatusInfo,
    ApplicationStatusInfo,
    ApplicationStatus,
    EndpointInfo,
    DeploymentInfo,
)
from ray.serve.schema import DeploymentDetails
import time
from ray.exceptions import RuntimeEnvSetupError
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info
from ray.serve._private.utils import check_obj_ref_ready_nowait
from ray.types import ObjectRef
import ray
from ray import cloudpickle
from ray.serve.exceptions import RayServeException

logger = logging.getLogger(SERVE_LOGGER_NAME)

CHECKPOINT_KEY = "serve-application-state-checkpoint"


class BuildAppStatus(Enum):
    IN_PROGRESS = 1
    SUCCEEDED = 2
    FAILED = 3


@dataclass
class ApplicationTargetState:
    """Maps deployment names to deployment infos."""

    deployment_infos: Optional[Dict[str, DeploymentInfo]]
    deleting: bool


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        name: str,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
        save_checkpoint_func: Callable,
        deploy_obj_ref: ObjectRef = None,
        deployment_time: float = 0,
    ):
        """
        Args:
            name: Application name.
            deployment_state_manager: State manager for all deployments
                in the cluster.
            endpoint_state: State manager for endpoints in the system.
            save_checkpoint_func: Function that can be called to write
                a checkpoint of the application state. This should be
                called in self._set_target_state() before actually
                setting the target state so that the controller can
                properly recover application states if it crashes.
            deploy_obj_ref: Task ObjRef of deploying application.
            deployment_time: Deployment timestamp.
        """

        self._name = name
        self._deploy_obj_ref = deploy_obj_ref
        self._status_msg = ""
        self._deployment_state_manager = deployment_state_manager
        self._endpoint_state = endpoint_state
        self._route_prefix = None
        self._docs_path = None

        self._status: ApplicationStatus = ApplicationStatus.DEPLOYING
        if deployment_time:
            self._deployment_timestamp = deployment_time
        else:
            self._deployment_timestamp = time.time()

        # Before a deploy app task finishes, we don't know what the
        # target deployments are, so set deployment_infos=None
        self._target_state: ApplicationTargetState = ApplicationTargetState(
            deployment_infos=None, deleting=False
        )
        self._save_checkpoint_func = save_checkpoint_func

    @property
    def route_prefix(self) -> Optional[str]:
        return self._route_prefix

    @property
    def docs_path(self) -> Optional[str]:
        return self._docs_path

    @property
    def status(self) -> ApplicationStatus:
        """Status of the application.

        DEPLOYING: The deploy task is still running, or the deployments
            have started deploying but aren't healthy yet.
        RUNNING: All deployments are healthy.
        DEPLOY_FAILED: The deploy task failed or one or more deployments
            became unhealthy in the process of deploying
        UNHEALTHY: While the application was running, one or more
            deployments transition from healthy to unhealthy.
        DELETING: Application and its deployments are being deleted.
        """
        return self._status

    @property
    def deployment_timestamp(self) -> int:
        return self._deployment_timestamp

    @property
    def deploy_obj_ref(self) -> Optional[ObjectRef]:
        return self._deploy_obj_ref

    @property
    def target_deployments(self) -> List[str]:
        """List of target deployment names in application."""
        if self._target_state.deployment_infos is None:
            return []
        return list(self._target_state.deployment_infos.keys())

    def recover_target_state_from_checkpoint(
        self, checkpoint_data: ApplicationTargetState
    ):
        logger.info(
            f"Recovering target state for application '{self._name}' from checkpoint."
        )
        self._set_target_state(
            checkpoint_data.deployment_infos, checkpoint_data.deleting
        )

    def _set_target_state(
        self,
        deployment_infos: Optional[Dict[str, DeploymentInfo]] = None,
        deleting: bool = False,
    ):
        """Set application target state.

        While waiting for deploy task to finish, this should be
            (None, False)
        When deploy task has finished and during normal operation, this should be
            (target_deployments, False)
        When a request to delete the application has been received, this should be
            ({}, True)
        """

        if deleting:
            target_state = ApplicationTargetState(dict(), True)
            self._update_status(ApplicationStatus.DELETING)
        else:
            target_state = ApplicationTargetState(deployment_infos, False)
            self._update_status(ApplicationStatus.DEPLOYING)

        # Checkpoint ahead, so that if the controller crashes before we
        # write to the target state, the target state will be recovered
        # after the controller recovers
        self._save_checkpoint_func(writeahead_checkpoints={self._name: target_state})
        # Set target state
        self._target_state = target_state

    def _delete_deployment(self, name):
        self._endpoint_state.delete_endpoint(name)
        self._deployment_state_manager.delete_deployment(name)

    def delete(self):
        """Delete the application"""
        logger.info(
            f"Deleting application '{self._name}'",
            extra={"log_to_stderr": False},
        )
        self._set_target_state(deleting=True)

    def is_deleted(self) -> bool:
        """Check whether the application is already deleted.

        For an application to be considered deleted, the target state has to be set to
        deleting and all deployments have to be deleted.
        """
        return self._target_state.deleting and len(self._get_live_deployments()) == 0

    def apply_deployment_info(
        self, deployment_name: str, deployment_info: DeploymentInfo
    ) -> None:
        """Deploys a deployment in the application."""
        route_prefix = deployment_info.route_prefix
        if route_prefix is not None and not route_prefix.startswith("/"):
            raise RayServeException(
                f'Invalid route prefix "{route_prefix}", it must start with "/"'
            )

        self._deployment_state_manager.deploy(deployment_name, deployment_info)

        if deployment_info.route_prefix is not None:
            config = deployment_info.deployment_config
            self._endpoint_state.update_endpoint(
                deployment_name,
                EndpointInfo(
                    route=deployment_info.route_prefix,
                    app_name=self._name,
                    app_is_cross_language=config.is_cross_language,
                ),
            )
        else:
            self._endpoint_state.delete_endpoint(deployment_name)

    def apply_deployment_args(self, deployment_params: List[Dict]) -> None:
        """Set the list of deployment infos in application target state.

        Args:
            deployment_params: list of deployment parameters, including
                all deployment information.

        Raises:
            RayServeException: If there is more than one deployment with
                a non-null route prefix or docs path.
        """
        # Makes sure that at most one deployment has a non-null route
        # prefix and docs path.
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
                f'Found multiple route prefixes from application "{self._name}",'
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

        for params in deployment_params:
            params["deployment_name"] = params.pop("name")
            params["app_name"] = self._name

        deployment_infos = {
            params["deployment_name"]: deploy_args_to_deployment_info(**params)
            for params in deployment_params
        }
        self._set_target_state(deployment_infos=deployment_infos)

    def update_obj_ref(self, deploy_obj_ref: ObjectRef, deployment_time: int) -> None:
        """Update deploy object ref.

        Updates the deployment timestamp, sets status to DEPLOYING, and
        wipes the current target state. While the task to build the
        application is running, the application state stops the
        reconciliation loop and waits on the task to finish and call
        apply_deployments_args().
        """

        if self._deploy_obj_ref:
            logger.info(
                f'Received new config for application "{self._name}". '
                "Cancelling previous request."
            )
            ray.cancel(self._deploy_obj_ref)

        self._deploy_obj_ref = deploy_obj_ref
        self._deployment_timestamp = deployment_time
        # Halt reconciliation of target deployments
        self._set_target_state(deployment_infos=None)

    def _get_live_deployments(self) -> List[str]:
        return self._deployment_state_manager.get_deployments_in_application(self._name)

    def _determine_app_status(self) -> Tuple[ApplicationStatus, str]:
        """Check deployment statuses and target state, and determine the
        corresponding application status.

        Returns:
            Status (ApplicationStatus):
                RUNNING: all deployments are healthy.
                DEPLOYING: there is one or more updating deployments,
                    and there are no unhealthy deployments.
                DEPLOY_FAILED: one or more deployments became unhealthy
                    while the application was deploying.
                UNHEALTHY: one or more deployments became unhealthy
                    while the application was running.
                DELETING: the application is being deleted.
            Error message (str):
                Non-empty string if status is DEPLOY_FAILED or UNHEALTHY
        """

        if self._target_state.deleting:
            return ApplicationStatus.DELETING, ""

        num_healthy_deployments = 0
        unhealthy_deployment_names = []

        for deployment_status in self.get_deployments_statuses():
            if deployment_status.status == DeploymentStatus.UNHEALTHY:
                unhealthy_deployment_names.append(deployment_status.name)
            if deployment_status.status == DeploymentStatus.HEALTHY:
                num_healthy_deployments += 1

        if num_healthy_deployments == len(self.target_deployments):
            return ApplicationStatus.RUNNING, ""
        elif len(unhealthy_deployment_names):
            status_msg = f"The deployments {unhealthy_deployment_names} are UNHEALTHY."
            if self._status in [
                ApplicationStatus.DEPLOYING,
                ApplicationStatus.DEPLOY_FAILED,
            ]:
                return ApplicationStatus.DEPLOY_FAILED, status_msg
            else:
                return ApplicationStatus.UNHEALTHY, status_msg
        else:
            return ApplicationStatus.DEPLOYING, ""

    def _reconcile_deploy_obj_ref(self) -> Tuple[BuildAppStatus, str]:
        """Reconcile the in-progress deploy task.

        Resets the self._deploy_obj_ref if it finished, regardless of
        whether it finished successfully.

        Returns:
            Status (BuildAppStatus):
                SUCCEEDED: task finished successfully.
                FAILED: an error occurred during execution of build app task
                IN_PROGRESS: task hasn't finished yet.
            Error message (str):
                Non-empty string if status is DEPLOY_FAILED or UNHEALTHY
        """
        if check_obj_ref_ready_nowait(self._deploy_obj_ref):
            deploy_obj_ref, self._deploy_obj_ref = self._deploy_obj_ref, None
            try:
                err = ray.get(deploy_obj_ref)
                if err is None:
                    logger.info(f"Deploy task for app '{self._name}' ran successfully.")
                    return BuildAppStatus.SUCCEEDED, ""
                else:
                    error_msg = (
                        f"Deploying app '{self._name}' failed with "
                        f"exception:\n{err}"
                    )
                    logger.warning(error_msg)
                    return BuildAppStatus.FAILED, error_msg
            except RuntimeEnvSetupError:
                error_msg = (
                    f"Runtime env setup for app '{self._name}' failed:\n"
                    + traceback.format_exc()
                )
                logger.warning(error_msg)
                return BuildAppStatus.FAILED, error_msg
            except Exception:
                error_msg = (
                    f"Unexpected error occured while deploying application "
                    f"'{self._name}': \n{traceback.format_exc()}"
                )
                logger.warning(error_msg)
                return BuildAppStatus.FAILED, error_msg

        return BuildAppStatus.IN_PROGRESS, ""

    def _reconcile_target_deployments(self) -> None:
        """Reconcile target deployments in application target state.

        Ensure each deployment is running on up-to-date info, and
        remove outdated deployments from the application.
        """

        # Set target state for each deployment
        for deployment_name, info in self._target_state.deployment_infos.items():
            self.apply_deployment_info(deployment_name, info)

        # Delete outdated deployments
        for deployment_name in self._get_live_deployments():
            if deployment_name not in self.target_deployments:
                self._delete_deployment(deployment_name)

    def update(self) -> bool:
        """Attempts to reconcile this application to match its target state.

        Updates the application status and status message based on the
        current state of the system.

        Returns:
            A boolean indicating whether the application is ready to be
            deleted.
        """

        # Reconcile in-progress deploy task, if there is any.
        if self._deploy_obj_ref:
            task_status, error_msg = self._reconcile_deploy_obj_ref()
            if task_status == BuildAppStatus.FAILED:
                self._update_status(ApplicationStatus.DEPLOY_FAILED, error_msg)

        # If we're waiting on the build app task to finish, we don't
        # have info on what the target list of deployments is, so don't
        # perform reconciliation or check on deployment statuses
        if self._target_state.deployment_infos is not None:
            self._reconcile_target_deployments()

            status, status_msg = self._determine_app_status()
            self._update_status(status, status_msg)

        # Check if app is ready to be deleted
        if self._target_state.deleting:
            return self.is_deleted()
        return False

    def get_checkpoint_data(self) -> ApplicationTargetState:
        return self._target_state

    def get_deployment(self, name: str) -> DeploymentInfo:
        """Get deployment info for deployment by name."""
        return self._deployment_state_manager.get_deployment(name)

    def get_deployments_statuses(self) -> List[DeploymentStatusInfo]:
        """Return all deployment status information"""
        return self._deployment_state_manager.get_deployment_statuses(
            self.target_deployments
        )

    def get_application_status_info(self) -> ApplicationStatusInfo:
        """Return the application status information"""
        return ApplicationStatusInfo(
            self._status,
            message=self._status_msg,
            deployment_timestamp=self._deployment_timestamp,
        )

    def list_deployment_details(self) -> Dict[str, DeploymentDetails]:
        """Gets detailed info on all live deployments in this application.
        (Does not include deleted deployments.)

        Returns:
            A dictionary of deployment infos. The set of deployment info returned
            may not be the full list of deployments that are part of the application.
            This can happen when the application is still deploying and bringing up
            deployments, or when the application is deleting and some deployments have
            been deleted.
        """
        details = {
            name: self._deployment_state_manager.get_deployment_details(name)
            for name in self.target_deployments
        }
        return {k: v for k, v in details.items() if v is not None}

    def _update_status(self, status: ApplicationStatus, status_msg: str = "") -> None:
        self._status = status
        self._status_msg = status_msg


class ApplicationStateManager:
    def __init__(
        self,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
        kv_store: KVStoreBase,
    ):
        self._deployment_state_manager = deployment_state_manager
        self._endpoint_state = endpoint_state
        self._kv_store = kv_store
        self._application_states: Dict[str, ApplicationState] = dict()
        self._recover_from_checkpoint()

    def _recover_from_checkpoint(self):
        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            application_state_info = cloudpickle.loads(checkpoint)

            for app_name, checkpoint_data in application_state_info.items():
                app_state = ApplicationState(
                    app_name,
                    self._deployment_state_manager,
                    self._endpoint_state,
                    self._save_checkpoint_func,
                )
                app_state.recover_target_state_from_checkpoint(checkpoint_data)
                self._application_states[app_name] = app_state

    def delete_application(self, name: str) -> None:
        """Delete application by name"""
        if name not in self._application_states:
            return
        self._application_states[name].delete()

    def apply_deployment_args(self, name: str, deployment_args: List[Dict]) -> None:
        """Apply list of deployment arguments to application target state.

        Args:
            name: application name
            deployment_args_list: arguments for deploying a list of deployments.

        Raises:
            RayServeException: If the list of deployments is trying to
                use a route prefix that is already used by another application
        """

        # Make sure route_prefix is not being used by other application.
        live_route_prefixes: Dict[str, str] = {
            self._application_states[app_name].route_prefix: app_name
            for app_name, app_state in self._application_states.items()
            if app_state.route_prefix is not None
            and not app_state.status == ApplicationStatus.DELETING
            and name != app_name
        }

        for deploy_param in deployment_args:
            deploy_app_prefix = deploy_param.get("route_prefix")
            if deploy_app_prefix in live_route_prefixes:
                raise RayServeException(
                    f"Prefix {deploy_app_prefix} is being used by application "
                    f'"{live_route_prefixes[deploy_app_prefix]}".'
                    f' Failed to deploy application "{name}".'
                )

        # This if conditional should only evaluate to true if the app is
        # being deployed through serve.run instead of from a config
        if name not in self._application_states:
            self._application_states[name] = ApplicationState(
                name,
                self._deployment_state_manager,
                self._endpoint_state,
                self._save_checkpoint_func,
            )
        record_extra_usage_tag(
            TagKey.SERVE_NUM_APPS, str(len(self._application_states))
        )
        self._application_states[name].apply_deployment_args(deployment_args)

    def get_deployments(self, app_name: str) -> List[str]:
        """Return all deployment names by app name"""
        if app_name not in self._application_states:
            return []
        return self._application_states[app_name].target_deployments

    def get_deployments_statuses(self, app_name: str) -> List[DeploymentStatusInfo]:
        """Return all deployment statuses by app name"""
        if app_name not in self._application_states:
            return []
        return self._application_states[app_name].get_deployments_statuses()

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

    def get_deployment_timestamp(self, name: str) -> float:
        if name not in self._application_states:
            return -1
        return self._application_states[name].deployment_timestamp

    def get_docs_path(self, app_name: str) -> Optional[str]:
        return self._application_states[app_name].docs_path

    def get_route_prefix(self, name: str) -> Optional[str]:
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
    ) -> None:
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
                endpoint_state=self._endpoint_state,
                save_checkpoint_func=self._save_checkpoint_func,
                deploy_obj_ref=deploy_obj_ref,
                deployment_time=deployment_time,
            )

    def update(self):
        """Update each application state"""
        apps_to_be_deleted = []
        for name, app in self._application_states.items():
            ready_to_be_deleted = app.update()
            if ready_to_be_deleted:
                apps_to_be_deleted.append(name)

        if len(apps_to_be_deleted) > 0:
            for app_name in apps_to_be_deleted:
                del self._application_states[app_name]
            record_extra_usage_tag(
                TagKey.SERVE_NUM_APPS, str(len(self._application_states))
            )

    def shutdown(self) -> None:
        for app_state in self._application_states.values():
            app_state.delete()

    def is_ready_for_shutdown(self) -> bool:
        """Return whether all applications have shut down.

        Iterate through all application states and check if all their applications
        are deleted.
        """
        return all(
            app_state.is_deleted() for app_state in self._application_states.values()
        )

    def _save_checkpoint_func(
        self, *, writeahead_checkpoints: Optional[Dict[str, ApplicationTargetState]]
    ) -> None:
        """Write a checkpoint of all application states."""

        application_state_info = {
            app_name: app_state.get_checkpoint_data()
            for app_name, app_state in self._application_states.items()
        }

        if writeahead_checkpoints is not None:
            application_state_info.update(writeahead_checkpoints)

        self._kv_store.put(
            CHECKPOINT_KEY,
            cloudpickle.dumps(application_state_info),
        )
