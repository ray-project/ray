from dataclasses import dataclass
import traceback
import logging
import time
from typing import Dict, List, Optional, Callable
from collections import Counter

import ray
from ray import cloudpickle
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.types import ObjectRef
from ray.exceptions import RayTaskError, RuntimeEnvSetupError
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.storage.kv_store import KVStoreBase
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

CHECKPOINT_KEY = "serve-application-state-checkpoint"


@dataclass
class ApplicationTargetState:
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
            name: application name
            deployment_state_manager: deployment state manager which is used for
                fetching deployment information
            deploy_obj_ref: Task ObjRef of deploying application.
            deployment_time: Deployment timestamp
        """
        self._name = name
        self._endpoint_state = endpoint_state
        self.deployment_state_manager = deployment_state_manager
        self.deploy_obj_ref = deploy_obj_ref
        self.app_msg = ""
        self.route_prefix = None
        self.docs_path = None
        self.ready_to_be_deleted = False

        if deploy_obj_ref:
            self.status: ApplicationStatus = ApplicationStatus.DEPLOYING
        else:
            self.status: ApplicationStatus = ApplicationStatus.NOT_STARTED

        if deployment_time:
            self.deployment_timestamp = deployment_time
        else:
            self.deployment_timestamp = time.time()

        self._target_state: ApplicationTargetState = ApplicationTargetState(
            deployment_infos=None, deleting=False
        )
        self._save_checkpoint_func = save_checkpoint_func

    def recover_target_state_from_checkpoint(self, checkpoint_data):
        logger.info(f"Recovering target state for app {self._name} from checkpoint.")
        self._target_state = checkpoint_data

    def _set_target_state(
        self,
        deployment_infos: Optional[Dict[str, DeploymentInfo]] = None,
        deleting: bool = False,
    ):
        if deleting:
            target_state = ApplicationTargetState(dict(), True)
            self.status = ApplicationStatus.DELETING
        else:
            target_state = ApplicationTargetState(deployment_infos, False)
            # print("target state: status to deploying", time.time())
            self.status = ApplicationStatus.DEPLOYING

        # Checkpoint ahead
        self._save_checkpoint_func(writeahead_checkpoints={self._name: target_state})
        # Set target state
        self._target_state = target_state

    def _delete_deployment(self, name):
        self._endpoint_state.delete_endpoint(name)
        self.deployment_state_manager.delete_deployment(name)

    def delete(self):
        """Delete the application"""
        logger.info(f"Deleting application '{self._name}'")
        self._set_target_state(deleting=True)

    def apply_deployment(
        self,
        deployment_name: str,
        deployment_info: DeploymentInfo,
    ) -> bool:
        """
        Deploys a deployment in the application.

        Returns: Whether the deployment is being updated.
        """
        updating = self.deployment_state_manager.deploy(
            deployment_name, deployment_info
        )

        if deployment_info.route_prefix is not None:
            assert deployment_info.route_prefix.startswith("/")
            self.route_prefix = deployment_info.route_prefix
            endpoint_info = EndpointInfo(route=deployment_info.route_prefix)
            self._endpoint_state.update_endpoint(deployment_name, endpoint_info)
        else:
            self._endpoint_state.delete_endpoint(deployment_name)

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

        for params in deployment_params:
            params["deployment_name"] = params.pop("name")

        deployment_infos = {
            params["deployment_name"]: deploy_args_to_deployment_info(
                app_name=self._name,
                **params,
            )
            for params in deployment_params
        }
        self._set_target_state(deployment_infos=deployment_infos)
        return [True for _ in deployment_params]

    def update_obj_ref(self, deploy_obj_ref: ObjectRef, deployment_time: int):
        self.deploy_obj_ref = deploy_obj_ref
        self.deployment_timestamp = deployment_time
        print("status set to deploying", time.time())
        self._set_target_state(deployment_infos=None)
        self.status = ApplicationStatus.DEPLOYING

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

        if self._target_state.deployment_infos is None:
            return

        # Deploy/update deployments
        for deployment_name, info in self._target_state.deployment_infos.items():
            # print("deployment_info", deployment_info.deployment_config)
            self.apply_deployment(deployment_name, info)

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
                if self.status == ApplicationStatus.RUNNING:
                    self.status = ApplicationStatus.ERRORED
                else:
                    self.status = ApplicationStatus.DEPLOY_FAILED
            elif deployment_statuses[DeploymentStatus.HEALTHY] == len(self.deployments):
                # print("application status is now RUNNING", time.time())
                self.status = ApplicationStatus.RUNNING

    @property
    def deployments(self) -> List[str]:
        """Names of all deployments in application."""
        if self._target_state.deployment_infos is None:
            return []
        return list(self._target_state.deployment_infos.keys())

    def get_deployment(self, name: str) -> DeploymentInfo:
        """Get deployment info for deployment by name."""
        return self.deployment_state_manager.get_deployment(name)

    def get_checkpoint_data(self):
        return self._target_state

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
        kv_store: KVStoreBase,
    ):
        self.deployment_state_manager = deployment_state_manager
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
                    self.deployment_state_manager,
                    self._endpoint_state,
                    self._save_checkpoint_func,
                )
                app_state.recover_target_state_from_checkpoint(checkpoint_data)
                self._application_states[app_name] = app_state

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
                self._endpoint_state,
                self._save_checkpoint_func,
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
                endpoint_state=self._endpoint_state,
                save_checkpoint_func=self._save_checkpoint_func,
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

    def _save_checkpoint_func(self, *, writeahead_checkpoints: Optional[Dict]) -> None:
        """Write a checkpoint of all application states.
        By default, this checkpoints the current in-memory state of each
        deployment. However, these can be overwritten by passing
        `writeahead_checkpoints` in order to checkpoint an update before
        applying it to the in-memory state.
        """

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
