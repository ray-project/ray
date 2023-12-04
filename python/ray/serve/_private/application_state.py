import logging
import time
import traceback
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple

import ray
from ray import cloudpickle
from ray._private.utils import import_attr
from ray.exceptions import RuntimeEnvSetupError
from ray.serve._private.common import (
    ApplicationStatus,
    ApplicationStatusInfo,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusTrigger,
    EndpointInfo,
    EndpointTag,
    TargetCapacityDirection,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.deploy_utils import (
    deploy_args_to_deployment_info,
    get_app_code_version,
    get_deploy_args,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.storage.kv_store import KVStoreBase
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    DEFAULT,
    check_obj_ref_ready_nowait,
    override_runtime_envs_except_env_vars,
)
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import DeploymentLanguage
from ray.serve.schema import DeploymentDetails, ServeApplicationSchema
from ray.types import ObjectRef

logger = logging.getLogger(SERVE_LOGGER_NAME)

CHECKPOINT_KEY = "serve-application-state-checkpoint"


class BuildAppStatus(Enum):
    """Status of the build application task."""

    NO_TASK_IN_PROGRESS = 1
    IN_PROGRESS = 2
    SUCCEEDED = 3
    FAILED = 4


@dataclass
class BuildAppTaskInfo:
    """Stores info on the current in-progress build app task.

    We use a class instead of only storing the task object ref because
    when a new config is deployed, there can be an outdated in-progress
    build app task. We attach the code version to the task info to
    distinguish outdated build app tasks.
    """

    obj_ref: ObjectRef
    code_version: str
    config: ServeApplicationSchema
    target_capacity: Optional[float]
    target_capacity_direction: Optional[TargetCapacityDirection]
    finished: bool


@dataclass
class ApplicationTargetState:
    """Defines target state of application.

    Target state can become inconsistent if the code version doesn't
    match that of the config. When that happens, a new build app task
    should be kicked off to reconcile the inconsistency.

    deployment_infos: map of deployment name to deployment info. This is
      - None if a config was deployed but the app hasn't finished
        building yet,
      - An empty dict if the app is deleting.
    code_version: Code version of all deployments in target state. None
        if application was deployed through serve.run.
    config: application config deployed by user. None if application was
        deployed through serve.run.
    target_capacity: the target_capacity to use when adjusting num_replicas.
    target_capacity_direction: the scale direction to use when
        running the Serve autoscaler.
    deleting: whether the application is being deleted.
    """

    deployment_infos: Optional[Dict[str, DeploymentInfo]]
    code_version: Optional[str]
    config: Optional[ServeApplicationSchema]
    target_capacity: Optional[float]
    target_capacity_direction: Optional[TargetCapacityDirection]
    deleting: bool


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        name: str,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
        save_checkpoint_func: Callable,
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
        """

        self._name = name
        self._status_msg = ""
        self._deployment_state_manager = deployment_state_manager
        self._endpoint_state = endpoint_state
        self._route_prefix: Optional[str] = None
        self._docs_path: Optional[str] = None
        self._ingress_deployment_name: str = None

        self._status: ApplicationStatus = ApplicationStatus.DEPLOYING
        self._deployment_timestamp = time.time()

        self._build_app_task_info: Optional[BuildAppTaskInfo] = None
        # Before a deploy app task finishes, we don't know what the
        # target deployments are, so set deployment_infos=None
        self._target_state: ApplicationTargetState = ApplicationTargetState(
            deployment_infos=None,
            code_version=None,
            config=None,
            target_capacity=None,
            target_capacity_direction=None,
            deleting=False,
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

        DEPLOYING: The build task is still running, or the deployments
            have started deploying but aren't healthy yet.
        RUNNING: All deployments are healthy.
        DEPLOY_FAILED: The build task failed or one or more deployments
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
    def target_deployments(self) -> List[str]:
        """List of target deployment names in application."""
        if self._target_state.deployment_infos is None:
            return []
        return list(self._target_state.deployment_infos.keys())

    @property
    def ingress_deployment(self) -> Optional[str]:
        return self._ingress_deployment_name

    def recover_target_state_from_checkpoint(
        self, checkpoint_data: ApplicationTargetState
    ):
        logger.info(
            f"Recovering target state for application '{self._name}' from checkpoint."
        )
        self._set_target_state(
            checkpoint_data.deployment_infos,
            checkpoint_data.code_version,
            checkpoint_data.config,
            checkpoint_data.target_capacity,
            checkpoint_data.target_capacity_direction,
            checkpoint_data.deleting,
        )

    def _set_target_state(
        self,
        deployment_infos: Optional[Dict[str, DeploymentInfo]],
        code_version: str,
        target_config: Optional[ServeApplicationSchema],
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
        deleting: bool = False,
    ):
        """Set application target state.

        While waiting for build task to finish, this should be
            (None, False)
        When build task has finished and during normal operation, this should be
            (target_deployments, False)
        When a request to delete the application has been received, this should be
            ({}, True)
        """

        if deleting:
            self._update_status(ApplicationStatus.DELETING)
        else:
            self._update_status(ApplicationStatus.DEPLOYING)

        if deployment_infos is None:
            self._ingress_deployment_name = None
        else:
            for name, info in deployment_infos.items():
                if info.ingress:
                    self._ingress_deployment_name = name

        target_state = ApplicationTargetState(
            deployment_infos,
            code_version,
            target_config,
            target_capacity,
            target_capacity_direction,
            deleting,
        )

        # Checkpoint ahead, so that if the controller crashes before we
        # write to the target state, the target state will be recovered
        # after the controller recovers
        self._save_checkpoint_func(writeahead_checkpoints={self._name: target_state})
        # Set target state
        self._target_state = target_state

    def _set_target_state_deleting(self):
        """Set target state to deleting.

        Wipes the target deployment infos, code version, and config.
        """
        self._set_target_state(dict(), None, None, None, None, True)

    def _clear_target_state_and_store_config(
        self, target_config: Optional[ServeApplicationSchema]
    ):
        """Clears the target state and stores the config."""

        self._set_target_state(None, None, target_config, None, None, False)

    def _delete_deployment(self, name):
        id = EndpointTag(name, self._name)
        self._endpoint_state.delete_endpoint(id)
        self._deployment_state_manager.delete_deployment(id)

    def delete(self):
        """Delete the application"""
        if self._status != ApplicationStatus.DELETING:
            logger.info(
                f"Deleting application '{self._name}'",
                extra={"log_to_stderr": False},
            )
        self._set_target_state_deleting()

    def is_deleted(self) -> bool:
        """Check whether the application is already deleted.

        For an application to be considered deleted, the target state has to be set to
        deleting and all deployments have to be deleted.
        """
        return self._target_state.deleting and len(self._get_live_deployments()) == 0

    def apply_deployment_info(
        self,
        deployment_name: str,
        deployment_info: DeploymentInfo,
    ) -> None:
        """Deploys a deployment in the application."""
        route_prefix = deployment_info.route_prefix
        if route_prefix is not None and not route_prefix.startswith("/"):
            raise RayServeException(
                f'Invalid route prefix "{route_prefix}", it must start with "/"'
            )

        deployment_id = DeploymentID(deployment_name, self._name)

        self._deployment_state_manager.deploy(deployment_id, deployment_info)

        if deployment_info.route_prefix is not None:
            config = deployment_info.deployment_config
            self._endpoint_state.update_endpoint(
                deployment_id,
                # The current meaning of the "is_cross_language" field is ambiguous.
                # We will work on optimizing and removing this field in the future.
                # Instead of using the "is_cross_language" field, we will directly
                # compare if the replica is Python, which will assist the Python
                # router in determining if the replica invocation is a cross-language
                # operation.
                EndpointInfo(
                    route=deployment_info.route_prefix,
                    app_is_cross_language=config.deployment_language
                    != DeploymentLanguage.PYTHON,
                ),
            )
        else:
            self._endpoint_state.delete_endpoint(deployment_id)

    def deploy(self, deployment_infos: Dict[str, DeploymentInfo]):
        """Deploy application from list of deployment infos.

        This function should only be called if the app is being deployed
        through serve.run instead of from a config.

        Raises: RayServeException if there is more than one route prefix
            or docs path.
        """

        # Check routes are unique in deployment infos
        self._route_prefix, self._docs_path = self._check_routes(deployment_infos)

        self._set_target_state(
            deployment_infos=deployment_infos,
            code_version=None,
            target_config=None,
            target_capacity=None,
            target_capacity_direction=None,
        )

    def deploy_config(
        self,
        config: ServeApplicationSchema,
        target_capacity: Optional[float],
        target_capacity_direction: Optional[TargetCapacityDirection],
        deployment_time: int,
    ) -> None:
        """Deploys an application config.

        If the code version matches that of the current live deployments
        then it only applies the updated config to the deployment state
        manager. If the code version doesn't match, this will re-build
        the application.
        """

        self._deployment_timestamp = deployment_time

        config_version = get_app_code_version(config)
        if config_version == self._target_state.code_version:
            try:
                overrided_infos = override_deployment_info(
                    self._name,
                    self._target_state.deployment_infos,
                    config,
                )
                self._check_routes(overrided_infos)
                self._set_target_state(
                    # Code version doesn't change.
                    code_version=self._target_state.code_version,
                    # Everything else must reflect the new config.
                    deployment_infos=overrided_infos,
                    target_config=config,
                    target_capacity=target_capacity,
                    target_capacity_direction=target_capacity_direction,
                )
            except (TypeError, ValueError, RayServeException):
                self._clear_target_state_and_store_config(config)
                self._update_status(
                    ApplicationStatus.DEPLOY_FAILED, traceback.format_exc()
                )
            except Exception:
                self._clear_target_state_and_store_config(config)
                self._update_status(
                    ApplicationStatus.DEPLOY_FAILED,
                    (
                        f"Unexpected error occured while applying config for "
                        f"application '{self._name}': \n{traceback.format_exc()}"
                    ),
                )
        else:
            # If there is an in progress build task, cancel it.
            if self._build_app_task_info and not self._build_app_task_info.finished:
                logger.info(
                    f"Received new config for application '{self._name}'. "
                    "Cancelling previous request."
                )
                ray.cancel(self._build_app_task_info.obj_ref)

            # Halt reconciliation of target deployments. A new target state
            # will be set once the new app has finished building.
            self._clear_target_state_and_store_config(config)

            # Kick off new build app task
            logger.info(f"Building application '{self._name}'.")
            build_app_obj_ref = build_serve_application.options(
                runtime_env=config.runtime_env
            ).remote(
                config.import_path,
                config.deployment_names,
                config_version,
                config.name,
                config.args,
            )
            self._build_app_task_info = BuildAppTaskInfo(
                obj_ref=build_app_obj_ref,
                code_version=config_version,
                config=config,
                target_capacity=target_capacity,
                target_capacity_direction=target_capacity_direction,
                finished=False,
            )

    def _get_live_deployments(self) -> List[str]:
        return self._deployment_state_manager.get_deployments_in_application(self._name)

    def _determine_app_status(self) -> Tuple[ApplicationStatus, str]:
        """Check deployment statuses and target state, and determine the
        corresponding application status.

        Returns:
            Status (ApplicationStatus):
                RUNNING: all deployments are healthy or autoscaling.
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
        num_autoscaling_deployments = 0
        num_updating_deployments = 0
        num_manually_scaling_deployments = 0
        unhealthy_deployment_names = []

        for deployment_status in self.get_deployments_statuses():
            if deployment_status.status == DeploymentStatus.UNHEALTHY:
                unhealthy_deployment_names.append(deployment_status.name)
            elif deployment_status.status == DeploymentStatus.HEALTHY:
                num_healthy_deployments += 1
            elif (
                deployment_status.status_trigger == DeploymentStatusTrigger.AUTOSCALING
            ):
                num_autoscaling_deployments += 1
            elif deployment_status.status == DeploymentStatus.UPDATING:
                num_updating_deployments += 1
            elif (
                deployment_status.status
                in [DeploymentStatus.UPSCALING, DeploymentStatus.DOWNSCALING]
                and deployment_status.status_trigger
                == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
            ):
                num_manually_scaling_deployments += 1
            else:
                raise RuntimeError(
                    "Found deployment with unexpected status "
                    f"{deployment_status.status} and status trigger "
                    f"{deployment_status.status_trigger}."
                )

        if len(unhealthy_deployment_names):
            status_msg = f"The deployments {unhealthy_deployment_names} are UNHEALTHY."
            if self._status in [
                ApplicationStatus.DEPLOYING,
                ApplicationStatus.DEPLOY_FAILED,
            ]:
                return ApplicationStatus.DEPLOY_FAILED, status_msg
            else:
                return ApplicationStatus.UNHEALTHY, status_msg
        elif num_updating_deployments + num_manually_scaling_deployments > 0:
            # If deployments are UPDATING or UPSCALING/DOWNSCALING
            # with status trigger CONFIG_UPDATE_STARTED, then
            # application is still DEPLOYING
            return ApplicationStatus.DEPLOYING, ""
        else:
            # If all deployments are HEALTHY or autoscaling, then
            # application is RUNNING
            assert num_healthy_deployments + num_autoscaling_deployments == len(
                self.target_deployments
            )
            return ApplicationStatus.RUNNING, ""

    def _reconcile_build_app_task(self) -> Tuple[Tuple, BuildAppStatus, str]:
        """If necessary, reconcile the in-progress build task.

        Returns:
            Deploy arguments (Dict[str, DeploymentInfo]):
                The deploy arguments returned from the build app task
                and their code version.
            Status (BuildAppStatus):
                NO_TASK_IN_PROGRESS: There is no build task to reconcile.
                SUCCEEDED: Task finished successfully.
                FAILED: An error occurred during execution of build app task
                IN_PROGRESS: Task hasn't finished yet.
            Error message (str):
                Non-empty string if status is DEPLOY_FAILED or UNHEALTHY
        """
        if self._build_app_task_info is None or self._build_app_task_info.finished:
            return None, BuildAppStatus.NO_TASK_IN_PROGRESS, ""

        if not check_obj_ref_ready_nowait(self._build_app_task_info.obj_ref):
            return None, BuildAppStatus.IN_PROGRESS, ""

        # Retrieve build app task result
        self._build_app_task_info.finished = True
        try:
            args, err = ray.get(self._build_app_task_info.obj_ref)
            if err is None:
                logger.info(f"Built application '{self._name}' successfully.")
            else:
                return (
                    None,
                    BuildAppStatus.FAILED,
                    f"Deploying app '{self._name}' failed with exception:\n{err}",
                )
        except RuntimeEnvSetupError:
            error_msg = (
                f"Runtime env setup for app '{self._name}' failed:\n"
                + traceback.format_exc()
            )
            return None, BuildAppStatus.FAILED, error_msg
        except Exception:
            error_msg = (
                f"Unexpected error occured while deploying application "
                f"'{self._name}': \n{traceback.format_exc()}"
            )
            return None, BuildAppStatus.FAILED, error_msg

        # Convert serialized deployment args (returned by build app task)
        # to deployment infos and apply option overrides from config
        try:
            deployment_infos = {
                params["deployment_name"]: deploy_args_to_deployment_info(
                    **params, app_name=self._name
                )
                for params in args
            }
            overrided_infos = override_deployment_info(
                self._name, deployment_infos, self._build_app_task_info.config
            )
            self._route_prefix, self._docs_path = self._check_routes(overrided_infos)
            return overrided_infos, BuildAppStatus.SUCCEEDED, ""
        except (TypeError, ValueError, RayServeException):
            return None, BuildAppStatus.FAILED, traceback.format_exc()
        except Exception:
            error_msg = (
                f"Unexpected error occured while applying config for application "
                f"'{self._name}': \n{traceback.format_exc()}"
            )
            return None, BuildAppStatus.FAILED, error_msg

    def _check_routes(
        self, deployment_infos: Dict[str, DeploymentInfo]
    ) -> Tuple[str, str]:
        """Check route prefixes and docs paths of deployments in app.

        There should only be one non-null route prefix. If there is one,
        set it as the application route prefix. This function must be
        run every control loop iteration because the target config could
        be updated without kicking off a new task.

        Returns: tuple of route prefix, docs path.
        Raises: RayServeException if more than one route prefix or docs
            path is found among deployments.
        """
        num_route_prefixes = 0
        num_docs_paths = 0
        route_prefix = None
        docs_path = None
        for info in deployment_infos.values():
            # Update route prefix of application, which may be updated
            # through a redeployed config.
            if info.route_prefix is not None:
                route_prefix = info.route_prefix
                num_route_prefixes += 1
            if info.docs_path is not None:
                docs_path = info.docs_path
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

        return route_prefix, docs_path

    def _reconcile_target_deployments(self) -> None:
        """Reconcile target deployments in application target state.

        Ensure each deployment is running on up-to-date info, and
        remove outdated deployments from the application.
        """

        # Set target state for each deployment
        for deployment_name, info in self._target_state.deployment_infos.items():
            deploy_info = deepcopy(info)

            # Apply the target capacity information to the deployment info.
            deploy_info.set_target_capacity(
                new_target_capacity=self._target_state.target_capacity,
                new_target_capacity_direction=(
                    self._target_state.target_capacity_direction
                ),
            )

            # Apply the application logging config to the deployment logging config
            # if it is not set.
            if (
                self._target_state.config
                and self._target_state.config.logging_config
                and deploy_info.deployment_config.logging_config is None
            ):
                deploy_info.deployment_config.logging_config = (
                    self._target_state.config.logging_config
                )
            self.apply_deployment_info(deployment_name, deploy_info)

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

        infos, task_status, msg = self._reconcile_build_app_task()
        if task_status == BuildAppStatus.SUCCEEDED:
            self._set_target_state(
                deployment_infos=infos,
                code_version=self._build_app_task_info.code_version,
                target_config=self._build_app_task_info.config,
                target_capacity=self._build_app_task_info.target_capacity,
                target_capacity_direction=(
                    self._build_app_task_info.target_capacity_direction
                ),
            )
        elif task_status == BuildAppStatus.FAILED:
            self._update_status(ApplicationStatus.DEPLOY_FAILED, msg)

        # Only reconcile deployments when the build app task is finished. If
        # it's not finished, we don't know what the target list of deployments
        # is, so we don't perform any reconciliation.
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

    def get_deployments_statuses(self) -> List[DeploymentStatusInfo]:
        """Return all deployment status information"""
        deployments = [
            DeploymentID(deployment, self._name)
            for deployment in self.target_deployments
        ]
        return self._deployment_state_manager.get_deployment_statuses(deployments)

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
            deployment_name: self._deployment_state_manager.get_deployment_details(
                DeploymentID(deployment_name, self._name)
            )
            for deployment_name in self.target_deployments
        }
        return {k: v for k, v in details.items() if v is not None}

    def _update_status(self, status: ApplicationStatus, status_msg: str = "") -> None:
        if status_msg and status in [
            ApplicationStatus.DEPLOY_FAILED,
            ApplicationStatus.UNHEALTHY,
        ]:
            logger.warning(status_msg)

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

        This function should only be called if the app is being deployed
        through serve.run instead of from a config.

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

        if name not in self._application_states:
            self._application_states[name] = ApplicationState(
                name,
                self._deployment_state_manager,
                self._endpoint_state,
                self._save_checkpoint_func,
            )
        ServeUsageTag.NUM_APPS.record(str(len(self._application_states)))

        deployment_infos = {
            params["deployment_name"]: deploy_args_to_deployment_info(
                **params, app_name=name
            )
            for params in deployment_args
        }
        self._application_states[name].deploy(deployment_infos)

    def deploy_config(
        self,
        name: str,
        app_config: ServeApplicationSchema,
        deployment_time: float = 0,
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
    ) -> None:
        """Deploy application from config."""

        if name not in self._application_states:
            self._application_states[name] = ApplicationState(
                name,
                self._deployment_state_manager,
                endpoint_state=self._endpoint_state,
                save_checkpoint_func=self._save_checkpoint_func,
            )
        ServeUsageTag.NUM_APPS.record(str(len(self._application_states)))
        self._application_states[name].deploy_config(
            app_config,
            target_capacity,
            target_capacity_direction,
            deployment_time=deployment_time,
        )

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

    def get_docs_path(self, app_name: str) -> Optional[str]:
        return self._application_states[app_name].docs_path

    def get_route_prefix(self, name: str) -> Optional[str]:
        return self._application_states[name].route_prefix

    def get_ingress_deployment_name(self, name: str) -> Optional[str]:
        if name not in self._application_states:
            return None

        return self._application_states[name].ingress_deployment

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

    def update(self):
        """Update each application state"""
        apps_to_be_deleted = []
        for name, app in self._application_states.items():
            ready_to_be_deleted = app.update()
            if ready_to_be_deleted:
                apps_to_be_deleted.append(name)
                logger.debug(f"Application '{name}' deleted successfully.")

        if len(apps_to_be_deleted) > 0:
            for app_name in apps_to_be_deleted:
                del self._application_states[app_name]
            ServeUsageTag.NUM_APPS.record(str(len(self._application_states)))

    def shutdown(self) -> None:
        for app_state in self._application_states.values():
            app_state.delete()

        self._kv_store.delete(CHECKPOINT_KEY)

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


@ray.remote(num_cpus=0, max_calls=1)
def build_serve_application(
    import_path: str,
    config_deployments: List[str],
    code_version: str,
    name: str,
    args: Dict,
) -> Tuple[List[Dict], Optional[str]]:
    """Import and build a Serve application.

    Args:
        import_path: import path to top-level bound deployment.
        config_deployments: list of deployment names specified in config
            with deployment override options. This is used to check that
            all deployments specified in the config are valid.
        code_version: code version inferred from app config. All
            deployment versions are set to this code version.
        name: application name. If specified, application will be deployed
            without removing existing applications.
        args: Arguments to be passed to the application builder.
        logging_config: The application logging config, if deployment logging
            config is not set, application logging config will be applied to the
            deployment logging config.
    Returns:
        Deploy arguments: a list of deployment arguments if application
            was built successfully, otherwise None.
        Error message: a string if an error was raised, otherwise None.
    """
    try:
        from ray.serve._private.api import call_app_builder_with_args_if_necessary
        from ray.serve._private.deployment_graph_build import build as pipeline_build
        from ray.serve._private.deployment_graph_build import (
            get_and_validate_ingress_deployment,
        )

        # Import and build the application.
        app = call_app_builder_with_args_if_necessary(import_attr(import_path), args)
        deployments = pipeline_build(app._get_internal_dag_node(), name)
        ingress = get_and_validate_ingress_deployment(deployments)

        deploy_args_list = []
        for deployment in deployments:
            is_ingress = deployment.name == ingress.name
            deploy_args_list.append(
                get_deploy_args(
                    name=deployment._name,
                    replica_config=deployment._replica_config,
                    ingress=is_ingress,
                    deployment_config=deployment._deployment_config,
                    version=code_version,
                    route_prefix=deployment.route_prefix,
                    docs_path=deployment._docs_path,
                )
            )
        return deploy_args_list, None
    except KeyboardInterrupt:
        # Error is raised when this task is canceled with ray.cancel(), which
        # happens when deploy_apps() is called.
        logger.info("Existing config deployment request terminated.")
        return None, None
    except Exception:
        return None, traceback.format_exc()


def override_deployment_info(
    app_name: str,
    deployment_infos: Dict[str, DeploymentInfo],
    override_config: Optional[ServeApplicationSchema],
) -> Dict[str, DeploymentInfo]:
    """Override deployment infos with options from app config.

    Args:
        app_name: application name
        deployment_infos: deployment info loaded from code
        override_config: application config deployed by user with
            options to override those loaded from code.

    Returns: the updated deployment infos.

    Raises:
        ValueError: If config options have invalid values.
        TypeError: If config options have invalid types.
    """

    deployment_infos = deepcopy(deployment_infos)
    if override_config is None:
        return deployment_infos

    config_dict = override_config.dict(exclude_unset=True)
    deployment_override_options = config_dict.get("deployments", [])

    # Override options for each deployment listed in the config.
    for options in deployment_override_options:
        deployment_name = options["name"]
        info = deployment_infos[deployment_name]

        if (
            info.deployment_config.autoscaling_config is not None
            and info.deployment_config.max_concurrent_queries
            < info.deployment_config.autoscaling_config.target_num_ongoing_requests_per_replica  # noqa: E501
        ):
            logger.warning(
                "Autoscaling will never happen, "
                "because 'max_concurrent_queries' is less than "
                "'target_num_ongoing_requests_per_replica' now."
            )

        # What to pass to info.update
        override_options = dict()

        # Override route prefix if specified in deployment config
        deployment_route_prefix = options.pop("route_prefix", DEFAULT.VALUE)
        if deployment_route_prefix is not DEFAULT.VALUE:
            override_options["route_prefix"] = deployment_route_prefix

        # Merge app-level and deployment-level runtime_envs.
        replica_config = info.replica_config
        app_runtime_env = override_config.runtime_env
        if "ray_actor_options" in options:
            # If specified, get ray_actor_options from config
            override_actor_options = options.pop("ray_actor_options", {})
        else:
            # Otherwise, get options from application code (and default to {}
            # if the code sets options to None).
            override_actor_options = replica_config.ray_actor_options or {}

        override_placement_group_bundles = options.pop(
            "placement_group_bundles", replica_config.placement_group_bundles
        )
        override_placement_group_strategy = options.pop(
            "placement_group_strategy", replica_config.placement_group_strategy
        )

        override_max_replicas_per_node = options.pop(
            "max_replicas_per_node", replica_config.max_replicas_per_node
        )

        merged_env = override_runtime_envs_except_env_vars(
            app_runtime_env, override_actor_options.get("runtime_env", {})
        )
        override_actor_options.update({"runtime_env": merged_env})
        replica_config.update_ray_actor_options(override_actor_options)
        replica_config.update_placement_group_options(
            override_placement_group_bundles, override_placement_group_strategy
        )
        replica_config.update_max_replicas_per_node(override_max_replicas_per_node)
        override_options["replica_config"] = replica_config

        # Override deployment config options
        original_options = info.deployment_config.dict()
        options.pop("name", None)
        original_options.update(options)
        override_options["deployment_config"] = DeploymentConfig(**original_options)
        deployment_infos[deployment_name] = info.update(**override_options)

    # Overwrite ingress route prefix
    app_route_prefix = config_dict.get("route_prefix", DEFAULT.VALUE)
    for deployment in list(deployment_infos.values()):
        if (
            app_route_prefix is not DEFAULT.VALUE
            and deployment.route_prefix is not None
        ):
            deployment.route_prefix = app_route_prefix

    return deployment_infos
