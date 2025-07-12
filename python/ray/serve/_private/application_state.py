import inspect
import json
import logging
import os
import time
import traceback
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import ray
from ray import cloudpickle
from ray._common.utils import import_attr
from ray.exceptions import RuntimeEnvSetupError
from ray.serve._private.build_app import BuiltApplication, build_app
from ray.serve._private.common import (
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusTrigger,
    EndpointInfo,
    TargetCapacityDirection,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import RAY_SERVE_ENABLE_TASK_EVENTS, SERVE_LOGGER_NAME
from ray.serve._private.deploy_utils import (
    deploy_args_to_deployment_info,
    get_app_code_version,
    get_deploy_args,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.logging_utils import configure_component_logger
from ray.serve._private.storage.kv_store import KVStoreBase
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    DEFAULT,
    check_obj_ref_ready_nowait,
    override_runtime_envs_except_env_vars,
    validate_route_prefix,
)
from ray.serve.api import ASGIAppReplicaWrapper
from ray.serve.config import AutoscalingConfig
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import (
    ApplicationStatus as ApplicationStatusProto,
    ApplicationStatusInfo as ApplicationStatusInfoProto,
    DeploymentLanguage,
    DeploymentStatusInfoList as DeploymentStatusInfoListProto,
    StatusOverview as StatusOverviewProto,
)
from ray.serve.schema import (
    APIType,
    ApplicationStatus,
    DeploymentDetails,
    LoggingConfig,
    ServeApplicationSchema,
)
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


@dataclass(eq=True)
class ApplicationStatusInfo:
    status: ApplicationStatus
    message: str = ""
    deployment_timestamp: float = 0

    def debug_string(self):
        return json.dumps(asdict(self), indent=4)

    def to_proto(self):
        return ApplicationStatusInfoProto(
            status=f"APPLICATION_STATUS_{self.status.name}",
            message=self.message,
            deployment_timestamp=self.deployment_timestamp,
        )

    @classmethod
    def from_proto(cls, proto: ApplicationStatusInfoProto):
        status = ApplicationStatusProto.Name(proto.status)[len("APPLICATION_STATUS_") :]
        return cls(
            status=ApplicationStatus(status),
            message=proto.message,
            deployment_timestamp=proto.deployment_timestamp,
        )


@dataclass(eq=True)
class StatusOverview:
    app_status: ApplicationStatusInfo
    name: str = ""
    deployment_statuses: List[DeploymentStatusInfo] = field(default_factory=list)

    def debug_string(self):
        return json.dumps(asdict(self), indent=4)

    def get_deployment_status(self, name: str) -> Optional[DeploymentStatusInfo]:
        """Get a deployment's status by name.

        Args:
            name: Deployment's name.

        Returns:
            Optional[DeploymentStatusInfo]: The status of the deployment if it exists,
                otherwise None.
        """

        for deployment_status in self.deployment_statuses:
            if name == deployment_status.name:
                return deployment_status

        return None

    def to_proto(self):
        # Create a protobuf for the Serve Application info
        app_status_proto = self.app_status.to_proto()

        # Create protobufs for all individual deployment statuses
        deployment_status_protos = map(
            lambda status: status.to_proto(), self.deployment_statuses
        )

        # Create a protobuf list containing all the deployment status protobufs
        deployment_status_proto_list = DeploymentStatusInfoListProto()
        deployment_status_proto_list.deployment_status_infos.extend(
            deployment_status_protos
        )

        # Return protobuf encapsulating application and deployment protos
        return StatusOverviewProto(
            name=self.name,
            app_status=app_status_proto,
            deployment_statuses=deployment_status_proto_list,
        )

    @classmethod
    def from_proto(cls, proto: StatusOverviewProto) -> "StatusOverview":
        # Recreate Serve Application info
        app_status = ApplicationStatusInfo.from_proto(proto.app_status)

        # Recreate deployment statuses
        deployment_statuses = []
        for info_proto in proto.deployment_statuses.deployment_status_infos:
            deployment_statuses.append(DeploymentStatusInfo.from_proto(info_proto))

        # Recreate StatusInfo
        return cls(
            app_status=app_status,
            deployment_statuses=deployment_statuses,
            name=proto.name,
        )


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
    api_type: APIType


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        name: str,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
        logging_config: LoggingConfig,
    ):
        """
        Args:
            name: Application name.
            deployment_state_manager: State manager for all deployments
                in the cluster.
            endpoint_state: State manager for endpoints in the system.
        """

        self._name = name
        self._status_msg = ""
        self._deployment_state_manager = deployment_state_manager
        self._endpoint_state = endpoint_state
        self._route_prefix: Optional[str] = None
        self._ingress_deployment_name: Optional[str] = None

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
            api_type=APIType.UNKNOWN,
        )
        self._logging_config = logging_config

    @property
    def route_prefix(self) -> Optional[str]:
        return self._route_prefix

    @property
    def docs_path(self) -> Optional[str]:
        # get the docs path from the running deployments
        # we are making an assumption that the docs path can only be set
        # on ingress deployments with fastapi.
        ingress_deployment = DeploymentID(self._ingress_deployment_name, self._name)
        return self._deployment_state_manager.get_deployment_docs_path(
            ingress_deployment
        )

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
    def deployment_timestamp(self) -> float:
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

    @property
    def api_type(self) -> APIType:
        return self._target_state.api_type

    def recover_target_state_from_checkpoint(
        self, checkpoint_data: ApplicationTargetState
    ):
        logger.info(
            f"Recovering target state for application '{self._name}' from checkpoint."
        )
        self._set_target_state(
            checkpoint_data.deployment_infos,
            api_type=checkpoint_data.api_type,
            code_version=checkpoint_data.code_version,
            target_config=checkpoint_data.config,
            target_capacity=checkpoint_data.target_capacity,
            target_capacity_direction=checkpoint_data.target_capacity_direction,
            deleting=checkpoint_data.deleting,
        )

        # Restore route prefix and docs path from checkpointed deployments when
        # the imperatively started application is restarting with controller.
        if checkpoint_data.deployment_infos is not None:
            self._route_prefix = self._check_routes(checkpoint_data.deployment_infos)

    def _set_target_state(
        self,
        deployment_infos: Optional[Dict[str, DeploymentInfo]],
        *,
        api_type: APIType,
        code_version: Optional[str],
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
            api_type=api_type,
        )

        self._target_state = target_state

    def _set_target_state_deleting(self):
        """Set target state to deleting.

        Wipes the target deployment infos, code version, and config.
        """
        self._set_target_state(
            deployment_infos={},
            api_type=self._target_state.api_type,
            code_version=None,
            target_config=None,
            deleting=True,
        )

    def _clear_target_state_and_store_config(
        self,
        target_config: Optional[ServeApplicationSchema],
    ):
        """Clears the target state and stores the config.

        NOTE: this currently assumes that this method is *only* called when managing
        apps deployed with the declarative API.
        """
        self._set_target_state(
            deployment_infos=None,
            api_type=APIType.DECLARATIVE,
            code_version=None,
            target_config=target_config,
            deleting=False,
        )

    def _delete_deployment(self, name):
        id = DeploymentID(name=name, app_name=self._name)
        self._endpoint_state.delete_endpoint(id)
        self._deployment_state_manager.delete_deployment(id)

    def delete(self):
        """Delete the application"""
        if self._status != ApplicationStatus.DELETING:
            logger.info(
                f"Deleting app '{self._name}'.",
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

        deployment_id = DeploymentID(name=deployment_name, app_name=self._name)

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

    def deploy_app(self, deployment_infos: Dict[str, DeploymentInfo]):
        """(Re-)deploy the application from list of deployment infos.

        This function should only be called to deploy an app from an
        imperative API (i.e., `serve.run` or Java API).

        Raises: RayServeException if there is more than one route prefix
            or docs path.
        """

        self._check_ingress_deployments(deployment_infos)
        # Check routes are unique in deployment infos
        self._route_prefix = self._check_routes(deployment_infos)

        self._set_target_state(
            deployment_infos=deployment_infos,
            api_type=APIType.IMPERATIVE,
            code_version=None,
            target_config=None,
            target_capacity=None,
            target_capacity_direction=None,
        )

    def apply_app_config(
        self,
        config: ServeApplicationSchema,
        target_capacity: Optional[float],
        target_capacity_direction: Optional[TargetCapacityDirection],
        deployment_time: float,
    ) -> None:
        """Apply the config to the application.

        If the code version matches that of the current live deployments
        then it only applies the updated config to the deployment state
        manager. If the code version doesn't match, this will re-build
        the application.

        This function should only be called to (re-)deploy an app from
        the declarative API (i.e., through the REST API).
        """

        self._deployment_timestamp = deployment_time

        config_version = get_app_code_version(config)
        if config_version == self._target_state.code_version:
            try:
                overrided_infos = override_deployment_info(
                    self._target_state.deployment_infos,
                    config,
                )
                self._route_prefix = self._check_routes(overrided_infos)
                self._set_target_state(
                    # Code version doesn't change.
                    code_version=self._target_state.code_version,
                    api_type=APIType.DECLARATIVE,
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
                        f"Unexpected error occurred while applying config for "
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

            # Record telemetry for container runtime env feature
            if self._target_state.config.runtime_env.get(
                "container"
            ) or self._target_state.config.runtime_env.get("image_uri"):
                ServeUsageTag.APP_CONTAINER_RUNTIME_ENV_USED.record("1")

            # Kick off new build app task
            logger.info(f"Importing and building app '{self._name}'.")
            build_app_obj_ref = build_serve_application.options(
                runtime_env=config.runtime_env,
                enable_task_events=RAY_SERVE_ENABLE_TASK_EVENTS,
            ).remote(
                config.import_path,
                config_version,
                config.name,
                config.args,
                self._logging_config,
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

        # Get the lowest rank, i.e. highest priority, deployment status info object
        # The deployment status info with highest priority determines the corresponding
        # application status to set.
        deployment_statuses = self.get_deployments_statuses()
        lowest_rank_status = min(deployment_statuses, key=lambda info: info.rank)
        if lowest_rank_status.status == DeploymentStatus.DEPLOY_FAILED:
            failed_deployments = [
                s.name
                for s in deployment_statuses
                if s.status == DeploymentStatus.DEPLOY_FAILED
            ]
            return (
                ApplicationStatus.DEPLOY_FAILED,
                f"Failed to update the deployments {failed_deployments}.",
            )
        elif lowest_rank_status.status == DeploymentStatus.UNHEALTHY:
            unhealthy_deployment_names = [
                s.name
                for s in deployment_statuses
                if s.status == DeploymentStatus.UNHEALTHY
            ]
            return (
                ApplicationStatus.UNHEALTHY,
                f"The deployments {unhealthy_deployment_names} are UNHEALTHY.",
            )
        elif lowest_rank_status.status == DeploymentStatus.UPDATING:
            return ApplicationStatus.DEPLOYING, ""
        elif (
            lowest_rank_status.status
            in [DeploymentStatus.UPSCALING, DeploymentStatus.DOWNSCALING]
            and lowest_rank_status.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        ):
            return ApplicationStatus.DEPLOYING, ""
        else:
            return ApplicationStatus.RUNNING, ""

    def _reconcile_build_app_task(self) -> Tuple[Optional[Dict], BuildAppStatus, str]:
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
                logger.info(f"Imported and built app '{self._name}' successfully.")
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
                f"Unexpected error occurred while deploying application "
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
                deployment_infos, self._build_app_task_info.config
            )
            self._route_prefix = self._check_routes(overrided_infos)
            return overrided_infos, BuildAppStatus.SUCCEEDED, ""
        except (TypeError, ValueError, RayServeException):
            return None, BuildAppStatus.FAILED, traceback.format_exc()
        except Exception:
            error_msg = (
                f"Unexpected error occurred while applying config for application "
                f"'{self._name}': \n{traceback.format_exc()}"
            )
            return None, BuildAppStatus.FAILED, error_msg

    def _check_ingress_deployments(
        self, deployment_infos: Dict[str, DeploymentInfo]
    ) -> None:
        """Check @serve.ingress of deployments in app.

        Raises: RayServeException if more than one @serve.ingress
            is found among deployments.
        """
        num_ingress_deployments = 0
        for info in deployment_infos.values():
            if inspect.isclass(info.replica_config.deployment_def) and issubclass(
                info.replica_config.deployment_def, ASGIAppReplicaWrapper
            ):
                num_ingress_deployments += 1

        if num_ingress_deployments > 1:
            raise RayServeException(
                f'Found multiple FastAPI deployments in application "{self._name}".'
                "Please only include one deployment with @serve.ingress"
                "in your application to avoid this issue."
            )

    def _check_routes(
        self, deployment_infos: Dict[str, DeploymentInfo]
    ) -> Tuple[str, str]:
        """Check route prefixes of deployments in app.

        There should only be one non-null route prefix. If there is one,
        set it as the application route prefix. This function must be
        run every control loop iteration because the target config could
        be updated without kicking off a new task.

        Returns: route prefix.
        Raises: RayServeException if more than one route prefix is found among deployments.
        """
        num_route_prefixes = 0
        route_prefix = None
        for info in deployment_infos.values():
            # Update route prefix of application, which may be updated
            # through a redeployed config.
            if info.route_prefix is not None:
                route_prefix = info.route_prefix
                num_route_prefixes += 1

        if num_route_prefixes > 1:
            raise RayServeException(
                f'Found multiple route prefixes from application "{self._name}",'
                " Please specify only one route prefix for the application "
                "to avoid this issue."
            )

        return route_prefix

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
                api_type=self._target_state.api_type,
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
            DeploymentID(name=deployment, app_name=self._name)
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
                DeploymentID(name=deployment_name, app_name=self._name)
            )
            for deployment_name in self.target_deployments
        }
        return {k: v for k, v in details.items() if v is not None}

    def _update_status(self, status: ApplicationStatus, status_msg: str = "") -> None:
        if (
            status_msg
            and status
            in [
                ApplicationStatus.DEPLOY_FAILED,
                ApplicationStatus.UNHEALTHY,
            ]
            and status_msg != self._status_msg
        ):
            logger.error(status_msg)

        self._status = status
        self._status_msg = status_msg


class ApplicationStateManager:
    def __init__(
        self,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
        kv_store: KVStoreBase,
        logging_config: LoggingConfig,
    ):
        self._deployment_state_manager = deployment_state_manager
        self._endpoint_state = endpoint_state
        self._kv_store = kv_store
        self._logging_config = logging_config

        self._shutting_down = False

        self._application_states: Dict[str, ApplicationState] = {}
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
                    self._logging_config,
                )
                app_state.recover_target_state_from_checkpoint(checkpoint_data)
                self._application_states[app_name] = app_state

    def delete_app(self, name: str) -> None:
        """Delete application by name"""
        if name not in self._application_states:
            return
        self._application_states[name].delete()

    def deploy_apps(self, name_to_deployment_args: Dict[str, List[Dict]]) -> None:
        live_route_prefixes: Dict[str, str] = {
            app_state.route_prefix: app_name
            for app_name, app_state in self._application_states.items()
            if app_state.route_prefix is not None
            and not app_state.status == ApplicationStatus.DELETING
        }

        for name, deployment_args in name_to_deployment_args.items():
            for deploy_param in deployment_args:
                # Make sure route_prefix is not being used by other application.
                deploy_app_prefix = deploy_param.get("route_prefix")
                if deploy_app_prefix is None:
                    continue

                existing_app_name = live_route_prefixes.get(deploy_app_prefix)
                # It's ok to redeploy an app with the same prefix
                # if it has the same name as the app already using that prefix.
                if existing_app_name is not None and existing_app_name != name:
                    raise RayServeException(
                        f"Prefix {deploy_app_prefix} is being used by application "
                        f'"{existing_app_name}". Failed to deploy application "{name}".'
                    )

                # We might be deploying more than one app,
                # so we need to add this app's prefix to the
                # set of live route prefixes that we're checking
                # against during this batch operation.
                live_route_prefixes[deploy_app_prefix] = name

            if name not in self._application_states:
                self._application_states[name] = ApplicationState(
                    name,
                    self._deployment_state_manager,
                    self._endpoint_state,
                    self._logging_config,
                )
            ServeUsageTag.NUM_APPS.record(str(len(self._application_states)))

            deployment_infos = {
                params["deployment_name"]: deploy_args_to_deployment_info(
                    **params, app_name=name
                )
                for params in deployment_args
            }
            self._application_states[name].deploy_app(deployment_infos)

    def deploy_app(self, name: str, deployment_args: List[Dict]) -> None:
        """Deploy the specified app to the list of deployment arguments.

        This function should only be called if the app is being deployed
        through serve.run instead of from a config.

        Args:
            name: application name
            deployment_args_list: arguments for deploying a list of deployments.

        Raises:
            RayServeException: If the list of deployments is trying to
                use a route prefix that is already used by another application
        """
        self.deploy_apps({name: deployment_args})

    def apply_app_configs(
        self,
        app_configs: List[ServeApplicationSchema],
        *,
        deployment_time: float = 0,
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
    ):
        """Declaratively apply the list of application configs.

        The applications will be reconciled to match the target state of the config.

        Any applications previously deployed declaratively that are *not* present in
        the list will be deleted.
        """
        for app_config in app_configs:
            if app_config.name not in self._application_states:
                logger.info(f"Deploying new app '{app_config.name}'.")
                self._application_states[app_config.name] = ApplicationState(
                    app_config.name,
                    self._deployment_state_manager,
                    endpoint_state=self._endpoint_state,
                    logging_config=self._logging_config,
                )

            self._application_states[app_config.name].apply_app_config(
                app_config,
                target_capacity,
                target_capacity_direction,
                deployment_time=deployment_time,
            )

        # Delete all apps that were previously deployed via the declarative API
        # but are not in the config being applied.
        existing_apps = {
            name
            for name, app_state in self._application_states.items()
            if app_state.api_type == APIType.DECLARATIVE
        }
        apps_in_config = {app_config.name for app_config in app_configs}
        for app_to_delete in existing_apps - apps_in_config:
            self.delete_app(app_to_delete)

        ServeUsageTag.NUM_APPS.record(str(len(self._application_states)))

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

    def get_app_source(self, name: str) -> APIType:
        return self._application_states[name].api_type

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
        self._shutting_down = True

        for app_state in self._application_states.values():
            app_state.delete()

        self._kv_store.delete(CHECKPOINT_KEY)

    def is_ready_for_shutdown(self) -> bool:
        """Return whether all applications have shut down.

        Iterate through all application states and check if all their applications
        are deleted.
        """
        return self._shutting_down and all(
            app_state.is_deleted() for app_state in self._application_states.values()
        )

    def save_checkpoint(self) -> None:
        """Write a checkpoint of all application states."""
        if self._shutting_down:
            # Once we're told to shut down, stop writing checkpoints.
            # Calling .shutdown() deletes any existing checkpoint.
            return

        application_state_info = {
            app_name: app_state.get_checkpoint_data()
            for app_name, app_state in self._application_states.items()
        }

        self._kv_store.put(
            CHECKPOINT_KEY,
            cloudpickle.dumps(application_state_info),
        )


@ray.remote(num_cpus=0, max_calls=1)
def build_serve_application(
    import_path: str,
    code_version: str,
    name: str,
    args: Dict,
    logging_config: LoggingConfig,
) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """Import and build a Serve application.

    Args:
        import_path: import path to top-level bound deployment.
        code_version: code version inferred from app config. All
            deployment versions are set to this code version.
        name: application name. If specified, application will be deployed
            without removing existing applications.
        args: Arguments to be passed to the application builder.
        logging_config: the logging config for the build app task.
    Returns:
        Deploy arguments: a list of deployment arguments if application
            was built successfully, otherwise None.
        Error message: a string if an error was raised, otherwise None.
    """
    configure_component_logger(
        component_name="controller",
        component_id=f"build_{name}_{os.getpid()}",
        logging_config=logging_config,
    )

    try:
        from ray.serve._private.api import call_user_app_builder_with_args_if_necessary

        # Import and build the application.
        args_info_str = f" with arguments {args}" if args else ""
        logger.info(f"Importing application '{name}'{args_info_str}.")

        app = call_user_app_builder_with_args_if_necessary(
            import_attr(import_path), args
        )

        deploy_args_list = []
        built_app: BuiltApplication = build_app(
            app,
            name=name,
            default_runtime_env=ray.get_runtime_context().runtime_env,
        )
        num_ingress_deployments = 0
        for deployment in built_app.deployments:
            if inspect.isclass(deployment.func_or_class) and issubclass(
                deployment.func_or_class, ASGIAppReplicaWrapper
            ):
                num_ingress_deployments += 1
            is_ingress = deployment.name == built_app.ingress_deployment_name
            deploy_args_list.append(
                get_deploy_args(
                    name=deployment._name,
                    replica_config=deployment._replica_config,
                    ingress=is_ingress,
                    deployment_config=deployment._deployment_config,
                    version=code_version,
                    route_prefix="/" if is_ingress else None,
                )
            )
        if num_ingress_deployments > 1:
            return None, (
                f'Found multiple FastAPI deployments in application "{built_app.name}". '
                "Please only include one deployment with @serve.ingress "
                "in your application to avoid this issue."
            )
        return deploy_args_list, None
    except KeyboardInterrupt:
        # Error is raised when this task is canceled with ray.cancel(), which
        # happens when deploy_apps() is called.
        logger.info(
            "Existing config deployment request terminated because of keyboard "
            "interrupt."
        )
        return None, None
    except Exception:
        logger.error(
            f"Exception importing application '{name}'.\n{traceback.format_exc()}"
        )
        return None, traceback.format_exc()


def override_deployment_info(
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
        if "max_ongoing_requests" in options:
            options["max_ongoing_requests"] = options.get("max_ongoing_requests")

        deployment_name = options["name"]
        if deployment_name not in deployment_infos:
            raise ValueError(
                f"Deployment '{deployment_name}' does not exist. "
                f"Available: {list(deployment_infos.keys())}"
            )

        info = deployment_infos[deployment_name]
        original_options = info.deployment_config.dict()
        original_options["user_configured_option_names"].update(set(options))

        # Override `max_ongoing_requests` and `autoscaling_config` if
        # `num_replicas="auto"`
        if options.get("num_replicas") == "auto":
            options["num_replicas"] = None

            new_config = AutoscalingConfig.default().dict()
            # If `autoscaling_config` is specified, its values override
            # the default `num_replicas="auto"` configuration
            autoscaling_config = (
                options.get("autoscaling_config")
                or info.deployment_config.autoscaling_config
            )
            if autoscaling_config:
                new_config.update(autoscaling_config)

            options["autoscaling_config"] = AutoscalingConfig(**new_config)

            ServeUsageTag.AUTO_NUM_REPLICAS_USED.record("1")

        # What to pass to info.update
        override_options = {}

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

        # Record telemetry for container runtime env feature at deployment level
        if override_actor_options.get("runtime_env") and (
            override_actor_options["runtime_env"].get("container")
            or override_actor_options["runtime_env"].get("image_uri")
        ):
            ServeUsageTag.DEPLOYMENT_CONTAINER_RUNTIME_ENV_USED.record("1")

        merged_env = override_runtime_envs_except_env_vars(
            app_runtime_env, override_actor_options.get("runtime_env", {})
        )
        override_actor_options.update({"runtime_env": merged_env})

        replica_config.update(
            ray_actor_options=override_actor_options,
            placement_group_bundles=override_placement_group_bundles,
            placement_group_strategy=override_placement_group_strategy,
            max_replicas_per_node=override_max_replicas_per_node,
        )
        override_options["replica_config"] = replica_config

        # Override deployment config options
        options.pop("name", None)
        original_options.update(options)
        override_options["deployment_config"] = DeploymentConfig(**original_options)
        deployment_infos[deployment_name] = info.update(**override_options)

        deployment_config = deployment_infos[deployment_name].deployment_config
        if (
            deployment_config.autoscaling_config is not None
            and deployment_config.max_ongoing_requests
            < deployment_config.autoscaling_config.get_target_ongoing_requests()
        ):
            logger.warning(
                "Autoscaling will never happen, "
                "because 'max_ongoing_requests' is less than "
                "'target_ongoing_requests' now."
            )

    # Overwrite ingress route prefix
    app_route_prefix = config_dict.get("route_prefix", DEFAULT.VALUE)
    validate_route_prefix(app_route_prefix)
    for deployment in list(deployment_infos.values()):
        if (
            app_route_prefix is not DEFAULT.VALUE
            and deployment.route_prefix is not None
        ):
            deployment.route_prefix = app_route_prefix

    return deployment_infos
