from dataclasses import dataclass
import hashlib
import json
import logging
import time
import traceback

# import time
from typing import Dict, List, Optional
from collections import Counter

import ray
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

# from ray.types import ObjectRef
from ray.exceptions import RayTaskError, RuntimeEnvSetupError
from ray._private.utils import import_attr
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
from ray.serve.schema import DeploymentDetails, ServeApplicationSchema
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)
from ray.serve._private.utils import override_runtime_envs_except_env_vars, DEFAULT
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info
from ray.serve.exceptions import RayServeException

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class ApplicationTargetState:
    deployment_params: Optional[Dict]
    code_version: Optional[str]
    deleting: bool


class ApplicationState:
    """Manage single application states with all operations"""

    def __init__(
        self,
        name: str,
        deployment_state_manager: DeploymentStateManager,
        endpoint_state: EndpointState,
    ):
        """
        Args:
            name: application name
            deployment_state_manager: deployment state manager which is used for
                fetching deployment information
            deployment_time: Deployment timestamp
        """
        self.name = name
        self.endpoint_state = endpoint_state
        self.deployment_state_manager = deployment_state_manager

        self.ready_to_be_deleted = False
        self.app_msg = ""
        self.route_prefix = None
        self.docs_path = None

        # Manage target state
        self._config = None
        self._build_app_obj_ref = None
        # self._target_deployment_params: Optional[List[Dict]] = None
        self._target_state: ApplicationTargetState = ApplicationTargetState(
            deployment_params=None, code_version=None, deleting=False
        )
        self._target_code_version: str = None

        self.status: ApplicationStatus = ApplicationStatus.NOT_STARTED
        # This set tracks old deployments that are being deleted
        self.deployments_to_delete = set()

    def apply_deployment(
        self,
        name: str,
        route_prefix: Optional[str],
        deployment_info: DeploymentInfo,
    ) -> bool:
        """
        Deploys a deployment in the application.

        Returns: Whether the deployment is being updated.
        """
        if route_prefix is not None:
            assert route_prefix.startswith("/")

        updating = self.deployment_state_manager.deploy(name, deployment_info)

        if route_prefix is not None:
            endpoint_info = EndpointInfo(route=route_prefix)
            self.endpoint_state.update_endpoint(name, endpoint_info)
        else:
            self.endpoint_state.delete_endpoint(name)

        return updating

    def deploy(
        self,
        config: ServeApplicationSchema,
        deployment_time: int,
    ):
        """Deploy from a config."""

        if config == self._config:
            return

        self._config = config
        # self._target_code_version = _get_app_version(config)
        self._target_state.code_version = _get_app_version(config)

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
        # self.deployments_to_delete = set(self.deployments).difference(
        #     {params["name"] for params in deployment_params}
        # )
        # for deployment_name in self.deployments_to_delete:
        #     self.delete_deployment(deployment_name)
        self._target_deployment_params = deployment_params

        # Deploy new deployments
        updating = []
        for deployment in deployment_params:
            updating.append(self.deploy_deployment(**deployment))

        self.status = ApplicationStatus.DEPLOYING
        return updating

    # def _process_terminating_deployments(self):
    #     """Update the tracking for all deployments being deleted

    #     When a deployment's status is None, the deployment will be
    #     removed from application.
    #     """
    #     for name in list(self.deployments_to_delete):
    #         if self.deployment_state_manager.get_deployment(name):
    #             logger.warning(
    #                 f"Deleting deployment {name} from application {self.name}."
    #             )
    #         else:
    #             self.deployments_to_delete.remove(name)

    def _build_app_obj_ref_is_ready(self) -> bool:
        if not self._build_app_obj_ref:
            return False

        finished, _ = ray.wait([self._build_app_obj_ref], timeout=0)
        if finished:
            return True
        return False

    def _check_curr_status(self):
        deployment_statuses = Counter(
            info.status
            for info in self.deployment_state_manager.get_deployment_statuses(
                self.deployments
            )
        )
        if deployment_statuses[DeploymentStatus.UNHEALTHY]:
            self.status = ApplicationStatus.DEPLOY_FAILED
        elif deployment_statuses[DeploymentStatus.HEALTHY] == len(self.deployments):
            self.status = ApplicationStatus.RUNNING

    # def _update_target_state(self):
    #     if self._build_app_obj_ref_is_ready():
    #         try:
    #             self._target_deployment_params = ray.get(self._build_app_obj_ref)
    #         except RayTaskError as e:
    #             # NOTE(zcin): we should use str(e) instead of traceback.format_exc()
    #             # here because the full details of the error is not displayed
    #             # properly with traceback.format_exc(). RayTaskError has its own
    #             # custom __str__ function.
    #             self.app_msg = f"Deploying app '{self.name}' failed:\n{str(e)}"
    #             self.status = ApplicationStatus.DEPLOY_FAILED
    #             logger.warning(self.app_msg)
    #         except RuntimeEnvSetupError:
    #             self.app_msg = (
    #                 f"Runtime env setup for app '{self.name}' "
    #                 f"failed:\n{traceback.format_exc()}"
    #             )
    #             self.status = ApplicationStatus.DEPLOY_FAILED
    #             logger.warning(self.app_msg)

    # def _deploy_or_update_deployments(self):
    #     """Deploy or update deployments to reach target state."""
    #     # Got the deployment params built from user source code
    #     for params in self._target_deployment_params:
    #         deployment_name = params["name"]
    #         previous_deployment = self.deployment_state_manager.get_deployment(
    #             deployment_name
    #         )
    #         deployment_info = deploy_args_to_deployment_info(
    #             app_name=self.name,
    #             previous_deployment=previous_deployment,
    #             **params,
    #         )
    #         if deployment_name not in self.deployment_state_manager._deployment_states:
    #             print(f"deploying {deployment_name} because it doesn't exist yet")
    #             self.deploy_deployment(
    #                 deployment_name, params["route_prefix"], deployment_info
    #             )
    #         elif self.should_update(deployment_info, previous_deployment):
    #             print(f"updating {deployment_name} because config changed")
    #             self.deploy_deployment(
    #                 deployment_name, params["route_prefix"], deployment_info
    #             )

    #     live_deployments = self.deployment_state_manager.get_deployments_in_application(
    #         self.name
    #     )
    #     for deployment_name in live_deployments:
    #         if deployment_name not in self.deployments:
    #             self.delete_deployment(deployment_name)

    #     self._check_curr_status()

    def delete_deployment(self, name):
        self.endpoint_state.delete_endpoint(name)
        print("telling deployment state manager to delete", name)
        self.deployment_state_manager.delete_deployment(name)

    def delete(self):
        """Delete the application"""
        self.status = ApplicationStatus.DELETING
        for deployment in self.deployments:
            self.delete_deployment(deployment)
        self._target_deployment_params = []

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
            live_deployments = (
                self.deployment_state_manager.get_deployments_in_application(self.name)
            )
            self.ready_to_be_deleted = len(live_deployments) == 0
            return

        # -------------------------------------------------------------------

        # self._update_target_state()

        if (
            self._target_state.deployment_params is None
            or self._target_state.deployment_params[0]["version"]
            != self._target_state.code_version
        ):
            # if self._build_app_obj_ref and not self.status == ApplicationStatus.RUNNING:
            #     logger.info(
            #         f"Received new config deployment for {self.name} request. Cancelling "
            #         "previous request."
            #     )
            #     ray.cancel(self._build_app_obj_ref)

            logger.info(f"Starting build_application task for app {self.name}.")
            self.deployment_timestamp = time.time()
            self._build_app_obj_ref = build_application.options(
                runtime_env=self._config.runtime_env
            ).remote(
                self.name,
                self._config.dict(exclude_unset=True).get(
                    "route_prefix", DEFAULT.VALUE
                ),
                self._config.import_path,
                self._config.runtime_env,
                self._config.dict(exclude_unset=True).get("deployments", []),
                self._target_code_version,
            )
            self.status = ApplicationStatus.DEPLOYING

        # If app was built successfully, fetch the results and set the target state
        if self._build_app_obj_ref_is_ready():
            try:
                deployment_params, code_version = ray.get(self._build_app_obj_ref)
                self._target_state = ApplicationTargetState(
                    deployment_params, code_version, False
                )
                self._build_app_obj_ref = None
            except RayTaskError as e:
                # NOTE(zcin): we should use str(e) instead of traceback.format_exc()
                # here because the full details of the error is not displayed
                # properly with traceback.format_exc(). RayTaskError has its own
                # custom __str__ function.
                self.app_msg = f"Deploying app '{self.name}' failed:\n{str(e)}"
                self.status = ApplicationStatus.DEPLOY_FAILED
                logger.warning(self.app_msg)
            except RuntimeEnvSetupError:
                self.app_msg = (
                    f"Runtime env setup for app '{self.name}' "
                    f"failed:\n{traceback.format_exc()}"
                )
                self.status = ApplicationStatus.DEPLOY_FAILED
                logger.warning(self.app_msg)

        # Reconcile target state!
        target_deployment_params = self._target_state.deployment_params
        code_version = self._target_state.code_version
        if target_deployment_params is not None:
            # Deploy/update deployments
            for params in target_deployment_params:
                deployment_name = params["name"]
                previous_deployment = self.get_deployment(deployment_name)
                deployment_info = deploy_args_to_deployment_info(
                    app_name=self.name,
                    previous_deployment=previous_deployment,
                    **params,
                )

                self.apply_deployment(
                    deployment_name, params["route_prefix"], deployment_info
                )

            # Delete deployments
            live_deployments = (
                self.deployment_state_manager.get_deployments_in_application(self.name)
            )
            for deployment_name in live_deployments:
                if deployment_name not in self.deployments:
                    self.delete_deployment(deployment_name)

            self._check_curr_status()

    @property
    def deployments(self) -> List[str]:
        """Names of all deployments in application."""
        if self._target_deployment_params is None:
            return []
        return [params["name"] for params in self._target_deployment_params]

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

    def deploy(
        self,
        name: str,
        config: ServeApplicationSchema,
        deployment_time: float = 0,
    ):
        """
        Something that sounds cool.
        """
        if name not in self._application_states:
            self._application_states[name] = ApplicationState(
                name,
                self.deployment_state_manager,
                self.endpoint_state,
            )

        record_extra_usage_tag(
            TagKey.SERVE_NUM_APPS, str(len(self._application_states))
        )
        self._application_states[name].deploy(config, deployment_time)

    def apply_deployment_args(
        self, name: str, deployment_args_list: List[Dict]
    ) -> List[bool]:
        """Apply deployment arguments.

        Args:
            name: application name
            deployment_args: deployment arguments needed for deploying a list of d
                deployments.
        Returns:
            Returns a list indicating whether each deployment is being updated.
        """

        # Make sure route_prefix is not being used by other application.
        # live_route_prefixes: Dict[str, str] = {
        #     self._application_states[app_name].route_prefix: app_name
        #     for app_name, app_state in self._application_states.items()
        #     if app_state.route_prefix is not None
        #     and not app_state.status == ApplicationStatus.DELETING
        #     and name != app_name
        # }

        # for deploy_param in deployment_args_list:
        #     deploy_app_prefix = deploy_param.get("route_prefix")
        #     if deploy_app_prefix in live_route_prefixes:
        #         raise RayServeException(
        #             f"Prefix {deploy_app_prefix} is being used by application "
        #             f'"{live_route_prefixes[deploy_app_prefix]}".'
        #             f' Failed to deploy application "{name}".'
        #         )

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

        if len(apps_to_be_deleted) > 0:
            for app_name in apps_to_be_deleted:
                del self._application_states[app_name]
            record_extra_usage_tag(
                TagKey.SERVE_NUM_APPS, str(len(self._application_states))
            )

    def shutdown(self):
        """Shutdown all applications."""
        for application_state in self._application_states.values():
            application_state.delete()


def _get_app_version(app_config: ServeApplicationSchema) -> bool:
    """This function determines the code version of an application based on the import
    path and runtime env.
    """
    encoded = json.dumps(
        {"import_path": app_config.import_path, "runtime_env": app_config.runtime_env},
        sort_keys=True,
    ).encode("utf-8")

    return hashlib.md5(encoded).hexdigest()


@ray.remote(num_cpus=0, max_calls=1)
def build_application(
    name: str,
    route_prefix: str,
    import_path: str,
    runtime_env: Dict,
    deployment_override_options: List[Dict],
    code_version: str,
):
    """Deploy Serve application from a user-provided config.

    Args:
        import_path: import path to ingress deployment.
        runtime_env: runtime environment for the application. This will also be the
            default runtime environment for each deployment unless overriden at the
            deployment level.
        deployment_override_options: Dictionary of options that overrides
            deployment options set in the graph's code itself.
        deployment_versions: Versions of each deployment, each of which is
            the same as the last deployment if it is a config update or
            a new randomly generated version if it is a code update
        name: unique name for the application.
        route_prefix: unique route prefix path for the application. If not DEFAULT.VALUE
            this will override the ingress deployment's route prefix.
    """
    try:
        from ray.serve.api import build

        # Import and build the application.
        app = build(import_attr(import_path), name)
        # Overwrite route prefix
        for deployment in list(app.deployments.values()):
            if (
                route_prefix is not DEFAULT.VALUE
                and deployment._route_prefix is not None
            ):
                deployment._route_prefix = route_prefix

        # Override options for each deployment.
        for options in deployment_override_options:
            deployment_name = options["name"]
            unique_deployment_name = (
                (name + DEPLOYMENT_NAME_PREFIX_SEPARATOR) if len(name) else ""
            ) + deployment_name

            if unique_deployment_name not in app.deployments:
                raise KeyError(
                    f'There is no deployment named "{deployment_name}" in the '
                    f'application "{name}".'
                )

            # Merge app-level and deployment-level runtime_envs.
            if "ray_actor_options" in options:
                # If specified, get ray_actor_options from config
                ray_actor_options = options["ray_actor_options"] or {}
            else:
                # Otherwise, get options from application code (and default to {}
                # if the code sets options to None).
                ray_actor_options = (
                    app.deployments[unique_deployment_name].ray_actor_options or {}
                )
            deployment_env = ray_actor_options.get("runtime_env", {})
            merged_env = override_runtime_envs_except_env_vars(
                runtime_env, deployment_env
            )
            ray_actor_options.update({"runtime_env": merged_env})
            options["ray_actor_options"] = ray_actor_options
            options["version"] = code_version  # TODO(zcin): FIGURE OUT IF THIS IS RIGHT
            options["name"] = unique_deployment_name
            # Update the deployment's options
            app.deployments[unique_deployment_name].set_options(
                **options, _internal=True
            )
        return (code_version, app.deploy_args_list)
    except KeyboardInterrupt:
        # Error is raised when this task is canceled with ray.cancel(), which
        # happens when deploy_apps() is called.
        logger.debug("Existing config deployment request terminated.")
