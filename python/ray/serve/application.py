from typing import List, Dict, TextIO, Optional, Union
import json
import yaml
import time
import sys

import ray
from ray import serve
from ray.serve.api import (
    Deployment,
    _get_global_client,
)
from ray.serve.common import DeploymentStatus
from ray.dashboard.modules.serve.schema import (
    ServeApplicationSchema,
    serve_application_to_schema,
    schema_to_serve_application,
    serve_application_status_to_schema,
)
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve.utils import logger


class Application:
    def __init__(self, deployments: List[Deployment] = None):
        """
        Initializes a new Serve Application, which contains a group of
        deployments.

        Args:
            deployments (List[Deployment]): The Serve Application is
                initialized to contain this group of deployments.
        """
        deployments = deployments or []

        self._deployments = dict()
        for idx, d in enumerate(deployments):
            if isinstance(d, Deployment):
                self._deployments[d.name] = d
            else:
                raise TypeError(
                    f"Got object of type {type(d)} at index {idx}. "
                    "Expected only Deployment objects to be passed in."
                )

    def add_deployment(self, deployment: Deployment):
        """Add deployment to the list. Validate name uniqueness."""

        if not isinstance(deployment, Deployment):
            raise TypeError(f"Got {type(deployment)}. Expected deployment.")

        self._deployments[deployment.name] = deployment

    def deploy(self, blocking: bool = True):
        """Async deploy."""

        if len(self._deployments) == 0:
            return

        parameter_group = []

        for deployment in self._deployments.values():
            if not isinstance(deployment, Deployment):
                raise TypeError(
                    f"deploy_group only accepts Deployments, but got unexpected "
                    f"type {type(deployment)}."
                )

            deployment_parameters = {
                "name": deployment._name,
                "func_or_class": deployment._func_or_class,
                "init_args": deployment.init_args,
                "init_kwargs": deployment.init_kwargs,
                "ray_actor_options": deployment._ray_actor_options,
                "config": deployment._config,
                "version": deployment._version,
                "prev_version": deployment._prev_version,
                "route_prefix": deployment.route_prefix,
                "url": deployment.url,
            }

            parameter_group.append(deployment_parameters)

        return _get_global_client().deploy_group(parameter_group, _blocking=blocking)

    @staticmethod
    def get_statuses() -> Dict[str, DeploymentStatus]:
        """Get current status of all deployments."""

        return _get_global_client().get_deployment_statuses()

    def run(
        self,
        runtime_env_updates=None,
        cluster_address="auto",
        dashboard_address="http://localhost:8265",
        logger=logger,
    ):
        """Blocking run."""

        try:
            runtime_env_updates = runtime_env_updates or {}

            ray.init(address=cluster_address, namespace="serve")
            serve.start(detached=True)
            ServeSubmissionClient(dashboard_address)._upload_working_dir_if_needed(
                runtime_env_updates
            )

            for deployment in self._deployments.values():
                self._configure_runtime_env(deployment, runtime_env_updates)
            self.deploy()

            logger.info("\nDeployed successfully!\n")

            while True:
                statuses = serve_application_status_to_schema(self.get_statuses()).json(
                    indent=4
                )
                logger.info(f"{statuses}")
                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("Got SIGINT (KeyboardInterrupt). Removing deployments.")
            for deployment in self._deployments.values():
                deployment.delete()
            if len(serve.list_deployments()) == 0:
                logger.info("No deployments left. Shutting down Serve.")
                serve.shutdown()
            sys.exit()

    def to_json(self, f: Optional[TextIO] = None) -> str:
        """Write list of deployments to json str or file."""

        json_str = serve_application_to_schema(self._deployments).json(indent=4)

        if f:
            f.write(json_str)
        return json_str

    @classmethod
    def from_json(cls, str_or_file: Union[str, TextIO]) -> "Application":
        """Load list of deployments from json str or file."""

        if isinstance(str_or_file, str):
            schema = ServeApplicationSchema.parse_raw(
                str_or_file, content_type="application/json"
            )
        else:
            schema = ServeApplicationSchema.parse_obj(json.load(str_or_file))

        return cls(schema_to_serve_application(schema))

    def to_yaml(self, f: Optional[TextIO]) -> Optional[str]:
        """Write list of deployments to yaml str or file."""

        json_str = serve_application_to_schema(self._deployments).json(indent=4)

        if f:
            yaml.safe_dump(json_str, f)
        return yaml.safe_dump(json_str)

    @classmethod
    def from_yaml(cls, str_or_file: Union[str, TextIO]) -> "Application":
        """Load list of deployments from yaml str or file."""

        deployments_json = yaml.safe_load(str_or_file)
        schema = ServeApplicationSchema.parse_obj(deployments_json)
        return cls(schema_to_serve_application(schema))

    def _configure_runtime_env(self, deployment: Deployment, updates: Dict):
        """
        Overwrites deployment's runtime_env with fields in updates. Any fields
        in deployment's runtime_env that aren't in updates stay the same.
        """

        if deployment.ray_actor_options is None:
            deployment._ray_actor_options = {"runtime_env": updates}
        elif "runtime_env" in deployment.ray_actor_options:
            deployment.ray_actor_options["runtime_env"].update(updates)
        else:
            deployment.ray_actor_options["runtime_env"] = updates

    def __getitem__(self, key: str):
        """Fetch deployment by name using dict syntax: app["name"]"""

        if key in self._deployments:
            return self._deployments[key]
        else:
            raise KeyError(f'Serve application does not contain a "{key}" deployment.')

    def __getattr__(self, name: str):
        """Fetch deployment by name using attributes: app.name"""

        if name in self._deployments:
            return self._deployments[name]
        else:
            raise AttributeError(
                f'Serve application does not contain a "{name}" deployment.'
            )

    def __iter__(self):
        """
        Iterator over Application's deployments.
        Enables "for deployment in Application" pattern.
        """

        return iter(self._deployments.values())
