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
    """
    Contains a group of Serve deployments and manages their deployment to the
    Ray cluster.

    Args:
        deployments (List[Deployment]): The Serve Application is
            initialized to contain this group of deployments.
    """

    def __init__(self, deployments: List[Deployment] = None):
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
        """
        Adds the deployment to this Serve application. Validates that a
        deployment with the same name doesn't already exist. To overwrite an
        existing deployment, use attribute or index notation. For example:

        app.deployment_name = deployment
        app[deployment_name] = deployment

        Args:
            deployment (Deployment): deployment to add to this Application.

        Raises:
            ValueError: If a deployment with deployment.name already exists in
                this Application.
        """

        if not isinstance(deployment, Deployment):
            raise TypeError(f"Got {type(deployment)}. Expected deployment.")
        elif deployment.name in self._deployments:
            raise ValueError(
                f'Deployment with name "{deployment.name}" already '
                "exists in this application. To overwrite this "
                "deployment, use attribute or index notation. "
                "For example:\n\napp.deployment_name = "
                "new_deployment"
            )

        self._deployments[deployment.name] = deployment

    def deploy(self, blocking: bool = True):
        """
        Deploys all the deployments contained in this Application to the Ray
        cluster atomically. Deployments in this Application can carry handles
        to one another.

        Args:
            blocking (bool): If True, this function only returns after the
                deployment is finished. If False, this function returns
                immediately after requesting the deployment.
        """

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
        """
        Gets the currents statuses of all deployments running on the Ray cluster.
        NOTE: these deployments may not match the deployments stored in this
        Application since other Applications and deployments may have been
        deployed to the cluster.

        Returns:
            Dict[str, DeploymentStatus]: This dictionary maps the running
                deployment's name to a DeploymentStatus object containing its
                status and a message explaining the status.
        """

        return _get_global_client().get_deployment_statuses()

    def run(
        self,
        runtime_env_updates=None,
        cluster_address="auto",
        dashboard_address="http://localhost:8265",
        logger=logger,
    ):
        """
        Deploys all deployments in this Application and then keeps looping.
        This function must be manually killed (e.g. using ctrl-C). When it
        recieves a kill signal, it tears down all deployments that it deployed.
        If there are no deployments left, it also shuts down the rest of Serve,
        including the controller. This is meant to help interactive development.

        Args:
            runtime_env_updates (Dict): This is a runtime_env dictionary that
                overwrites all the runtime_envs of the deployments in this
                Application. Only the fields defined in runtime_env_updates
                are overwritten. Everything else is left alone.
            cluster_address (String): The Ray cluster head node's address.
                ray.init(address=cluster_address, namespace="serve") is called
                to attach to the cluster.
            dashboard_address (String): The cluster's dashboard address. This
                is used to upload any local working_dir passed in with
                runtime_env_updates.
            logger: Any Python object that implements the standard logger
                interface.
        """

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
        """
        Returns this Application's deployments as a JSON string. Optionally
        writes the JSON string to a file as well. To write to a file, use this
        pattern:

        with open("file_name.txt", "w") as f:
            app.to_json(f=f)

        This JSON adheres to the Serve REST API schema. It can be deployed
        via the Serve REST API.

        Args:
            f (Optional[TextIO]): A pointer to the file where the JSON should
                be written.

        Returns:
            String: The deployments' JSON string. The output is similar to
                json.dumps().
        """

        json_str = serve_application_to_schema(self._deployments).json(indent=4)

        if f:
            f.write(json_str)
        return json_str

    @classmethod
    def from_json(cls, str_or_file: Union[str, TextIO]) -> "Application":
        """
        Takes in a string or a file pointer to a file containing deployment
        definitions in JSON. These definitions are converted to a new
        Application object containing the deployments.

        To read from a file, use the following pattern:

        with open("file_name.txt", "w") as f:
            app = app.from_json(str_or_file)

        Args:
            str_or_file (Union[String, TextIO]): Either a string containing
                JSON deployment definitions or a pointer to a file containing
                JSON deployment definitions. The JSON format must adhere to the
                ServeApplicationSchema JSON Schema defined in
                ray.dashboard.modules.serve.schema.

        Returns:
            Application: a new Application object containing the deployments.
        """

        if isinstance(str_or_file, str):
            schema = ServeApplicationSchema.parse_raw(
                str_or_file, content_type="application/json"
            )
        else:
            schema = ServeApplicationSchema.parse_obj(json.load(str_or_file))

        return cls(schema_to_serve_application(schema))

    def to_yaml(self, f: Optional[TextIO]) -> Optional[str]:
        """
        Returns this Application's deployments as a YAML string. Optionally
        writes the YAML string to a file as well. To write to a file, use this
        pattern:

        with open("file_name.txt", "w") as f:
            app.to_yaml(f=f)

        This file is formatted as a Serve YAML config file. It can be deployed
        via the Serve CLI.

        Args:
            f (Optional[TextIO]): A pointer to the file where the YAML should
                be written.

        Returns:
            String: The deployments' YAML string. The output is from
                yaml.safe_dump().
        """

        json_str = serve_application_to_schema(self._deployments).json(indent=4)

        if f:
            yaml.safe_dump(json_str, f)
        return yaml.safe_dump(json_str)

    @classmethod
    def from_yaml(cls, str_or_file: Union[str, TextIO]) -> "Application":
        """
        Takes in a string or a file pointer to a file containing deployment
        definitions in YAML. These definitions are converted to a new
        Application object containing the deployments.

        To read from a file, use the following pattern:

        with open("file_name.txt", "w") as f:
            app = app.from_yaml(str_or_file)

        Args:
            str_or_file (Union[String, TextIO]): Either a string containing
                YAML deployment definitions or a pointer to a file containing
                YAML deployment definitions. The YAML format must adhere to the
                ServeApplicationSchema JSON Schema defined in
                ray.dashboard.modules.serve.schema. This function works with
                Serve YAML config files.

        Returns:
            Application: a new Application object containing the deployments.
        """

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
        """
        Fetch a deployment by name using dict syntax: app["name"]

        Raises:
            KeyError: if the name is not in this Application.
        """

        if key in self._deployments:
            return self._deployments[key]
        else:
            raise KeyError(f'Serve application does not contain a "{key}" deployment.')

    def __setitem__(self, key: str, value: Deployment):
        """
        Fetch a deployment by name using dict syntax: app[name] = new_deployment

        Use this to overwrite existing deployments.

        Args:
            key (String): name
            value (Deployment): the deployment that the name maps to

        Raises:
            TypeError: if the key is not a String or the value is not a deployment.
        """

        if not isinstance(key, str):
            raise TypeError(f"key should be a string, but got object of type {key}.")
        elif not isinstance(value, Deployment):
            raise TypeError(f"Got {type(Deployment)} for value. Expected deployment.")

        self._deployments[key] = value

    def __getattr__(self, name: str):
        """
        Fetch a deployment by name using attribute syntax: app.name

        Raises:
            AttributeError: if the name is not in this Application.
        """

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
