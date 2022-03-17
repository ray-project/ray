from typing import List, TextIO, Optional, Union, Dict
import yaml
import time
import sys

from ray import serve
from ray.serve.api import (
    Deployment,
    internal_get_global_client,
)
from ray.serve.schema import (
    ServeApplicationSchema,
    serve_application_to_schema,
    schema_to_serve_application,
    serve_application_status_to_schema,
)
from ray.serve.utils import logger
from ray.serve.api import get_deployment_statuses
from ray.autoscaler._private.cli_logger import _CliLogger
from logging import Logger


class Application:
    """Used to deploy, update, and monitor groups of Serve deployments.

    Args:
        deployments (List[Deployment]): The Serve Application is
            initialized to contain this group of deployments.
    """

    def __init__(self, deployments: List[Deployment] = None):
        deployments = deployments or []

        self._deployments: Dict[str, Deployment] = dict()
        for d in deployments:
            self.add_deployment(d)

    def add_deployment(self, deployment: Deployment):
        """Adds the deployment to this Serve application.

        Validates that a deployment with the same name doesn't already exist.
        To overwrite an existing deployment, use index notation. For example:

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
        """Atomically deploys the Application's deployments to the Ray cluster.

        The Application's deployments can carry handles to one another.

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

        return internal_get_global_client().deploy_group(
            parameter_group, _blocking=blocking
        )

    def run(self, logger: Union[Logger, _CliLogger] = logger):
        """Deploys all deployments in this Application and logs status.

        This function keeps looping and printing status, so it must be manually
        killed (e.g. using ctrl-C). When it recieves a kill signal, it tears
        down all deployments that it deployed. If there are no deployments
        left, it also shuts down the rest of Serve, including the controller.
        This is meant to help interactive development.

        Args:
            logger: Any Python object that implements the standard logger
                interface.
        """

        try:
            serve.start(detached=True)
            self.deploy()

            logger.info("\nDeployed successfully!\n")

            while True:
                statuses = serve_application_status_to_schema(
                    get_deployment_statuses()
                ).json(indent=4)
                logger.info(f"{statuses}")
                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("Got SIGINT (KeyboardInterrupt). Removing deployments.")
            deployment_names = [d.name for d in self._deployments.values()]
            internal_get_global_client().delete_deployments(
                deployment_names, blocking=True
            )
            if len(serve.list_deployments()) == 0:
                logger.info("No deployments left. Shutting down Serve.")
                serve.shutdown()
            sys.exit()

    def to_dict(self) -> Dict:
        """Returns this Application's deployments as a dictionary.

        This dictionary adheres to the Serve REST API schema. It can be deployed
        via the Serve REST API.

        If any deployment's func_or_class is a function or class (and not an
        import path), it overwrites it with that function or class's import path.

        Returns:
            Dict: The Application's deployments formatted in a dictionary.
        """

        return serve_application_to_schema(self._deployments.values()).dict()

    @classmethod
    def from_dict(cls, d: Dict) -> "Application":
        """Converts a dictionary of deployment data to an Application.

        Takes in a dictionary matching the Serve REST API schema and converts
        it to an Application containing those deployments.

        Args:
            d (Dict): A dictionary containing the deployments' data that matches
                the Serve REST API schema.

        Returns:
            Application: a new Application object containing the deployments.
        """

        schema = ServeApplicationSchema.parse_obj(d)
        return cls(schema_to_serve_application(schema))

    def to_yaml(self, f: Optional[TextIO] = None) -> Optional[str]:
        """Returns this Application's deployments as a YAML string.

        Optionally writes the YAML string to a file as well. To write to a
        file, use this pattern:

        with open("file_name.txt", "w") as f:
            app.to_yaml(f=f)

        This file is formatted as a Serve YAML config file. It can be deployed
        via the Serve CLI.

        If any deployment's func_or_class is a function or class (and not an
        import path), it overwrites it with that function or class's import path.

        Args:
            f (Optional[TextIO]): A pointer to the file where the YAML should
                be written.

        Returns:
            Optional[String]: The deployments' YAML string. The output is from
                yaml.safe_dump(). Returned only if no file pointer is passed in.
        """

        deployment_dict = serve_application_to_schema(self._deployments.values()).dict()

        if f:
            yaml.safe_dump(deployment_dict, f, default_flow_style=False)
        return yaml.safe_dump(deployment_dict, default_flow_style=False)

    @classmethod
    def from_yaml(cls, str_or_file: Union[str, TextIO]) -> "Application":
        """Converts YAML data to deployments for an Application.

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
                ray.serve.schema. This function works with
                Serve YAML config files.

        Returns:
            Application: a new Application object containing the deployments.
        """

        deployments_json = yaml.safe_load(str_or_file)
        schema = ServeApplicationSchema.parse_obj(deployments_json)
        return cls(schema_to_serve_application(schema))

    def __getitem__(self, key: str):
        """Fetch a deployment by name using dict syntax: app["name"]

        Raises:
            KeyError: if the name is not in this Application.
        """

        if key in self._deployments:
            return self._deployments[key]
        else:
            raise KeyError(f'Serve application does not contain a "{key}" deployment.')

    def __setitem__(self, key: str, value: Deployment):
        """Set a deployment by name with dict syntax: app[name]=new_deployment

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

    def __iter__(self):
        """Iterator over Application's deployments.

        Enables "for deployment in Application" pattern.
        """

        return iter(self._deployments.values())

    def __len__(self):
        """Number of deployments in this Application."""

        return len(self._deployments)

    def __contains__(self, key: str):
        """Checks if the key exists in self._deployments."""

        return key in self._deployments
