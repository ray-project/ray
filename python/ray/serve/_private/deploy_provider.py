import os
from abc import ABC, abstractmethod
from subprocess import CalledProcessError, check_output
from tempfile import NamedTemporaryFile
from typing import Dict, Optional

import click
import yaml

from ray._private.utils import import_attr
from ray.autoscaler._private.cli_logger import cli_logger
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve.schema import ServeDeploySchema

# Method that dynamically imported modules must implement to return a `DeployProvider`.
DEPLOY_PROVIDER_FACTORY_METHOD = "get_ray_serve_deploy_provider"
DEPLOY_PROVIDER_ENV_VAR = "RAY_SERVE_DEPLOY_PROVIDER"


class DeployProvider(ABC):
    @staticmethod
    @abstractmethod
    def deploy(
        config: Dict,
        *,
        address: str,
        name: Optional[str],
        ray_version: str,
        base_image: Optional[str] = None,
    ):
        pass


def get_deploy_provider(provider_name: Optional[str]) -> DeployProvider:
    if provider_name is None:
        provider_name = os.environ.get(DEPLOY_PROVIDER_ENV_VAR, "local")

    deploy_provider = {
        "anyscale": AnyscaleDeployProvider,
        "local": LocalDeployProvider,
    }.get(provider_name, None)

    if deploy_provider is None:
        try:
            deploy_provider_factory = import_attr(
                f"{provider_name}.{DEPLOY_PROVIDER_FACTORY_METHOD}"
            )
        except (ModuleNotFoundError, AttributeError):
            raise ModuleNotFoundError(
                f"Failed to import '{DEPLOY_PROVIDER_FACTORY_METHOD}' "
                f"from deploy provider module '{provider_name}'."
            )
        deploy_provider = deploy_provider_factory()

    return deploy_provider


class LocalDeployProvider(DeployProvider):
    """Provider that deploys by sending a REST API request to a cluster."""

    def deploy(
        config: Dict,
        *,
        address: str,
        name: Optional[str],
        ray_version: str,
        base_image: Optional[str] = None,
    ):
        ServeDeploySchema.parse_obj(config)
        if base_image is not None:
            raise ValueError(
                "`--base-image` is not supported when using the 'local' deploy "
                "provider because it deploys to an existing Ray cluster."
            )

        ServeSubmissionClient(address).deploy_applications(config)
        cli_logger.success(
            "\nSent deploy request successfully.\n "
            "* Use `serve status` to check applications' statuses.\n "
            "* Use `serve config` to see the current application config(s).\n"
        )


class AnyscaleDeployProvider(DeployProvider):
    """Provider that deploys to the anyscale platform."""

    def deploy(
        config: Dict,
        *,
        address: str,
        name: Optional[str],
        ray_version: str,
        base_image: Optional[str] = None,
    ):
        service_config = {
            "ray_serve_config": config,
        }
        if name is not None:
            service_config["name"] = name
        if base_image is not None:
            service_config["cluster_env"] = base_image

        # TODO(edoakes): use the Anyscale SDK (or another fixed entrypoint) instead of
        # subprocessing out to the CLI.
        with NamedTemporaryFile(mode="w") as f:
            yaml.dump(service_config, f, default_flow_style=False)
            f.flush()

            try:
                check_output(["anyscale", "service", "rollout", "-f", f.name])
            except CalledProcessError:
                raise click.ClickException("Failed to deploy service.")
