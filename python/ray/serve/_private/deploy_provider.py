import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from subprocess import CalledProcessError, check_output
from tempfile import NamedTemporaryFile
from typing import Callable, Optional

import click
import yaml

from ray._private.utils import import_attr
from ray.autoscaler._private.cli_logger import cli_logger
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve.schema import ServeDeploySchema

# Method that dynamically imported modules must implement to return a `DeployProvider`.
DEPLOY_PROVIDER_FACTORY_METHOD = "get_ray_serve_deploy_provider"
DEPLOY_PROVIDER_ENV_VAR = "RAY_SERVE_DEPLOY_PROVIDER"


@dataclass(frozen=True)
class DeployOptions:
    address: str
    name: Optional[str] = None
    base_image: Optional[str] = None
    in_place: bool = False


class DeployProvider(ABC):
    @abstractmethod
    def supports_local_uris(self) -> bool:
        """Whether or not this deploy provider supports local URIs in the runtime_env.

        If they do, the relevant validation will be skipped when building the config.
        """
        pass

    @abstractmethod
    def deploy(
        self,
        config: ServeDeploySchema,
        *,
        options: DeployOptions,
    ):
        """The primary method providers must implement to deploy a Serve config."""
        pass


def _get_builtin_deploy_provider_factory(provider_name: str) -> Callable:
    return {
        "anyscale": AnyscaleDeployProvider,
        "local": LocalDeployProvider,
    }.get(provider_name, None)


def get_deploy_provider(provider_name: Optional[str]) -> DeployProvider:
    """Returns the specified deploy provider or a default.

    Order of precedence is: provided name > env var > default (local provider).
    """
    if provider_name is None:
        provider_name = os.environ.get(DEPLOY_PROVIDER_ENV_VAR, "local")

    deploy_provider_factory = _get_builtin_deploy_provider_factory(provider_name)
    if deploy_provider_factory is None:
        try:
            deploy_provider_factory = import_attr(
                f"{provider_name}.{DEPLOY_PROVIDER_FACTORY_METHOD}"
            )
        except (ModuleNotFoundError, AttributeError):
            raise ModuleNotFoundError(
                f"Failed to import '{DEPLOY_PROVIDER_FACTORY_METHOD}' "
                f"from deploy provider module '{provider_name}'."
            )

    return deploy_provider_factory()


class LocalDeployProvider(DeployProvider):
    """Provider that deploys by sending a REST API request to a cluster."""

    def supports_local_uris(self) -> bool:
        return False

    def deploy(
        self,
        config: ServeDeploySchema,
        *,
        options: DeployOptions,
    ):
        if options.base_image is not None:
            raise ValueError(
                "`--base-image` is not supported when using the 'local' deploy "
                "provider because it deploys to an existing Ray cluster."
            )

        ServeSubmissionClient(options.address).deploy_applications(
            config.dict(exclude_unset=True),
        )
        cli_logger.success(
            "\nSent deploy request successfully.\n "
            "* Use `serve status` to check applications' statuses.\n "
            "* Use `serve config` to see the current application config(s).\n"
        )


class AnyscaleDeployProvider(DeployProvider):
    """Provider that deploys to the anyscale platform."""

    def supports_local_uris(self) -> bool:
        return True

    def deploy(
        self,
        config: ServeDeploySchema,
        *,
        options: DeployOptions,
    ):
        service_config = {
            "ray_serve_config": config.dict(exclude_unset=True),
        }
        if options.name is not None:
            service_config["name"] = options.name
        if options.base_image is not None:
            service_config["cluster_env"] = options.base_image
        if options.in_place:
            service_config["rollout_strategy"] = "IN_PLACE"

        # TODO(edoakes): use the Anyscale SDK (or another fixed entrypoint) instead of
        # subprocessing out to the CLI.
        with NamedTemporaryFile(mode="w") as f:
            yaml.dump(service_config, f, default_flow_style=False)
            f.flush()

            try:
                check_output(["anyscale", "service", "rollout", "-f", f.name])
            except CalledProcessError:
                raise click.ClickException("Failed to deploy service.")
