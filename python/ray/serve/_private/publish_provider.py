from abc import ABC, abstractmethod
from subprocess import CalledProcessError, check_output
from tempfile import NamedTemporaryFile
from typing import Dict, Optional

import click
import yaml

from ray._private.utils import import_attr

# Method that dynamically imported modules must implement to return a `PublishProvider`.
PUBLISH_PROVIDER_FACTORY_METHOD = "get_ray_serve_publish_provider"


class PublishProvider(ABC):
    @staticmethod
    @abstractmethod
    def publish(
        config: Dict,
        *,
        name: Optional[str],
        ray_version: str,
        base_image: Optional[str] = None,
    ):
        pass


def get_publish_provider(provider_name: str) -> PublishProvider:
    publish_provider = {
        "anyscale": AnyscalePublishProvider,
    }.get(provider_name, None)

    if publish_provider is None:
        try:
            publish_provider_factory = import_attr(
                f"{provider_name}.{PUBLISH_PROVIDER_FACTORY_METHOD}"
            )
        except (ModuleNotFoundError, AttributeError):
            raise ModuleNotFoundError(
                f"Failed to import '{PUBLISH_PROVIDER_FACTORY_METHOD}' "
                f"from publish provider module '{provider_name}'."
            )
        publish_provider = publish_provider_factory()

    return publish_provider


class AnyscalePublishProvider(PublishProvider):
    def publish(
        config: Dict,
        *,
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
