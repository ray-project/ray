import yaml
from abc import abstractmethod, ABC
from tempfile import NamedTemporaryFile
from typing import Dict, Optional
from subprocess import check_output

from ray._private.utils import import_attr
from ray.serve.schema import ServeDeploySchema

PUBLISH_PROVIDER_FACTORY_METHOD = "get_ray_serve_publish_provider"


class PublishProvider(ABC):
    @staticmethod
    @abstractmethod
    def publish(
        config: Dict,
        *,
        name: str,
        ray_version: str,
        base_image: Optional[str] = None,
    ):
        pass

def get_publish_provider(provider_name: str) -> PublishProvider:
    publish_provider = {
        "anyscale": AnyscalePublishProvider,
    }.get(provider_name, None)

    if publish_provider is None:
        publish_provider_factory = import_attr(
            f"{provider_name}.{PUBLISH_PROVIDER_FACTORY_METHOD}"
        )
        publish_provider = publish_provider_factory()

    return publish_provider


class AnyscalePublishProvider(PublishProvider):
    def publish(
        config: Dict,
        *,
        name: str,
        ray_version: str,
        base_image: Optional[str] = None,
    ):
        service_config = {
            "cloud": "anyscale_v2_default_cloud",
            "name": name,
            "ray_serve_config": config,
        }
        if base_image is not None:
            service_config["cluster_env"] = base_image

        # TODO(edoakes): use the Anyscale SDK (or another fixed entrypoint) instead of
        # subprocessing out to the CLI.
        with NamedTemporaryFile(mode="w") as f:
            yaml.dump(service_config, f, default_flow_style=False)
            f.flush()

            check_output(["anyscale", "service", "rollout", "-f", f.name])
