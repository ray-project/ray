from typing import Optional

from ray.serve._private.deploy_provider import DeployProvider
from ray.serve.schema import ServeDeploySchema


class FakeDeployProvider(DeployProvider):
    """DeployProvider implementation for unit testing."""

    def __init__(self):
        self.reset()
        self._supports_local_uris = True

    def set_supports_local_uris(self, supported: bool):
        self._supports_local_uris = supported

    def supports_local_uris(self):
        return self._supports_local_uris

    def reset(self):
        self.deployed_config = None
        self.deployed_address = None
        self.deployed_name = None
        self.deployed_base_image = None

    def deploy(
        self,
        config: ServeDeploySchema,
        *,
        address: str,
        name: Optional[str],
        base_image: Optional[str] = None,
    ):
        self.deployed_config = config
        self.deployed_address = address
        self.deployed_name = name
        self.deployed_base_image = base_image


DEPLOY_PROVIDER_SINGLETON = None


def get_ray_serve_deploy_provider():
    global DEPLOY_PROVIDER_SINGLETON
    if DEPLOY_PROVIDER_SINGLETON is None:
        DEPLOY_PROVIDER_SINGLETON = FakeDeployProvider()

    return DEPLOY_PROVIDER_SINGLETON
