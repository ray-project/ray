from typing import Optional

from ray.serve._private.deploy_provider import DeployOptions, DeployProvider
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
        self.deployed_config: Optional[ServeDeploySchema] = None
        self.deployed_options: Optional[DeployOptions] = None

    def deploy(self, config: ServeDeploySchema, *, options: DeployOptions):
        self.deployed_config = config
        self.deployed_options = options


DEPLOY_PROVIDER_SINGLETON = None


def get_ray_serve_deploy_provider():
    global DEPLOY_PROVIDER_SINGLETON
    if DEPLOY_PROVIDER_SINGLETON is None:
        DEPLOY_PROVIDER_SINGLETON = FakeDeployProvider()

    return DEPLOY_PROVIDER_SINGLETON
