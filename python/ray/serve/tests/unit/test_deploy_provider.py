import os
import sys

import pytest

from ray.serve._private.deploy_provider import (
    DEPLOY_PROVIDER_ENV_VAR,
    AnyscaleDeployProvider,
    LocalDeployProvider,
    get_deploy_provider,
)
from ray.serve.tests.unit.fake_deploy_provider import FakeDeployProvider


def test_get_builtin_deploy_providers():
    """Test getting builtin deploy provider."""
    assert isinstance(get_deploy_provider(None), LocalDeployProvider)
    assert isinstance(get_deploy_provider("local"), LocalDeployProvider)
    assert isinstance(get_deploy_provider("anyscale"), AnyscaleDeployProvider)


@pytest.mark.parametrize("from_env_var", [False, True])
def test_get_custom_deploy_provider(from_env_var: bool):
    """Test dynamically importing a deploy provider."""
    import_path = "ray.serve.tests.unit.fake_deploy_provider"
    if from_env_var:
        try:
            os.environ[DEPLOY_PROVIDER_ENV_VAR] = import_path
            deploy_provider = get_deploy_provider(None)
        finally:
            os.environ.pop(DEPLOY_PROVIDER_ENV_VAR)
    else:
        deploy_provider = get_deploy_provider(import_path)

    assert isinstance(deploy_provider, FakeDeployProvider)
    deploy_provider.deploy(
        {},
        address="http://localhost:8265",
        name="test-name",
        base_image="test-image",
    )
    assert deploy_provider.deployed_config == {}
    assert deploy_provider.deployed_address == "http://localhost:8265"
    assert deploy_provider.deployed_name == "test-name"
    assert deploy_provider.deployed_base_image == "test-image"


def test_get_nonexistent_custom_deploy_provider():
    """Test dynamically importing a deploy provider."""
    expected_msg_template = (
        "Failed to import 'get_ray_serve_deploy_provider' "
        "from deploy provider module '{module}'."
    )
    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="ray"),
    ):
        get_deploy_provider("ray")

    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="ray.bar"),
    ):
        get_deploy_provider("ray.bar")

    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="foo"),
    ):
        get_deploy_provider("foo")

    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="foo.bar"),
    ):
        get_deploy_provider("foo.bar")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
