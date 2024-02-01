import sys

import pytest

from ray.serve._private.deploy_provider import (
    AnyscaleDeployProvider,
    get_deploy_provider,
)
from ray.serve.tests.unit.fake_deploy_provider import FakeDeployProvider


def test_get_default_deploy_provider():
    """Test getting a default deploy provider."""
    assert get_deploy_provider("anyscale") == AnyscaleDeployProvider


def test_get_custom_deploy_provider():
    """Test dynamically importing a deploy provider."""
    deploy_provider = get_deploy_provider("ray.serve.tests.unit.fake_deploy_provider")
    assert isinstance(deploy_provider, FakeDeployProvider)
    deploy_provider.deploy(
        {},
        address="http://localhost:8265",
        name="test-name",
        ray_version="test-version",
        base_image="test-image",
    )
    assert deploy_provider.deployed_config == {}
    assert deploy_provider.deployed_address == "http://localhost:8265"
    assert deploy_provider.deployed_name == "test-name"
    assert deploy_provider.deployed_ray_version == "test-version"
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
