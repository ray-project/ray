import sys

import pytest

from ray.serve._private.publish_provider import (
    AnyscalePublishProvider,
    get_publish_provider,
)
from ray.serve.tests.unit.publish_provider import TestPublishProvider


def test_get_default_publish_provider():
    """Test getting a default publish provider."""
    assert get_publish_provider("anyscale") == AnyscalePublishProvider


def test_get_custom_publish_provider():
    """Test dynamically importing a publish provider."""
    publish_provider = get_publish_provider("ray.serve.tests.unit.publish_provider")
    assert isinstance(publish_provider, TestPublishProvider)
    publish_provider.publish(
        {}, name="test-name", ray_version="test-version", base_image="test-image"
    )
    assert publish_provider.published_config == {}
    assert publish_provider.published_name == "test-name"
    assert publish_provider.published_ray_version == "test-version"
    assert publish_provider.published_base_image == "test-image"


def test_get_nonexistent_custom_publish_provider():
    """Test dynamically importing a publish provider."""
    expected_msg_template = (
        "Failed to import 'get_ray_serve_publish_provider' "
        "from publish provider module '{module}'."
    )
    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="ray"),
    ):
        get_publish_provider("ray")

    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="ray.bar"),
    ):
        get_publish_provider("ray.bar")

    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="foo"),
    ):
        get_publish_provider("foo")

    with pytest.raises(
        ModuleNotFoundError,
        match=expected_msg_template.format(module="foo.bar"),
    ):
        get_publish_provider("foo.bar")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
