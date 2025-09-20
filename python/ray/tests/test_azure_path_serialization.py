"""Tests for Azure autoscaler Path object serialization.

This test verifies that the Azure autoscaler properly handles Path objects
by converting them to strings before storing them in the configuration dictionary,
which ensures that the configuration can be properly serialized to JSON.

The issue was introduced in PR #54596 which added automatic SSH key generation
for Azure clusters but stored Path objects directly in the configuration,
which later caused serialization errors.
"""
import json
import sys

import pytest

from ray.autoscaler._private._azure.config import _configure_key_pair


def test_azure_key_pair_string_conversion(tmp_path):
    """Test that Azure key pair configuration converts Path objects to strings."""

    # Create the key files under pytest's temporary path
    private_key_path = tmp_path / "id_rsa"
    public_key_path = tmp_path / "id_rsa.pub"

    # Create and write the key files
    private_key_path.write_text("")
    public_key_path.write_text("ssh-rsa TEST_KEY user@example.com")

    # Create a test config with Path objects
    config = {
        "auth": {
            "ssh_user": "ubuntu",
            "ssh_private_key": private_key_path,
            "ssh_public_key": public_key_path,
        },
        "provider": {"location": "westus2", "resource_group": "test-group"},
        "available_node_types": {"ray.head.default": {"node_config": {}}},
    }

    # Process the config
    result_config = _configure_key_pair(config)

    # Verify the paths were converted to strings
    assert isinstance(result_config["auth"]["ssh_private_key"], str)
    assert isinstance(result_config["auth"]["ssh_public_key"], str)
    assert isinstance(result_config["file_mounts"]["~/.ssh/id_rsa.pub"], str)

    # Verify we can serialize the config to JSON without errors
    json_str = json.dumps(result_config)
    # If we get here, serialization succeeded
    # Now try to deserialize to make sure it's valid JSON
    deserialized = json.loads(json_str)
    assert isinstance(deserialized, dict)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
