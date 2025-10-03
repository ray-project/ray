"""Tests for Azure autoscaler Path object serialization and SSH key handling.

This test verifies that the Azure autoscaler properly:
1. Converts Path objects to strings before storing them in configuration
2. Always removes ssh_public_key from auth config (both user-specified and auto-generated)
   to prevent bootstrap config from containing paths that don't exist on head node
3. Always injects public key content into ARM template parameters for VM creation
4. Ensures configuration can be properly serialized to JSON

The ssh_public_key path is removed because bootstrap config gets copied to worker nodes
and must only contain paths that exist on the head node. The public key content is
still used via ARM template parameter injection during VM creation.

The original issue was introduced in PR #54596 which added automatic SSH key
generation but stored Path objects directly in the configuration, causing
serialization errors.
"""
import json
import sys

import pytest

from ray.autoscaler._private._azure.config import _configure_key_pair


@pytest.mark.parametrize(
    "test_case,auth_config,expected_public_key_content",
    [
        (
            "user_specified_keys",
            {
                "ssh_user": "ubuntu",
                "ssh_private_key": "private_key_path",  # Will be replaced with actual path
                "ssh_public_key": "public_key_path",  # Will be replaced with actual path
            },
            "ssh-rsa TEST_KEY user@example.com",
        ),
        (
            "auto_generated_keys",
            {"ssh_user": "ubuntu"},
            None,  # Will be auto-generated, so we just check it exists
        ),
    ],
)
def test_azure_key_pair_string_conversion(
    tmp_path, test_case, auth_config, expected_public_key_content
):
    """Test that Azure key pair configuration converts Path objects to strings.

    Tests both user-specified and auto-generated SSH key scenarios.
    """

    # Create the key files under pytest's temporary path (needed for user-specified case)
    private_key_path = tmp_path / "id_rsa"
    public_key_path = tmp_path / "id_rsa.pub"

    private_key_path.write_text("")
    public_key_path.write_text("ssh-rsa TEST_KEY user@example.com")

    # Replace placeholder paths with actual paths for user-specified keys
    if (
        "ssh_private_key" in auth_config
        and auth_config["ssh_private_key"] == "private_key_path"
    ):
        auth_config["ssh_private_key"] = private_key_path
    if (
        "ssh_public_key" in auth_config
        and auth_config["ssh_public_key"] == "public_key_path"
    ):
        auth_config["ssh_public_key"] = public_key_path

    # Create test configuration
    config = {
        "auth": auth_config,
        "provider": {"location": "westus2", "resource_group": "test-group"},
        "available_node_types": {"ray.head.default": {"node_config": {}}},
    }

    # Process the config
    result_config = _configure_key_pair(config)

    # Verify private key path exists and was converted to string
    assert "ssh_private_key" in result_config["auth"]
    assert isinstance(result_config["auth"]["ssh_private_key"], str)

    # Verify ssh_public_key is always removed (both user-specified and auto-generated)
    # because bootstrap config must only contain paths that exist on head node
    assert "ssh_public_key" not in result_config["auth"]

    # Verify public key content was injected into ARM parameters
    head_node_config = result_config["available_node_types"]["ray.head.default"][
        "node_config"
    ]
    assert "azure_arm_parameters" in head_node_config
    assert "publicKey" in head_node_config["azure_arm_parameters"]

    actual_public_key = head_node_config["azure_arm_parameters"]["publicKey"]
    if expected_public_key_content is not None:
        # User-specified case: verify exact content
        assert actual_public_key == expected_public_key_content
    else:
        # Auto-generated case: just verify it exists and looks like an SSH key
        assert actual_public_key.strip()
        assert actual_public_key.startswith("ssh-rsa")

    # Verify config can be serialized to JSON without errors
    json_str = json.dumps(result_config)
    # If we get here, serialization succeeded
    # Now try to deserialize to make sure it's valid JSON
    deserialized = json.loads(json_str)
    assert isinstance(deserialized, dict)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
