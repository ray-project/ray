import sys

import pytest

from ray.autoscaler._private.util import prepare_config, with_envs, with_head_node_ip


@pytest.mark.parametrize(
    "mount_config",
    [{"auth_mode": "disabled", "file_mounts": {"/home/ray/path": "."}}, {}],
)
def test_prepare_config_rejects_colon_in_docker_cluster_name(mount_config):
    config = {
        "provider": {"type": "aws"},
        "cluster_name": "default:with_colon",
        "docker": {"container_name": "ray", "image": "rayproject/ray"},
        **mount_config,
    }

    with pytest.raises(ValueError, match="`cluster_name` cannot contain ':'"):
        prepare_config(config)


def test_prepare_config_accepts_colon_without_docker_bind_mounts():
    config = {
        "provider": {"type": "aws"},
        "cluster_name": "default:with_colon",
        "auth_mode": "disabled",
        "docker": {"container_name": "ray", "image": "rayproject/ray"},
    }

    assert prepare_config(config)["cluster_name"] == "default:with_colon"

    config["docker"] = {}
    config["file_mounts"] = {"/home/ray/path": "."}
    assert prepare_config(config)["cluster_name"] == "default:with_colon"


def test_with_envs():
    assert with_envs(
        ["echo $RAY_HEAD_IP", "ray start"], {"RAY_HEAD_IP": "127.0.0.0"}
    ) == [
        "export RAY_HEAD_IP=127.0.0.0; echo $RAY_HEAD_IP",
        "export RAY_HEAD_IP=127.0.0.0; ray start",
    ]

    assert with_head_node_ip(["echo $RAY_HEAD_IP"], "127.0.0.0") == [
        "export RAY_HEAD_IP=127.0.0.0; echo $RAY_HEAD_IP"
    ]

    assert (
        "export RAY_HEAD_IP=456"
        in with_envs(
            ["echo $RAY_CLOUD_ID"], {"RAY_CLOUD_ID": "123", "RAY_HEAD_IP": "456"}
        )[0]
    )
    assert (
        "export RAY_CLOUD_ID=123"
        in with_envs(
            ["echo $RAY_CLOUD_ID"], {"RAY_CLOUD_ID": "123", "RAY_HEAD_IP": "456"}
        )[0]
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
