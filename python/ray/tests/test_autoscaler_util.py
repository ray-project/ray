import os
import sys
import pytest


from ray.autoscaler._private.util import with_envs, with_head_node_ip


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
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
