import sys

import pytest

from ray.autoscaler._private.util import (
    format_resource,
    parse_usage,
    with_envs,
    with_head_node_ip,
)


def test_format_resource():
    assert format_resource(1.0) == "1"
    assert format_resource(0.010000000000000009) == "0.01"
    assert format_resource(0.0001) == "0.0001"
    assert format_resource(0.00999999999) == "0.01"
    assert format_resource(123.456) == "123.456"
    assert format_resource(123.0) == "123"
    assert format_resource(0) == "0"
    assert format_resource(123456789) == "123456789"
    assert format_resource(123456789.0) == "123456789"
    assert format_resource(1000000.0) == "1000000"
    assert format_resource(1000000) == "1000000"


def test_parse_usage_formatting():
    usage = {
        "CPU": (0.010000000000000009, 1.0),
        "special_hardware": (0.010000000000000009, 1.0),
        "memory": (1024 * 1024, 1024 * 1024 * 2),
    }

    lines = parse_usage(usage, verbose=False)
    # Expected lines (sorted by resource name):
    # 0.01/1 CPU
    # 1.00MiB/2.00MiB memory
    # 0.01/1 special_hardware

    assert "0.01/1 CPU" in lines
    assert "1.00MiB/2.00MiB memory" in lines
    assert "0.01/1 special_hardware" in lines


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
