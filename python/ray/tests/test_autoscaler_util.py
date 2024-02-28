import os
import sys
import pytest


from ray.autoscaler._private.util import (
    with_envs,
    with_head_node_ip,
    _rewrite_and_warn,
    rewrite_deprecated_workers_fields,
)


# #CVGA-start
class TestRewriteAndWarn:
    def test_deprecated_key_replacement(self, capsys):
        config = {"old_key": "value1"}
        _rewrite_and_warn("old_key", "new_key", config)
        captured = capsys.readouterr()
        assert "`old_key` is deprecated" in captured.out
        assert config["new_key"] == "value1"
        assert "old_key" not in config

    def test_both_keys_present(self, capsys):
        config = {"old_key": "value1", "new_key": "value2"}
        _rewrite_and_warn("old_key", "new_key", config)
        captured = capsys.readouterr()
        assert "`old_key` is deprecated" in captured.out
        assert "Both `old_key` and `new_key` are provided" in captured.out
        assert config["new_key"] == "value2"
        assert "old_key" not in config

    def test_new_key_only(self, capsys):
        config = {"new_key": "value2"}
        _rewrite_and_warn("old_key", "new_key", config)
        assert config["new_key"] == "value2"


# #CVGA-end


class TestRewriteDeprecatedWorkersFields:
    def test_deprecated_fields_only(self):
        config = {
            "max_workers": 100,
            "available_node_types": {
                "head_node_type": {
                    "max_workers": 2,
                    "min_workers": 1,
                },
                "worker_node_type": {
                    "max_workers": 10,
                    "min_workers": 1,
                },
            },
        }
        updated_config = rewrite_deprecated_workers_fields(config)
        assert updated_config == {
            "max_worker_nodes": 100,
            "available_node_types": {
                "head_node_type": {
                    "max_worker_nodes": 2,
                    "min_worker_nodes": 1,
                },
                "worker_node_type": {
                    "max_worker_nodes": 10,
                    "min_worker_nodes": 1,
                },
            },
        }

    def test_both_deprecated_and_new_fields(self):
        config = {
            "max_workers": 100,
            "max_worker_nodes": 1000,
            "available_node_types": {
                "head_node_type": {
                    "max_workers": 2,
                    "max_worker_nodes": 20,
                    "min_workers": 1,
                    "min_worker_nodes": 10,
                },
                "worker_node_type": {
                    "max_workers": 10,
                    "max_worker_nodes": 100,
                    "min_workers": 1,
                    "min_worker_nodes": 10,
                },
            },
        }
        updated_config = rewrite_deprecated_workers_fields(config)
        assert updated_config == {
            "max_worker_nodes": 1000,
            "available_node_types": {
                "head_node_type": {
                    "max_worker_nodes": 20,
                    "min_worker_nodes": 10,
                },
                "worker_node_type": {
                    "max_worker_nodes": 100,
                    "min_worker_nodes": 10,
                },
            },
        }

    def test_new_fields_only(self):
        config = {
            "max_worker_nodes": 1000,
            "available_node_types": {
                "head_node_type": {
                    "max_worker_nodes": 20,
                    "min_worker_nodes": 10,
                },
                "worker_node_type": {
                    "max_worker_nodes": 100,
                    "min_worker_nodes": 10,
                },
            },
        }
        updated_config = rewrite_deprecated_workers_fields(config)
        assert updated_config == config


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
