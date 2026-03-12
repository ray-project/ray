import sys

import pytest

from ray_release.compute_config_utils import (
    get_head_node_config,
    get_worker_max_count,
    get_worker_min_count,
    get_worker_node_configs,
    is_new_schema,
)
from ray_release.exception import MixedSchemaError


class TestIsNewSchema:
    def test_new_schema_head_node(self):
        assert is_new_schema({"head_node": {}}) is True

    def test_new_schema_worker_nodes(self):
        assert is_new_schema({"worker_nodes": []}) is True

    def test_new_schema_both_keys(self):
        assert is_new_schema({"head_node": {}, "worker_nodes": []}) is True

    def test_legacy_schema_head_node_type(self):
        assert is_new_schema({"head_node_type": {}}) is False

    def test_legacy_schema_worker_node_types(self):
        assert is_new_schema({"worker_node_types": []}) is False

    def test_legacy_schema_both_keys(self):
        assert is_new_schema({"head_node_type": {}, "worker_node_types": []}) is False

    def test_neither_keys_is_new_schema(self):
        assert is_new_schema({}) is True
        assert is_new_schema({"cloud": "my-cloud"}) is True

    def test_mixed_schema_raises(self):
        with pytest.raises(MixedSchemaError):
            is_new_schema({"head_node": {}, "worker_node_types": []})

    def test_mixed_schema_raises_reverse(self):
        with pytest.raises(MixedSchemaError):
            is_new_schema({"head_node_type": {}, "worker_nodes": []})


class TestGetHeadNodeConfig:
    def test_new_schema(self):
        config = {"head_node": {"instance_type": "m5.xlarge"}}
        assert get_head_node_config(config) == {"instance_type": "m5.xlarge"}

    def test_legacy_schema(self):
        config = {"head_node_type": {"instance_type": "m5.xlarge"}}
        assert get_head_node_config(config) == {"instance_type": "m5.xlarge"}

    def test_new_schema_missing(self):
        config = {"worker_nodes": []}
        assert get_head_node_config(config) == {}

    def test_legacy_schema_missing(self):
        config = {"worker_node_types": []}
        assert get_head_node_config(config) == {}

    def test_new_schema_none_value(self):
        config = {"head_node": None}
        assert get_head_node_config(config) == {}


class TestGetWorkerNodeConfigs:
    def test_new_schema(self):
        workers = [{"instance_type": "m5.xlarge", "min_nodes": 1, "max_nodes": 4}]
        config = {"worker_nodes": workers}
        assert get_worker_node_configs(config) == workers

    def test_legacy_schema(self):
        workers = [{"instance_type": "m5.xlarge", "min_workers": 1, "max_workers": 4}]
        config = {"worker_node_types": workers}
        assert get_worker_node_configs(config) == workers

    def test_new_schema_missing(self):
        config = {"head_node": {}}
        assert get_worker_node_configs(config) == []

    def test_legacy_schema_missing(self):
        config = {"head_node_type": {}}
        assert get_worker_node_configs(config) == []

    def test_new_schema_none_value(self):
        config = {"worker_nodes": None}
        assert get_worker_node_configs(config) == []

    def test_new_schema_empty_list(self):
        config = {"worker_nodes": []}
        assert get_worker_node_configs(config) == []


class TestGetWorkerMinCount:
    def test_new_schema(self):
        assert get_worker_min_count({"min_nodes": 3}, new_schema=True) == 3

    def test_legacy_schema(self):
        assert get_worker_min_count({"min_workers": 3}, new_schema=False) == 3

    def test_new_schema_default(self):
        assert get_worker_min_count({}, new_schema=True) == 0

    def test_legacy_schema_default(self):
        assert get_worker_min_count({}, new_schema=False) == 0

    def test_custom_default(self):
        assert get_worker_min_count({}, new_schema=True, default=5) == 5


class TestGetWorkerMaxCount:
    def test_new_schema(self):
        assert get_worker_max_count({"max_nodes": 10}, new_schema=True) == 10

    def test_legacy_schema(self):
        assert get_worker_max_count({"max_workers": 10}, new_schema=False) == 10

    def test_new_schema_default(self):
        assert get_worker_max_count({}, new_schema=True) == 0

    def test_legacy_schema_default(self):
        assert get_worker_max_count({}, new_schema=False) == 0

    def test_custom_default(self):
        assert get_worker_max_count({}, new_schema=True, default=5) == 5


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
