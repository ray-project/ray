import pytest

from ray.autoscaler._private.util import prepare_config
from ray.test_utils import load_test_config


def test_max_worker_default():

    # Load config, call prepare config, check that default max_workers
    # is filled correctly for node types that don't specify it.
    # Check that max_workers is untouched for node types
    # that do specify it.
    config = load_test_config("test_multi_node.yaml")
    node_types = config["available_node_types"]

    # Max workers initially absent for this node type.
    assert "max_workers" not in\
        node_types["worker_node_max_unspecified"]
    # Max workers specified for this node type.
    assert "max_workers" in\
        node_types["worker_node_max_specified"]

    prepared_config = prepare_config(config)
    prepared_node_types = prepared_config["available_node_types"]
    # Max workers unchanged.
    assert node_types["worker_node_max_specified"]["max_workers"] ==\
        prepared_node_types["worker_node_max_specified"]["max_workers"] == 3
    # Max workers correctly auto-filled with specified cluster-wide value of 5.
    assert prepared_node_types["worker_node_max_unspecified"]["max_workers"]\
        == config["max_workers"] == 5

    # Repeat with a config that doesn't specify global max workers --
    # default value of 2 should be pulled in for global max workers.
    config = load_test_config("test_multi_node.yaml")
    # Delete global max_workers so it can be autofilled with default of 2.
    del config["max_workers"]
    node_types = config["available_node_types"]

    # Max workers initially absent for this node type.
    assert "max_workers" not in\
        node_types["worker_node_max_unspecified"]
    # Max workers specified for this node type.
    assert "max_workers" in\
        node_types["worker_node_max_specified"]

    prepared_config = prepare_config(config)
    prepared_node_types = prepared_config["available_node_types"]
    # Max workers unchanged.
    assert node_types["worker_node_max_specified"]["max_workers"] ==\
        prepared_node_types["worker_node_max_specified"]["max_workers"] == 3
    # Max workers correctly auto-filled with default cluster-wide value of 2.
    assert prepared_node_types["worker_node_max_unspecified"]["max_workers"]\
        == 2


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
