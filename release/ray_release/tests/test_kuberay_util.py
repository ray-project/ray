import sys

import pytest

from ray_release.kuberay_util import convert_cluster_compute_to_kuberay_compute_config


def test_convert_cluster_compute_to_kuberay_compute_config():
    compute_config = {
        "head_node_type": {
            "resources": {
                "limits": {
                    "cpu": "16",
                    "memory": "32Gi",
                }
            }
        },
        "worker_node_types": [
            {
                "name": "worker",
                "resources": {
                    "limits": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                    "requests": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                },
                "min_workers": 0,
                "max_workers": 2,
                "use_spot": False,
            }
        ],
    }
    kuberay_compute_config = convert_cluster_compute_to_kuberay_compute_config(
        compute_config
    )
    assert kuberay_compute_config == {
        "head_node": {
            "resources": {
                "limits": {
                    "cpu": "16",
                    "memory": "32Gi",
                }
            }
        },
        "worker_nodes": [
            {
                "group_name": "worker",
                "min_nodes": 0,
                "max_nodes": 2,
                "resources": {
                    "limits": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                    "requests": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                },
            }
        ],
    }


def test_convert_cluster_compute_to_kuberay_compute_config_new_schema():
    compute_config = {
        "head_node": {
            "resources": {
                "limits": {
                    "cpu": "16",
                    "memory": "32Gi",
                }
            }
        },
        "worker_nodes": [
            {
                "instance_type": "m5.xlarge",
                "resources": {
                    "limits": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                    "requests": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                },
                "min_nodes": 0,
                "max_nodes": 2,
            }
        ],
    }
    kuberay_compute_config = convert_cluster_compute_to_kuberay_compute_config(
        compute_config
    )
    assert kuberay_compute_config == {
        "head_node": {
            "resources": {
                "limits": {
                    "cpu": "16",
                    "memory": "32Gi",
                }
            }
        },
        "worker_nodes": [
            {
                "group_name": "worker-group-0-m5.xlarge",
                "min_nodes": 0,
                "max_nodes": 2,
                "resources": {
                    "limits": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                    "requests": {
                        "cpu": "4",
                        "memory": "8Gi",
                    },
                },
            }
        ],
    }


def test_convert_kuberay_new_schema_no_name_fallback():
    """Verify group_name fallback for new-schema workers without name."""
    compute_config = {
        "head_node": {},
        "worker_nodes": [
            {"instance_type": "m5.xlarge", "min_nodes": 1, "max_nodes": 2},
            {"instance_type": "g4dn.xlarge", "min_nodes": 0, "max_nodes": 4},
        ],
    }
    result = convert_cluster_compute_to_kuberay_compute_config(compute_config)
    assert result["worker_nodes"][0]["group_name"] == "worker-group-0-m5.xlarge"
    assert result["worker_nodes"][1]["group_name"] == "worker-group-1-g4dn.xlarge"


def test_convert_kuberay_legacy_missing_min_max_workers_produces_none():
    """Legacy config without min_workers/max_workers must yield null in JSON.

    KubeRay API treats null as \"use API defaults\" and 0 as \"explicitly zero\".
    So missing keys must become None (JSON null), not 0.
    """
    compute_config = {
        "head_node_type": {},
        "worker_node_types": [
            {
                "name": "worker",
                "instance_type": "m5.xlarge",
                "resources": {"limits": {"cpu": "4"}},
                # no min_workers / max_workers
            }
        ],
    }
    result = convert_cluster_compute_to_kuberay_compute_config(compute_config)
    assert result["worker_nodes"][0]["min_nodes"] is None
    assert result["worker_nodes"][0]["max_nodes"] is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
