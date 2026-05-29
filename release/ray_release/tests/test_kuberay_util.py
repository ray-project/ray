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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
