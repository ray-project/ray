
# coding: utf-8
import os
import sys
from typing import Dict

import pytest  # noqa

from ray.autoscaler.v2.schema import ClusterStatus, Stats
from ray.autoscaler.v2.utils import ClusterStatusParser
from ray._private.test_utils import load_test_config
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig

from ray.core.generated.experimental.autoscaler_pb2 import (
    GetClusterStatusReply,
)
from google.protobuf.json_format import ParseDict


def _gen_cluster_status_reply(data: Dict):
    return ParseDict(data, GetClusterStatusReply(), ignore_unknown_fields=True)



def test_cluster_status_parser():
    test_data = {
        "cluster_resource_state": {
            "node_states": [
                {
                    "node_id": b"1"*4,
                    "instance_id": "instance1",
                    "ray_node_type_name": "head_node",
                    "available_resources": {
                        "CPU": 1,
                    },
                    "total_resources": {
                        "CPU": 1,
                    },
                    "status": "RUNNING",
                    "node_ip_address": "10.10.10.10",
                    "instance_type_name": "m5.large",
                }
            ],
            "pending_resource_requests": [
            ]
        },
        "autoscaling_state": {}
    }
    reply = _gen_cluster_status_reply(test_data)
    stats = Stats()
    cluster_status =  ClusterStatusParser.from_get_cluster_status_reply(reply, stats)

    print(cluster_status)
    assert False



if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))