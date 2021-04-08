import sys

import pytest

import ray
import ray.util.client.server.server as ray_client_server

from ray.rllib.examples import rock_paper_scissors_multiagent
from ray.util.client.common import ClientObjectRef
from ray.util.client.ray_client_helpers import ray_start_client_server

def test_rllib_integration(call_ray_stop_only):
    ray.init(num_cpus = 1)
    ray_client_server.serve("localhost:50051")
    # Connection timeout. Help.
    ray.util.connect("localhost:50051")

    rock_paper_scissors_multiagent.main()


if __name__ == "__main__":
    sys.exit(pytest.main(["-s", __file__]))
