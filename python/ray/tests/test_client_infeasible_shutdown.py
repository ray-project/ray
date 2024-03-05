import ray
import os
import time
import sys
import subprocess
from ray._private.test_utils import (
    convert_actor_state,
    make_global_state_accessor,
    wait_for_condition,
)
import ray._private.gcs_utils as gcs_utils
import pytest

CLIENT_SERVER_PORT = 25555

# Tests that if a client has infesible tasks when shutting down, the demands from tasks
# are removed from the resource usage.
# See https://github.com/ray-project/ray/issues/43687
@pytest.mark.parametrize(
    "call_ray_start",
    [f"ray start --head --ray-client-server-port={CLIENT_SERVER_PORT}"],
    indirect=True,
)
def test_client_infeasible_shutdown(call_ray_start):
    script = """
#!/usr/bin/env python3
import ray
import time

ray.init("ray://127.0.0.1:25555")

@ray.remote(num_cpus=1000)
def cant_schedule():
    return "absurd"

ref = cant_schedule.remote()
time.sleep(10)
"""

    for _ in range(10):
        subprocess.check_call([sys.executable, "-c", script])
    time.sleep(10)  # give gcs some time to update status
    cluster = ray.init()
    resource_usage_msg = make_global_state_accessor(cluster).get_all_resource_usage()
    resource_usage = gcs_utils.ResourceUsageBatchData.FromString(resource_usage_msg)
    assert (
        len(resource_usage.resource_load_by_shape.resource_demands) == 0
    ), resource_usage


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
