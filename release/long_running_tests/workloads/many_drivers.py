# This workload tests many drivers using the same cluster.
import json
import os
import time
import argparse

import ray
from ray.cluster_utils import Cluster
from ray._private.test_utils import run_string_as_driver


def update_progress(result):
    result["last_update"] = time.time()
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/release_test_output.json"
    )
    # Safe write to file to guard against malforming the json
    # when the job gets interrupted in the middle of writing
    test_output_json_tmp = test_output_json + ".tmp"
    with open(test_output_json_tmp, "wt") as f:
        json.dump(result, f)
    os.replace(test_output_json_tmp, test_output_json)


num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 4

message = (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)
assert (
    num_nodes * object_store_memory + num_redis_shards * redis_max_memory
    < ray._private.utils.get_system_memory() / 2
), message

# Simulate a cluster on one machine.

cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=4,
        num_gpus=0,
        resources={str(i): 5},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        dashboard_host="0.0.0.0",
    )
ray.init(address=cluster.address)

# Run the workload.

# Define a driver script that runs a few tasks and actors on each node in the
# cluster.
driver_script = """
import ray

ray.init(address="{}")

num_nodes = {}


@ray.remote
def f():
    return 1


@ray.remote
class Actor(object):
    def method(self):
        return 1


for _ in range(5):
    for i in range(num_nodes):
        assert (ray.get(
            f._remote(args=[],
            kwargs={{}},
            resources={{str(i): 1}})) == 1)
        actor = Actor._remote(
            args=[], kwargs={{}}, resources={{str(i): 1}})
        assert ray.get(actor.method.remote()) == 1

# Tests datasets doesn't leak workers.
ray.data.range(100).map(lambda x: x).take()

print("success")
""".format(
    cluster.address, num_nodes
)


@ray.remote
def run_driver():
    output = run_string_as_driver(driver_script, encode="utf-8")
    assert "success" in output


iteration = 0
running_ids = [
    run_driver._remote(args=[], kwargs={}, num_cpus=0, resources={str(i): 0.01})
    for i in range(num_nodes)
]
start_time = time.time()
previous_time = start_time

parser = argparse.ArgumentParser(prog="Many Drivers long running tests")
parser.add_argument(
    "--iteration-num", type=int, help="How many iterations to run", required=False
)
parser.add_argument(
    "--smoke-test",
    action="store_true",
    help="Whether or not the test is smoke test.",
    default=False,
)
args = parser.parse_args()

iteration_num = args.iteration_num
if args.smoke_test:
    iteration_num = 400
while True:
    if iteration_num is not None and iteration_num < iteration:
        break
    # Wait for a driver to finish and start a new driver.
    [ready_id], running_ids = ray.wait(running_ids, num_returns=1)
    ray.get(ready_id)

    running_ids.append(
        run_driver._remote(
            args=[], kwargs={}, num_cpus=0, resources={str(iteration % num_nodes): 0.01}
        )
    )

    new_time = time.time()
    print(
        "Iteration {}:\n"
        "  - Iteration time: {}.\n"
        "  - Absolute time: {}.\n"
        "  - Total elapsed time: {}.".format(
            iteration, new_time - previous_time, new_time, new_time - start_time
        )
    )
    update_progress(
        {
            "iteration": iteration,
            "iteration_time": new_time - previous_time,
            "absolute_time": new_time,
            "elapsed_time": new_time - start_time,
        }
    )
    previous_time = new_time
    iteration += 1
