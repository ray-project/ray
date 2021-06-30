import json
import os
import random
import string
import time

import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster

# Global variables / constants appear only right after imports.
# Ray serve deployment setup constants
NUM_REPLICAS = 7
MAX_BATCH_SIZE = 16

# Cluster setup constants
NUM_REDIS_SHARDS = 1
REDIS_MAX_MEMORY = 10**8
OBJECT_STORE_MEMORY = 10**8
NUM_NODES = 4

# RandomTest setup constants
CPUS_PER_NODE = 10


def update_progress(result):
    """
    Write test result json to /tmp/, which will be read from
    anyscale product runs in each releaser test
    """
    result["last_update"] = time.time()
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/release_test_output.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


cluster = Cluster()
for i in range(NUM_NODES):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=NUM_REDIS_SHARDS if i == 0 else None,
        num_cpus=16,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=OBJECT_STORE_MEMORY,
        redis_max_memory=REDIS_MAX_MEMORY,
        dashboard_host="0.0.0.0",
    )

ray.init(
    namespace="serve_failure_test",
    address=cluster.address,
    dashboard_host="0.0.0.0",
    log_to_driver=False)
serve.start(detached=True)


@ray.remote
class RandomKiller:
    def __init__(self, kill_period_s=1):
        self.client = serve.connect()
        self.kill_period_s = kill_period_s

    def _get_all_serve_actors(self):
        controller = self.client._controller
        routers = list(ray.get(controller.get_http_proxies.remote()).values())
        all_handles = routers + [controller]
        worker_handle_dict = ray.get(controller._all_replica_handles.remote())
        for _, replica_dict in worker_handle_dict.items():
            all_handles.extend(list(replica_dict.values()))

        return all_handles

    def run(self):
        while True:
            ray.kill(
                random.choice(self._get_all_serve_actors()), no_restart=False)
            time.sleep(self.kill_period_s)


class RandomTest:
    def __init__(self, max_deployments=1):
        self.max_deployments = max_deployments
        self.weighted_actions = [
            (self.create_deployment, 1),
            (self.verify_deployment, 4),
        ]
        self.deployments = []
        for _ in range(max_deployments):
            self.create_deployment()

    def create_deployment(self):
        if len(self.deployments) == self.max_deployments:
            deployment_to_delete = self.deployments.pop()
            serve.get_deployment(deployment_to_delete).delete()

        new_name = "".join(
            [random.choice(string.ascii_letters) for _ in range(10)])

        @serve.deployment(name=new_name)
        def handler(self, *args):
            return new_name

        handler.deploy()

        self.deployments.append(new_name)

    def verify_deployment(self):
        deployment = random.choice(self.deployments)
        for _ in range(100):
            try:
                r = requests.get("http://127.0.0.1:8000/" + deployment)
                assert r.text == deployment
            except Exception:
                print("Request to {} failed.".format(deployment))
                time.sleep(0.01)

    def run(self):
        iteration = 0
        start_time = time.time()
        previous_time = start_time
        while True:
            for _ in range(100):
                actions, weights = zip(*self.weighted_actions)
                random.choices(actions, weights=weights)[0]()

            new_time = time.time()
            print("Iteration {}:\n"
                  "  - Iteration time: {}.\n"
                  "  - Absolute time: {}.\n"
                  "  - Total elapsed time: {}.".format(
                      iteration, new_time - previous_time, new_time,
                      new_time - start_time))
            previous_time = new_time
            iteration += 1

            update_progress({
                "iteration": iteration,
                "iteration_time": new_time - previous_time,
                "absolute_time": new_time,
                "elapsed_time": new_time - start_time,
            })


random_killer = RandomKiller.remote()
random_killer.run.remote()
RandomTest(max_deployments=NUM_NODES * CPUS_PER_NODE).run()
