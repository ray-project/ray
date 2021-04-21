import random
import string
import time

import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster

num_redis_shards = 1
redis_max_memory = 10**8
object_store_memory = 10**8
num_nodes = 4
cpus_per_node = 10
cluster = Cluster()
for i in range(num_nodes):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_redis_shards=num_redis_shards if i == 0 else None,
        num_cpus=16,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=object_store_memory,
        redis_max_memory=redis_max_memory,
        dashboard_host="0.0.0.0",
    )

ray.init(
    address=cluster.address, dashboard_host="0.0.0.0", log_to_driver=False)
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
            serve.delete_deployment(deployment_to_delete)

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


random_killer = RandomKiller.remote()
random_killer.run.remote()
RandomTest(max_deployments=num_nodes * cpus_per_node).run()
