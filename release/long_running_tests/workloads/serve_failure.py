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
client = serve.start(detached=True)


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
    def __init__(self, serve_client, max_endpoints=1):
        self.client = serve_client
        self.max_endpoints = max_endpoints
        self.weighted_actions = [
            (self.create_endpoint, 1),
            (self.verify_endpoint, 4),
        ]
        self.endpoints = []
        for _ in range(max_endpoints):
            self.create_endpoint()

    def create_endpoint(self):
        if len(self.endpoints) == self.max_endpoints:
            endpoint_to_delete = self.endpoints.pop()
            self.client.delete_endpoint(endpoint_to_delete)
            self.client.delete_backend(endpoint_to_delete)

        new_endpoint = "".join(
            [random.choice(string.ascii_letters) for _ in range(10)])

        def handler(self, *args):
            return new_endpoint

        self.client.create_backend(new_endpoint, handler)
        self.client.create_endpoint(
            new_endpoint, backend=new_endpoint, route="/" + new_endpoint)

        self.endpoints.append(new_endpoint)

    def verify_endpoint(self):
        endpoint = random.choice(self.endpoints)
        for _ in range(100):
            try:
                r = requests.get("http://127.0.0.1:8000/" + endpoint)
                assert r.text == endpoint
            except Exception:
                print("Request to {} failed.".format(endpoint))
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
# Subtract 1 CPU for the controller and 1 CPU from each node for the HTTP
# server.
RandomTest(client, max_endpoints=(num_nodes * (cpus_per_node - 1)) - 1).run()
