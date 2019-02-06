import ray
import numpy as np
import time
from collections import Counter
import json
import sys

LOCAL = len(sys.argv) > 1

# Start the Ray processes.
if LOCAL:
    print("Starting test locally...")
    from ray.test.cluster_utils import Cluster
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 10,
            "_internal_config": json.dumps({
                "initial_reconstruction_timeout_milliseconds": 1000,
                "num_heartbeats_timeout": 10,
            })
        })
    num_nodes = 10
    for i in range(num_nodes - 1):
        node_to_kill = cluster.add_node(
            num_cpus=10,
            num_gpus=10,
            _internal_config=json.dumps({
                "initial_reconstruction_timeout_milliseconds": 1000,
                "num_heartbeats_timeout": 10,
            }))
    ray.init(redis_address=cluster.redis_address)
else:
    num_nodes = 100
    ray.init(redis_address="localhost:6379")


@ray.remote(num_gpus=1)
class EventLoop(object):
    def __init__(self, num_children):
        self.children = [Child.remote() for _ in range(num_children)]
        self.nodes = ray.get([child.node.remote() for child in self.children])
        self.counts = Counter()

        self.tasks = [child.ping.remote() for child in self.children]
        self.index = {task: i for i, task in enumerate(self.tasks)}

    def ping(self):
        ready, unready = ray.wait(self.tasks, num_returns=1)
        for task in ready:
            i = self.index[task]
            new_task = self.children[i].ping.remote()
            self.tasks[i] = new_task
            self.index[new_task] = i
            self.counts[self.nodes[i]] += 1

    def counts(self):
        return dict(self.counts)


@ray.remote
class Child(object):
    def __init__(self):
        return

    def ping(self):
        time.sleep(np.random.rand())
        return

    def node(self):
        return ray.worker.global_worker.plasma_client.store_socket_name


num_children = 4
num_actors = num_nodes // 3
actors = [EventLoop.remote(num_children) for _ in range(num_actors)]

start_time = time.time()
interval_time = start_time
num_rounds = 0

tasks = [actor.ping.remote() for actor in actors]
index = {task: i for i, task in enumerate(tasks)}

while time.time() - start_time < 60:
    ready, unready = ray.wait(tasks, num_returns=1)
    for task in ready:
        i = index[task]
        new_task = actors[i].ping.remote()
        tasks[i] = new_task
        index[new_task] = i

    num_rounds += 1
    if time.time() - interval_time > 10:
        print(num_rounds, "more tasks after", time.time() - start_time)
        interval_time = time.time()
        num_rounds = 0

        if LOCAL and time.time() - start_time > 20 and time.time(
        ) - start_time < 30:
            print("Removing node")
            node_to_kill = cluster.list_all_nodes()[-1]
            cluster.remove_node(node_to_kill)

node_count = Counter()
for i, actor in enumerate(actors):
    print("Collecting results from EventLoop actor", i)
    try:
        counts = ray.get(actor.counts.remote())
    except Exception:
        print("EventLoop actor", i, "died")
        continue

    for node, count in counts.items():
        node_count[node] += count

print("Final task counts per node:")
for node, count in node_count.items():
    print(node, count)
