import asyncio
import logging
import os
import random
import string
import time
from typing import List

import requests

import ray
from ray import serve
from ray.serve.context import _get_global_client
from ray.cluster_utils import Cluster
from ray._private.test_utils import safe_write_to_results_json

# Global variables / constants appear only right after imports.
# Ray serve deployment setup constants
NUM_REPLICAS = 7
MAX_BATCH_SIZE = 16

# Cluster setup constants
OBJECT_STORE_MEMORY = 10**8
NUM_NODES = 4

# RandomTest setup constants
CPUS_PER_NODE = 10
NUM_ITERATIONS = 350
ACTIONS_PER_ITERATION = 20

RAY_UNIT_TEST = "RAY_UNIT_TEST" in os.environ

# RAY_OVERRIDE_RESOURCES will prevent "num_cpus" from working
# when adding a node with custom resources to Cluster.
if "RAY_OVERRIDE_RESOURCES" in os.environ:
    del os.environ["RAY_OVERRIDE_RESOURCES"]


def update_progress(result):
    """
    Write test result json to /tmp/, which will be read from
    anyscale product runs in each releaser test
    """
    result["last_update"] = time.time()
    safe_write_to_results_json(result)


cluster = Cluster()
for i in range(NUM_NODES):
    cluster.add_node(
        redis_port=6379 if i == 0 else None,
        num_cpus=16,
        num_gpus=0,
        resources={str(i): 2},
        object_store_memory=OBJECT_STORE_MEMORY,
        dashboard_host="0.0.0.0",
    )

ray.init(
    namespace="serve_failure_test",
    address=cluster.address,
    dashboard_host="0.0.0.0",
    log_to_driver=True,
)
serve.start(proxy_location="HeadOnly")


@ray.remote(max_restarts=-1, max_task_retries=-1)
class RandomKiller:
    def __init__(self, kill_period_s=1):
        self.kill_period_s = kill_period_s
        self.sanctuary = set()

    async def run(self):
        while True:
            chosen = random.choice(self._get_serve_actors())
            print(f"Killing {chosen}")
            ray.kill(chosen, no_restart=False)
            await asyncio.sleep(self.kill_period_s)

    async def spare(self, app_name: str):
        print(f'Sparing application "{app_name}" replicas.')
        self.sanctuary.add(app_name)

    async def stop_spare(self, app_name: str):
        print(f'No longer sparing application "{app_name}" replicas.')
        self.sanctuary.discard(app_name)

    def _get_serve_actors(self):
        controller = _get_global_client()._controller
        routers = list(ray.get(controller.get_proxies.remote()).values())
        all_handles = routers + [controller]
        replica_dict = ray.get(controller._all_running_replicas.remote())
        for deployment_id, replica_info_list in replica_dict.items():
            if deployment_id.app not in self.sanctuary:
                for replica_info in replica_info_list:
                    all_handles.append(replica_info.actor_handle)

        return all_handles


class RandomTest:
    def __init__(self, random_killer_handle, max_applications=1):
        self.max_applications = max_applications
        self.weighted_actions = [
            (self.create_application, 1),
            (self.verify_application, 4),
        ]
        self.applications = []

        self.random_killer = random_killer_handle

        # Deploy in parallel to avoid long test startup time.
        self.wait_for_applications_running(
            [self.create_application(blocking=False) for _ in range(max_applications)]
        )

        self.random_killer.run.remote()

    def wait_for_applications_running(self, application_names: List[str]):
        client = _get_global_client()
        for name in application_names:
            client._wait_for_application_running(name, timeout_s=60)

    def create_application(self, blocking: bool = True) -> str:
        if len(self.applications) == self.max_applications:
            app_to_delete = self.applications.pop()
            serve.delete(app_to_delete)

        new_name = "".join([random.choice(string.ascii_letters) for _ in range(10)])

        @serve.deployment(name=new_name)
        def handler(self, *args):
            logging.getLogger("ray.serve").setLevel(logging.ERROR)
            return new_name

        if blocking:
            ray.get(self.random_killer.spare.remote(new_name))
            serve._run(
                handler.bind(),
                name=new_name,
                route_prefix=f"/{new_name}",
                _blocking=True,
            )
            self.applications.append(new_name)
            ray.get(self.random_killer.stop_spare.remote(new_name))
        else:
            serve._run(
                handler.bind(),
                name=new_name,
                route_prefix=f"/{new_name}",
                _blocking=False,
            )
            self.applications.append(new_name)

        return new_name

    def verify_application(self):
        app = random.choice(self.applications)
        for _ in range(100):
            try:
                r = requests.get("http://127.0.0.1:8000/" + app)
                assert r.text == app
            except Exception:
                print("Request to {} failed.".format(app))
                time.sleep(0.1)

    def run(self):
        start_time = time.time()
        previous_time = start_time
        for iteration in range(NUM_ITERATIONS):
            for _ in range(ACTIONS_PER_ITERATION):
                actions, weights = zip(*self.weighted_actions)
                action_chosen = random.choices(actions, weights=weights)[0]
                print(f"Executing {action_chosen}")
                action_chosen()

            new_time = time.time()
            print(
                f"Iteration {iteration}:\n"
                f"  - Iteration time: {new_time - previous_time}.\n"
                f"  - Absolute time: {new_time}.\n"
                f"  - Total elapsed time: {new_time - start_time}."
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

            if RAY_UNIT_TEST:
                break


random_killer = RandomKiller.remote()
tester = RandomTest(random_killer, max_applications=NUM_NODES * CPUS_PER_NODE)
tester.run()
