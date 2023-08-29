"""
Test that no requests fail due to graceful node draining.

1) Start a NodeDrainerActor that keeps draining randomly selected worker nodes.
2) While the NodeDrainerActor is running, keep sending requests to the head node
   http proxy and make sure requests don't fail.
"""

import os
import json
import time
import aiohttp
import asyncio
import random
import logging
from typing import Dict

import ray
from ray import serve
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2
from ray.serve.context import get_global_client
from ray.serve.schema import ServeInstanceDetails
from ray.serve._private.common import ReplicaState
from ray._private.test_utils import wait_for_condition
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.serve._private.default_impl import create_cluster_node_info_cache


@ray.remote(num_cpus=0)
class NodeDrainerActor:
    def __init__(self, head_node_id, app_name, deployment_name, num_replicas):
        self._head_node_id = head_node_id
        self._app_name = app_name
        self._deployment_name = deployment_name
        self._num_replicas = num_replicas
        self._serve_client = get_global_client()
        self._gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
        self._cluster_node_info_cache = create_cluster_node_info_cache(self._gcs_client)
        self._drained_nodes = set()
        self._stop_event = asyncio.Event()

    def _get_running_replicas(self) -> Dict[str, str]:
        serve_details = ServeInstanceDetails(
            **ray.get(
                self._serve_client._controller.get_serve_instance_details.remote()
            )
        )
        running_replicas = {
            replica.replica_id: replica.node_id
            for replica in serve_details.applications[self._app_name]
            .deployments[f"{self._app_name}_{self._deployment_name}"]
            .replicas
            if replica.state == ReplicaState.RUNNING
        }
        return running_replicas

    def _check_node_is_drained(self, node_id):
        self._cluster_node_info_cache.update()
        return node_id not in self._cluster_node_info_cache.get_alive_node_ids()

    async def run(self):
        while not self._stop_event.is_set():
            wait_for_condition(
                lambda: len(self._get_running_replicas()) == self._num_replicas,
                timeout=3600,
            )

            # Randomly select a worker node to drain.
            candidate_node_ids = set(self._get_running_replicas().values()) - {
                self._head_node_id
            }
            draining_node_id = random.choice(tuple(candidate_node_ids))
            logging.info(f"Start to drain node {draining_node_id}.")
            is_accepted = self._gcs_client.drain_node(
                draining_node_id,
                autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
                "spot instance preemption",
            )
            assert is_accepted
            wait_for_condition(
                lambda: self._check_node_is_drained(draining_node_id), timeout=60
            )
            self._drained_nodes.add(draining_node_id)
            logging.info(f"Node {draining_node_id} is drained.")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=0.1)
            except asyncio.exceptions.TimeoutError:
                pass

        logging.info(
            f"Finish draining nodes. Drained {len(self._drained_nodes)} nodes."
        )
        return self._drained_nodes

    async def stop(self):
        self._stop_event.set()


@serve.deployment
class Deployment:
    def __call__(self):
        time.sleep(3)
        return "hello"


async def send_requests_single_client(num_requests_to_send):
    async def fetch(session):
        async with session.get("http://localhost:8000/") as response:
            if response.status != 200:
                logging.error(f"Request failed, response is {str(response)}.")
                return False
            response = await response.text()
            assert response == "hello", response
            return True

    async with aiohttp.ClientSession() as session:
        for _ in range(num_requests_to_send):
            res = await fetch(session)
            if not res:
                return False

        return True


async def send_requests(num_clients, num_requests_to_send_per_client):
    client_tasks = [send_requests_single_client for _ in range(num_clients)]

    res = await asyncio.gather(
        *[client_task(num_requests_to_send_per_client) for client_task in client_tasks]
    )

    return all(res)


def main():
    ray.init()

    head_node_id = ray.get_runtime_context().get_node_id()
    num_replicas = 50

    serve.run(
        Deployment.options(name="deployment", num_replicas=num_replicas).bind(),
        name="default",
    )

    node_drainer_actor = NodeDrainerActor.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(head_node_id, soft=False)
    ).remote(
        head_node_id=head_node_id,
        app_name="default",
        deployment_name="deployment",
        num_replicas=num_replicas,
    )
    run_obj_ref = node_drainer_actor.run.remote()

    num_clients = 20

    # Send requests and make sure no requests fail due to node draining.
    res = asyncio.run(
        send_requests(num_clients=num_clients, num_requests_to_send_per_client=300)
    )

    assert res, "Some requests failed due to node draining."

    node_drainer_actor.stop.remote()
    drained_nodes = ray.get(run_obj_ref)

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        results = {
            "success": 1,
            "num_drained_nodes": len(drained_nodes),
        }

        f.write(json.dumps(results))

    serve.shutdown()
    logging.info(
        f"Successfully drained {len(drained_nodes)} nodes with no failed requests."
    )


if __name__ == "__main__":
    main()
