import argparse
import logging
import asyncio
import grpc
import ray

from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc

# from ray._private.test_utils import get_and_run_node_killer


def get_and_run_node_killer(node_kill_interval_s,
                            namespace=None,
                            lifetime="",
                            no_start=False):
    assert ray.is_initialized(), (
        "The API is only available when Ray is initialized.")

    @ray.remote(num_cpus=0)
    class NodeKillerActor:
        def __init__(self, head_node_id, node_kill_interval_s: float = 60):
            self.node_kill_interval_s = node_kill_interval_s
            self.is_running = False
            self.head_node_id = head_node_id
            self.killed_nodes = set()
            # -- logger. --
            logging.basicConfig(level=logging.INFO)

        def ready(self):
            pass

        async def run(self):
            self.is_running = True
            while self.is_running:
                node_to_kill_ip = None
                node_to_kill_port = None
                nodes = ray.nodes()
                alive_nodes = self._get_alive_nodes(nodes)
                for node in nodes:
                    node_id = node["NodeID"]
                    # make sure at least 1 worker node is alive.
                    if (node["Alive"] and node_id != self.head_node_id
                            and node_id not in self.killed_nodes
                            and alive_nodes > 2):
                        node_to_kill_ip = node["NodeManagerAddress"]
                        node_to_kill_port = node["NodeManagerPort"]
                        break

                if node_to_kill_port is not None:
                    self._kill_raylet(
                        node_to_kill_ip, node_to_kill_port, graceful=False)
                    logging.info(
                        "Killing a node of address: "
                        f"{node_to_kill_ip}, port: {node_to_kill_port}")
                    self.killed_nodes.add(node_id)
                await asyncio.sleep(self.node_kill_interval_s)

        async def stop_run(self):
            was_running = self.is_running
            self.is_running = False
            return was_running

        async def get_total_killed_nodes(self):
            """Get the total number of killed nodes"""
            return len(self.killed_nodes)

        def _kill_raylet(self, ip, port, graceful=False):
            raylet_address = f"{ip}:{port}"
            channel = grpc.insecure_channel(raylet_address)
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            stub.ShutdownRaylet(
                node_manager_pb2.ShutdownRayletRequest(graceful=graceful))

        def _get_alive_nodes(self, nodes):
            alive_nodes = 0
            for node in nodes:
                if node["Alive"]:
                    alive_nodes += 1
            return alive_nodes

    head_node_ip = ray.worker.global_worker.node_ip_address
    head_node_id = ray.worker.global_worker.current_node_id.hex()
    # Schedule the actor on the current node.
    node_killer = NodeKillerActor.options(
        resources={
            f"node:{head_node_ip}": 0.001
        },
        namespace=namespace,
        name="node_killer",
        lifetime=lifetime).remote(
            head_node_id, node_kill_interval_s=node_kill_interval_s)
    print("Waiting for node killer actor to be ready...")
    ray.get(node_killer.ready.remote())
    print("Node killer actor is ready now.")
    if not no_start:
        node_killer.run.remote()
    return node_killer


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-kill-interval", type=int, default=60)
    parser.add_argument(
        "--no-start",
        action="store_true",
        default=False,
        help=("If set, node killer won't be started when "
              "the script is done. Script needs to manually "
              "obtain the node killer handle and invoke run method to "
              "start a node killer."))
    return parser.parse_known_args()


def main():
    args, _ = parse_script_args()
    ray.init(address="auto")
    get_and_run_node_killer(
        args.node_kill_interval,
        namespace="release_test_namespace",
        lifetime="detached",
        no_start=args.no_start)
    print("Successfully deployed a node killer.")


main()
