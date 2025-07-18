import asyncio

import ray
from .resource_killer import ResourceKillerActor


class NodeKillerBase(ResourceKillerActor):
    async def _find_resources_to_kill(self):
        nodes_to_kill = []
        while not nodes_to_kill and self.is_running:
            worker_nodes = [
                node
                for node in ray.nodes()
                if node["Alive"]
                and (node["NodeID"] != self.head_node_id)
                and (node["NodeID"] not in self.killed)
            ]
            if self.kill_filter_fn:
                candidates = list(filter(self.kill_filter_fn(), worker_nodes))
            else:
                candidates = worker_nodes

            # Ensure at least one worker node remains alive.
            if len(worker_nodes) < self.batch_size_to_kill + 1:
                # Give the cluster some time to start.
                await asyncio.sleep(1)
                continue

            # Collect nodes to kill, limited by batch size.
            for candidate in candidates[: self.batch_size_to_kill]:
                nodes_to_kill.append(
                    (
                        candidate["NodeID"],
                        candidate["NodeManagerAddress"],
                        candidate["NodeManagerPort"],
                    )
                )

        return nodes_to_kill
