from typing import List

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.load_metrics import LoadMetrics


class ResourceDemandScheduler:
    def __init__(self, provider: NodeProvider, load_metrics: LoadMetrics,
                 instance_types: dict, max_workers: int):
        self.provider = provider
        self.load_metrics = load_metrics
        self.instance_types = instance_types
        self.max_workers = max_workers

    def get_instances_to_launch(self, existing_nodes: List[str]):
        return []
