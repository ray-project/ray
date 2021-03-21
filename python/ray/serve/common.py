from pydantic import BaseModel
from typing import Dict, Any
from uuid import UUID

import numpy as np

from ray.serve.config import BackendConfig, ReplicaConfig

BackendTag = str
EndpointTag = str
ReplicaTag = str
NodeId = str
GoalId = UUID
Duration = float


class BackendInfo(BaseModel):
    # TODO(architkulkarni): Add type hint for worker_class after upgrading
    # cloudpickle and adding types to RayServeWrappedReplica
    worker_class: Any
    backend_config: BackendConfig
    replica_config: ReplicaConfig

    class Config:
        # TODO(architkulkarni): Remove once ReplicaConfig is a pydantic
        # model
        arbitrary_types_allowed = True


class TrafficPolicy:
    def __init__(self, traffic_dict: Dict[str, float]) -> None:
        self.traffic_dict: Dict[str, float] = dict()
        self.shadow_dict: Dict[str, float] = dict()
        self.set_traffic_dict(traffic_dict)

    @property
    def backend_tags(self):
        return set(self.traffic_dict.keys()).union(
            set(self.shadow_dict.keys()))

    def set_traffic_dict(self, traffic_dict: Dict[str, float]) -> None:
        prob = 0
        for backend, weight in traffic_dict.items():
            if weight < 0:
                raise ValueError(
                    "Attempted to assign a weight of {} to backend '{}'. "
                    "Weights cannot be negative.".format(weight, backend))
            prob += weight

        # These weights will later be plugged into np.random.choice, which
        # uses a tolerance of 1e-8.
        if not np.isclose(prob, 1, atol=1e-8):
            raise ValueError("Traffic dictionary weights must sum to 1, "
                             "currently they sum to {}".format(prob))
        self.traffic_dict = traffic_dict

    def set_shadow(self, backend: str, proportion: float):
        if proportion == 0 and backend in self.shadow_dict:
            del self.shadow_dict[backend]
        else:
            self.shadow_dict[backend] = proportion

    def __repr__(self) -> str:
        return f"<Traffic {self.traffic_dict}; Shadow {self.shadow_dict}>"
