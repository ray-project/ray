from ray.rllib.algorithms.cql.cql import CQLTrainer, CQL_DEFAULT_CONFIG
from ray.rllib.algorithms.cql.cql_torch_policy import CQLTorchPolicy

__all__ = [
    "CQL_DEFAULT_CONFIG",
    "CQLTorchPolicy",
    "CQLTrainer",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.dreamer", "ray.rllib.algorithms.dreamer", error=False
)
