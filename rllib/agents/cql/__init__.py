from ray.rllib.algorithms.cql.cql import CQL as CQLTrainer, CQL_DEFAULT_CONFIG
from ray.rllib.algorithms.cql.cql_tf_policy import CQLTFPolicy
from ray.rllib.algorithms.cql.cql_torch_policy import CQLTorchPolicy
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "CQL_DEFAULT_CONFIG",
    "CQLTFPolicy",
    "CQLTorchPolicy",
    "CQLTrainer",
]

deprecation_warning("ray.rllib.agents.cql", "ray.rllib.algorithms.cql", error=True)
