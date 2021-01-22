"""CQL (derived from SAC).
"""
from typing import Optional, Type

from ray.rllib.agents.sac.sac import SACTrainer, \
    DEFAULT_CONFIG as SAC_CONFIG
from ray.rllib.agents.cql.cql_torch_policy import CQLTorchPolicy
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.policy.policy import Policy
from ray.rllib.utils import merge_dicts

# yapf: disable
# __sphinx_doc_begin__
CQL_DEFAULT_CONFIG = merge_dicts(
    SAC_CONFIG, {
        # You should override this to point to an offline dataset.
        "input": "sampler",
        # Offline RL does not need IS estimators
        "input_evaluation": [],
        # Number of iterations with Behavior Cloning Pretraining
        "bc_iters": 20000,
        # CQL Loss Temperature
        "temperature": 1.0,
        # Num Actions to sample for CQL Loss
        "num_actions": 10,
        # Whether to use the Langrangian for Alpha Prime (in CQL Loss)
        "lagrangian": False,
        # Lagrangian Threshold
        "lagrangian_thresh": 5.0,
        # Min Q Weight multiplier
        "min_q_weight": 5.0,
    })
# __sphinx_doc_end__
# yapf: enable


def validate_config(config: TrainerConfigDict):
    if config["framework"] == "tf":
        raise ValueError("Tensorflow CQL not implemented yet!")


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    if config["framework"] == "torch":
        return CQLTorchPolicy


CQLTrainer = SACTrainer.with_updates(
    name="CQL",
    default_config=CQL_DEFAULT_CONFIG,
    validate_config=validate_config,
    default_policy=CQLTorchPolicy,
    get_policy_class=get_policy_class,
)
