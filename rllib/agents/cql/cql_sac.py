"""CQL (derived from SAC).
"""
from typing import Optional, Type

from ray.rllib.agents.cql.cql_sac_tf_policy import CQLSACTFPolicy
from ray.rllib.agents.sac.sac import SACTrainer, \
    DEFAULT_CONFIG as SAC_CONFIG
from ray.rllib.agents.cql.cql_sac_torch_policy import CQLSACTorchPolicy
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.policy.policy import Policy
from ray.rllib.utils import merge_dicts

# yapf: disable
# __sphinx_doc_begin__
CQLSAC_DEFAULT_CONFIG = merge_dicts(
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
        # Initial value to use for the Alpha Prime (in CQL Loss).
        "initial_alpha_prime": 1.0,
        # The default value is set as the same of SAC which is good for
        # online training. For offline training we could start to optimize
        # the models right away.
        "learning_starts": 1500,
        # Replay Buffer should be size of offline dataset for fastest
        # training
        "buffer_size": 1000000,
        # Upper bound for alpha value during the lagrangian constraint
        "alpha_upper_bound": 1.0,
        # Lower bound for alpha value during the lagrangian constraint
        "alpha_lower_bound": 0.0,
    })
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    if config["framework"] == "torch":
        return CQLSACTorchPolicy
    else:
        return CQLSACTFPolicy


CQLSACTrainer = SACTrainer.with_updates(
    name="CQL_SAC",
    default_config=CQLSAC_DEFAULT_CONFIG,
    default_policy=CQLSACTFPolicy,
    get_policy_class=get_policy_class,
)
