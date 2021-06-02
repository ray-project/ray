"""CQL (derived from DQN).
"""
from typing import Optional, Type

from ray.rllib.agents.cql.cql_dqn_tf_policy import CQLDQNTFPolicy
from ray.rllib.agents.dqn.dqn import DQNTrainer, \
    DEFAULT_CONFIG as DQN_CONFIG
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.policy.policy import Policy
from ray.rllib.utils import merge_dicts

# yapf: disable
# __sphinx_doc_begin__
CQLDQN_DEFAULT_CONFIG = merge_dicts(
    DQN_CONFIG, {
        # You should override this to point to an offline dataset.
        "input": "sampler",
        # Offline RL does not need IS estimators
        "input_evaluation": [],
        # Min Q Weight multiplier
        "min_q_weight": 1.0,
        # The default value is set as the same of DQN which is good for
        # online training. For offline training we could start to optimize
        # the models right away.
        "learning_starts": 1000,
        # Replay Buffer should be size of offline dataset for fastest
        # training
        "buffer_size": 1000000,
    })
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    if config["framework"] == "torch":
        raise ValueError("Torch CQL not implemented yet!")
    else:
        return CQLDQNTFPolicy


CQLDQNTrainer = DQNTrainer.with_updates(
    name="CQL_DQN",
    default_config=CQLDQN_DEFAULT_CONFIG,
    default_policy=CQLDQNTFPolicy,
    get_policy_class=get_policy_class,
)
