"""
Branching Dueling Q-Network (BDQ) 
==============================================

This file defines the Trainer class for the Branching Dueling Q-Network
algorithm. It inherits from the DQNTrainer class with minor modifications.
See `bdq_tf_policy.py` for the definition of the policy.

For more information, see the Branching Dueling Q-Network paper https://arxiv.org/abs/1711.08946
"""


from typing import Optional, Type

from ray.rllib.agents.bdq.simple_bdq_tf_policy import SimpleBDQTFPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict

from ray.rllib.agents.dqn.dqn import DEFAULT_CONFIG, DQNTrainer

# fmt: off
# __sphinx_doc_begin__
BDQ_DEFAULT_CONFIG = merge_dicts(DEFAULT_CONFIG, {
    # env_config
    "env_config": {"action_per_branch": 8},
    # bdq model
    "model": {
        "custom_model": "bdq_tf_model",
        # Extra kwargs to be passed to your model's c'tor.
        "custom_model_config": {
            "hiddens_common": [512, 256],
            "hiddens_actions": [128],
            "hiddens_value": [128],
            "action_per_branch": 8,
        },
    },
})
# __sphinx_doc_end__
# fmt: on

class BDQTrainer(DQNTrainer):
    @classmethod
    @override(DQNTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return BDQ_DEFAULT_CONFIG
    
    @override(DQNTrainer)
    def get_default_policy_class(
        self, config: TrainerConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return NotImplementedError
        else:
            return SimpleBDQTFPolicy
