"""
Policy Gradient (PG)
====================

This file defines the distributed Trainer class for policy gradients.
See `pg_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation: https://docs.ray.io/en/master/rllib-algorithms.html#pg
"""
from typing import Type

from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.trainer.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.trainer.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.typing import PartialTrainerConfigDict, TrainerConfigDict

# yapf: disable
# __sphinx_doc_begin__

# Adds the following updates to the (base) `Trainer` config in
# rllib/agents/trainer.py (`COMMON_CONFIG` dict).
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default.
    "num_workers": 0,
    # Learning rate.
    "lr": 0.0004,
})

# __sphinx_doc_end__
# yapf: enable


class PGTrainer(Trainer):

    @ExperimentalAPI
    def train_one_step(self):
        pass  # TODO: implement pythonic exec function for PG

    # TODO: Deprecate (will be called by Trainer super class for backward
    #  compat reasons, but we shouldn't use this anymore)
    def _init(self, config, env_creator):
        pass

    @override(Trainer)
    def get_default_policy_class(self) -> Type[Policy]:
        return PGTorchPolicy if self.config["framework"] == "torch" \
            else PGTFPolicy

    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG
