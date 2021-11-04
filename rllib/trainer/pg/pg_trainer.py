"""
Policy Gradient (PG)
====================

This file defines the distributed Trainer class for policy gradients.
See `pg_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation: https://docs.ray.io/en/master/rllib-algorithms.html#pg
"""
from typing import Type

from ray.rllib.agents.trainer import Trainer
from ray.rllib.trainer.pg.default_config import DEFAULT_CONFIG
from ray.rllib.trainer.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.trainer.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict


class PGTrainer(Trainer):
    @override(Trainer)
    def get_default_policy_class(self) -> Type[Policy]:
        return PGTorchPolicy if self.config["framework"] == "torch" \
            else PGTFPolicy

    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG
