"""
Policy Gradient (PG)
====================

This file defines the distributed Trainer class for policy gradients.
See `pg_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation: https://docs.ray.io/en/master/rllib-algorithms.html#pg
"""

import logging
from typing import Optional, Type

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import TrainerConfigDict

logger = logging.getLogger(__name__)

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


def validate_config(config: TrainerConfigDict) -> None:
    """Validates the Trainer's config dict.

    Args:
        config (TrainerConfigDict): The Trainer's config to check.

    Raises:
        ValueError: In case something is wrong with the config.
    """
    # Switch on trajectory view API by default for this algo (if None).
    if config["_use_trajectory_view_api"] is None:
        logger.info(
            "Switching on Trajectory View API for PG by default. "
            "Set `_use_trajectory_view_api=False` explicitly in your config "
            "to switch this off.")
        config["_use_trajectory_view_api"] = True


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function. Class is chosen based on DL-framework.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with PGTrainer.
            If None, use `default_policy` provided in build_trainer().
    """
    if config["framework"] == "torch":
        return PGTorchPolicy


# Build a child class of `Trainer`, which uses the framework specific Policy
# determined in `get_policy_class()` above.
PGTrainer = build_trainer(
    name="PG",
    default_config=DEFAULT_CONFIG,
    validate_config=validate_config,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class,
)
