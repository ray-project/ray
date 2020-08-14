"""
Implementation of a vanilla policy gradients algorithm. Based on:

[1] - Policy Gradient Methods for Reinforcement Learning with Function
    Approximation.
    Sutton, McAllester, Singh, Mansour - AT&T Labs - 1999
    https://papers.nips.cc/paper/1713-policy-gradient-methods-for-
    reinforcement-learning-with-function-approximation.pdf
[2] - Simple Statistical Gradient-Following Algorithms for Connectionist
    Reinforcement Learning.
    Williams - College of Computer Science - Northeastern University - 1992
    http://www-anw.cs.umass.edu/~barto/courses/cs687/williams92simple.pdf
"""

from typing import Type

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.pg.config_schema import pg_config_schema
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import TrainerConfigDict

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


def get_policy_class(config: TrainerConfigDict) -> Type[Policy]:
    """Policy class picker function. Class is chosen based on DL-framework.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Type[Policy]: The Policy class to use with PGTrainer.
    """
    if config["framework"] == "torch":
        return PGTorchPolicy
    else:
        return PGTFPolicy


# Build a child class of `Trainer`, which uses the framework specific Policy
# determined in `get_policy_class()` above.
PGTrainer = build_trainer(
    name="PG",
    default_config=DEFAULT_CONFIG,
    config_schema=pg_config_schema,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class)
