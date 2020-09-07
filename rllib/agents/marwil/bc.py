"""Behavioral Cloning (derived from MARWIL).

Simply uses the MARWIL agent with beta force-set to 0.0.
"""
from ray.rllib.agents.marwil.marwil import MARWILTrainer, \
    DEFAULT_CONFIG as MARWIL_CONFIG
from ray.rllib.utils.typing import TrainerConfigDict

BC_DEFAULT_CONFIG = MARWILTrainer.merge_trainer_configs(
    MARWIL_CONFIG, {
        "beta": 0.0,
    })


def validate_config(config: TrainerConfigDict):
    if config["beta"] != 0.0:
        raise ValueError(
            "For behavioral cloning, `beta` parameter must be 0.0!")


BCTrainer = MARWILTrainer.with_updates(
    name="BC",
    default_config=BC_DEFAULT_CONFIG,
    validate_config=validate_config,
)
