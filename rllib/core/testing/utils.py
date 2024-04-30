from typing import Union

import gymnasium as gym

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.learner_group import LearnerGroup
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
def add_module_to_learner_or_learner_group(
    config: AlgorithmConfig,
    env: "gym.Env",
    module_id: str,
    learner_group_or_learner: Union[LearnerGroup, "Learner"],
):
    learner_group_or_learner.add_module(
        module_id=module_id,
        module_spec=config.get_marl_module_spec(env=env).module_specs[
            DEFAULT_MODULE_ID
        ],
    )
