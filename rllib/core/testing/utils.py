from typing import Type, Union, TYPE_CHECKING

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.learner.learner_group import LearnerGroup
from ray.rllib.utils.annotations import DeveloperAPI

if TYPE_CHECKING:
    import gymnasium as gym
    import torch
    import tensorflow as tf

    from ray.rllib.core.learner.learner import Learner


Optimizer = Union["tf.keras.optimizers.Optimizer", "torch.optim.Optimizer"]


DEFAULT_POLICY_ID = "default_policy"


@DeveloperAPI
def get_optimizer_default_class(framework: str) -> Type[Optimizer]:
    if framework == "tf2":
        import tensorflow as tf

        return tf.keras.optimizers.Adam
    elif framework == "torch":
        import torch

        return torch.optim.Adam
    else:
        raise ValueError(f"Unsupported framework: {framework}")


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
            DEFAULT_POLICY_ID
        ],
    )
