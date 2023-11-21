from typing import Optional, Type, Union, TYPE_CHECKING

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.learner.learner_group import LearnerGroup
#from ray.rllib.core.learner.learner import (
#    FrameworkHyperparameters,
#    #LearnerSpec,
#)
#from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
#from ray.rllib.core.testing.testing_learner import BaseTestingLearnerHyperparameters
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModuleSpec,
    MultiAgentRLModule,
)
from ray.rllib.utils.annotations import DeveloperAPI

if TYPE_CHECKING:
    import gymnasium as gym
    import torch
    import tensorflow as tf

    from ray.rllib.core.learner.learner import Learner
    from ray.rllib.core.rl_module import RLModule


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
def get_learner(
    *,
    framework: str,
    framework_hps: Optional[FrameworkHyperparameters] = None,
    env: "gym.Env",
    learner_hps: Optional[BaseTestingLearnerHyperparameters] = None,
    is_multi_agent: bool = False,
) -> "Learner":
    """Construct a learner for testing.

    Args:
        framework: The framework used for training.
        framework_hps: The FrameworkHyperparameters instance to pass to the
            Learner's constructor.
        env: The environment to train on.
        learner_hps: The LearnerHyperparameter instance to pass to the Learner's
            constructor.
        is_multi_agent: Whether to construct a multi agent rl module.

    Returns:
        A learner.

    """
    # Get our testing (BC) Learner class (given the framework).
    _cls = get_learner_class(framework)
    # Get our RLModule spec to use.
    spec = get_module_spec(framework=framework, env=env, is_multi_agent=is_multi_agent)
    # Adding learning rate as a configurable parameter to avoid hardcoding it
    # and information leakage across tests that rely on knowing the LR value
    # that is used in the learner.
    learner = _cls(
        module_spec=spec,
        learner_hyperparameters=learner_hps or BaseTestingLearnerHyperparameters(),
        framework_hyperparameters=framework_hps or FrameworkHyperparameters(),
    )
    learner.build()
    return learner


@DeveloperAPI
def add_module_to_learner_or_learner_group(
    framework: str,
    env: "gym.Env",
    module_id: str,
    learner_group_or_learner: Union[LearnerGroup, "Learner"],
):
    learner_group_or_learner.add_module(
        module_id=module_id,
        module_spec=get_module_spec(framework, env, is_multi_agent=False),
    )
