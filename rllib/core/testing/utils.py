from typing import Type, Union, TYPE_CHECKING

from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.core.learner.learner_group import LearnerGroup
from ray.rllib.core.learner.learner import LearnerSpec, FrameworkHyperparameters
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig

from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModuleSpec,
    MultiAgentRLModule,
)


if TYPE_CHECKING:
    import gymnasium as gym
    import torch
    import tensorflow as tf

    from ray.rllib.core.learner.learner import Learner
    from ray.rllib.core.rl_module import RLModule


Optimizer = Union["tf.keras.optimizers.Optimizer", "torch.optim.Optimizer"]


DEFAULT_POLICY_ID = "default_policy"


@DeveloperAPI
def get_learner_class(framework: str) -> Type["Learner"]:
    if framework == "tf":
        from ray.rllib.core.testing.tf.bc_learner import BCTfLearner

        return BCTfLearner
    elif framework == "torch":
        from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner

        return BCTorchLearner
    else:
        raise ValueError(f"Unsupported framework: {framework}")


@DeveloperAPI
def get_module_class(framework: str) -> Type["RLModule"]:
    if framework == "tf":
        from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule

        return DiscreteBCTFModule
    elif framework == "torch":
        from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

        return DiscreteBCTorchModule
    else:
        raise ValueError(f"Unsupported framework: {framework}")


@DeveloperAPI
def get_module_spec(framework: str, env: "gym.Env", is_multi_agent: bool = False):

    spec = SingleAgentRLModuleSpec(
        module_class=get_module_class(framework),
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config_dict={"fcnet_hiddens": [32]},
    )

    if is_multi_agent:
        # TODO (Kourosh): Make this more multi-agent for example with policy ids "1",
        # and "2".
        return MultiAgentRLModuleSpec(
            module_class=MultiAgentRLModule, module_specs={DEFAULT_POLICY_ID: spec}
        )
    else:
        return spec


@DeveloperAPI
def get_optimizer_default_class(framework: str) -> Type[Optimizer]:
    if framework == "tf":
        import tensorflow as tf

        return tf.keras.optimizers.Adam
    elif framework == "torch":
        import torch

        return torch.optim.Adam
    else:
        raise ValueError(f"Unsupported framework: {framework}")


@DeveloperAPI
def get_learner(
    framework: str,
    env: "gym.Env",
    learning_rate: float = 1e-3,
    is_multi_agent: bool = False,
) -> "Learner":
    """Construct a learner for testing.

    Args:
        framework: The framework used for training.
        env: The environment to train on.
        learning_rate: The learning rate to use for each learner.
        is_multi_agent: Whether to construct a multi agent rl module.

    Returns:
        A learner.

    """

    _cls = get_learner_class(framework)
    spec = get_module_spec(framework=framework, env=env, is_multi_agent=is_multi_agent)
    # adding learning rate as a configurable parameter to avoid hardcoding it
    # and information leakage across tests that rely on knowing the LR value
    # that is used in the learner.
    return _cls(module_spec=spec, optimizer_config={"lr": learning_rate})


@DeveloperAPI
def get_learner_group(
    framework: str,
    env: "gym.Env",
    scaling_config: LearnerGroupScalingConfig,
    learning_rate: float = 1e-3,
    is_multi_agent: bool = False,
    eager_tracing: bool = False,
) -> LearnerGroup:
    """Construct a learner_group for testing.

    Args:
        framework: The framework used for training.
        env: The environment to train on.
        scaling_config: A config for the amount and types of resources to use for
            training.
        learning_rate: The learning rate to use for each learner.
        is_multi_agent: Whether to construct a multi agent rl module.
        eager_tracing: TF Specific. Whether to use tf.function for tracing
            optimizations.

    Returns:
        A learner_group.

    """
    if framework == "tf":
        framework_hps = FrameworkHyperparameters(eager_tracing=eager_tracing)
    else:
        framework_hps = None
    learner_spec = LearnerSpec(
        learner_class=get_learner_class(framework),
        module_spec=get_module_spec(
            framework=framework, env=env, is_multi_agent=is_multi_agent
        ),
        optimizer_config={"lr": learning_rate},
        learner_group_scaling_config=scaling_config,
        framework_hyperparameters=framework_hps,
    )
    lg = LearnerGroup(learner_spec)

    return lg


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
