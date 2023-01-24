from typing import Type, Union, TYPE_CHECKING
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec


from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.core.rl_trainer.trainer_runner import TrainerRunner

from rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from rllib.core.rl_module.tests.test_marl_module import DEFAULT_POLICY_ID

if TYPE_CHECKING:
    import gymnasium as gym
    import torch
    import tensorflow as tf

    from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
    from ray.rllib.core.rl_module import RLModule


Optimizer = Union["tf.keras.optimizers.Optimizer", "torch.optim.Optimizer"]


@DeveloperAPI
def get_trainer_class(framework: str) -> Type["RLTrainer"]:
    if framework == "tf":
        from ray.rllib.core.testing.tf.bc_rl_trainer import BCTfRLTrainer

        return BCTfRLTrainer
    elif framework == "torch":
        from ray.rllib.core.testing.torch.bc_rl_trainer import BCTorchRLTrainer

        return BCTorchRLTrainer
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
        model_config={"hidden_dim": 32},
    )

    if is_multi_agent:
        # TODO (Kourosh): Make this more multi-agent for example with policy ids "1", and "2".
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
def get_rl_trainer(
    framework: str,
    env: "gym.Env",
    is_multi_agent: bool = False,
) -> "RLTrainer":

    _cls = get_trainer_class(framework)
    spec = get_module_spec(framework=framework, env=env, is_multi_agent=is_multi_agent)
    return _cls(module_spec=spec, optimizer_config={"lr": 0.1})


@DeveloperAPI
def get_trainer_runner(
    framework: str,
    env: "gym.Env",
    compute_config: dict,
    is_multi_agent: bool = False,
) -> TrainerRunner:
    trainer_class = get_trainer_class(framework)
    trainer_cfg = dict(
        module_spec=get_module_spec(
            framework=framework, env=env, is_multi_agent=is_multi_agent
        ),
        optimizer_config={"lr": 0.1},
    )
    runner = TrainerRunner(trainer_class, trainer_cfg, compute_config=compute_config)

    return runner


@DeveloperAPI
def add_module_to_runner_or_trainer(
    framework: str,
    env: "gym.Env",
    module_id: str,
    runner_or_trainer: Union[TrainerRunner, "RLTrainer"],
):
    runner_or_trainer.add_module(
        module_id=module_id,
        module_spec=get_module_spec(framework, env, is_multi_agent=False),
        optimizer_cls=get_optimizer_default_class(framework),
    )
