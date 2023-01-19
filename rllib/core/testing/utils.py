from typing import Type, Union, TYPE_CHECKING

import torch
import tensorflow as tf

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.core.rl_trainer.trainer_runner import TrainerRunner

if TYPE_CHECKING:
    import gymnasium as gym

    from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
    from ray.rllib.core.rl_module import RLModule


Optimizer = Union[tf.keras.optimizers.Optimizer, torch.optim.Optimizer]


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
def get_optimizer_default_class(framework: str) -> Type[Optimizer]:
    if framework == "tf":
        return tf.keras.optimizers.Adam
    elif framework == "torch":
        return torch.optim.Adam
    else:
        raise ValueError(f"Unsupported framework: {framework}")


@DeveloperAPI
def get_trainer_runner(
    framework: str, env: "gym.Env", compute_config: dict
) -> TrainerRunner:
    trainer_class = get_trainer_class(framework)
    trainer_cfg = dict(
        module_class=get_module_class(framework),
        module_kwargs={
            "observation_space": env.observation_space,
            "action_space": env.action_space,
            "model_config": {"hidden_dim": 32},
        },
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
        module_cls=get_module_class(framework),
        module_kwargs={
            "observation_space": env.observation_space,
            "action_space": env.action_space,
            "model_config": {"hidden_dim": 32},
        },
        optimizer_cls=get_optimizer_default_class(framework),
    )
