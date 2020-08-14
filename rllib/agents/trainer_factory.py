"""I should be able to construct a trainer given the name of the trainer and the
values of the config that I wish to be different from the default config for that
algorithm."""
import enum
from typing import List, Optional, Type

from ray.rllib.agents import ddpg, dqn, ppo, sac
from ray.rllib.agents.ddpg import apex, td3
from ray.rllib.agents.dqn import apex
from ray.rllib.agents.ppo import ddppo, appo
from ray.rllib.agents.sac import apex
from ray.rllib.agents.trainer import Trainer


class Framework(enum.Enum):
    TensorFlow = enum.auto()
    Eager = enum.auto()
    Torch = enum.auto()

    @classmethod
    def all(cls) -> List["Framework"]:
        return [x for x in cls]


class Algorithm(enum.Enum):
    def get_trainer_cls(self) -> Type[Trainer]:
        return self.value[0]

    def get_default_config(self) -> dict:
        return self.value[1]


class DiscreteActionSpaceAlgorithm(Algorithm):
    APEX_DQN = (dqn.ApexTrainer, dqn.apex.APEX_DEFAULT_CONFIG)
    APEX_SAC = (sac.ApexSACTrainer, sac.apex.APEX_SAC_DEFAULT_CONFIG)
    APPO = (ppo.APPOTrainer, ppo.appo.DEFAULT_CONFIG)
    SIMPLE_Q = (dqn.SimpleQTrainer, dqn.SIMPLE_Q_DEFAULT_CONFIG)
    DDPPO = (ppo.ddppo.DDPPOTrainer, ppo.ddppo.DEFAULT_CONFIG)
    DQN = (dqn.DQNTrainer, dqn.DEFAULT_CONFIG)
    PPO = (ppo.PPOTrainer, ppo.DEFAULT_CONFIG)
    SAC = (sac.SACTrainer, sac.DEFAULT_CONFIG)


class ContinuousActionSpaceAlgorithm(Algorithm):
    APEX_DDPG = (
        ddpg.ApexDDPGTrainer,
        ddpg.apex.APEX_DDPG_DEFAULT_CONFIG,
    )
    APEX_SAC = (sac.ApexSACTrainer, sac.apex.APEX_SAC_DEFAULT_CONFIG)
    APPO = (ppo.APPOTrainer, ppo.appo.DEFAULT_CONFIG)
    DDPG = (ddpg.DDPGTrainer, ddpg.DEFAULT_CONFIG)
    DDPPO = (ppo.ddppo.DDPPOTrainer, ppo.ddppo.DEFAULT_CONFIG)
    TD3 = (ddpg.TD3Trainer, ddpg.td3.TD3_DEFAULT_CONFIG)
    PPO = (ppo.PPOTrainer, ppo.DEFAULT_CONFIG)
    SAC = (sac.SACTrainer, sac.DEFAULT_CONFIG)


def trainer_factory(
    algorithm: Algorithm, config_overrides: dict, env: Optional[str] = None
) -> Trainer:
    """Constructs a trainer given the algorithm type and the updated parts of config.

    Args:
        algorithm (Algorithm):
        config_overrides (dict): keys and values that are different from the default
            config of the algorithm.
        env (str): name of environment used to train the agent.
            must be registered with RLLib.

    Returns:
        Trainer

    """
    trainer_cls = algorithm.get_trainer_cls()
    default_config = algorithm.get_default_config()
    return trainer_cls(config={**default_config, **config_overrides}, env=env)
