"""Contains the pytest params used in `test_agents` tests."""
from itertools import chain
from typing import List, Optional, Tuple

import attr
import pytest

from ray.rllib.agents.trainer_factory import (
    Algorithm,
    DiscreteActionSpaceAlgorithm,
    ContinuousActionSpaceAlgorithm,
    Framework,
)


@attr.s(auto_attribs=True)
class TestAgentParams:
    algorithm: Algorithm
    config_updates: dict
    env: str
    framework: Framework
    n_iter: int
    threshold: float

    @classmethod
    def for_frameworks(
        cls,
        algorithm: Algorithm,
        config_updates: dict,
        env: str,
        frameworks: Optional[List[Framework]] = None,
        n_iter=2,
        threshold=1.0,
    ) -> List["TestAgentParams"]:
        if frameworks is None:
            frameworks = Framework.all()
        return [
            cls(
                algorithm=algorithm,
                config_updates=config_updates,
                env=env,
                framework=framework,
                n_iter=n_iter,
                threshold=threshold,
            )
            for framework in frameworks
        ]

    @classmethod
    def for_cart_pole(
        cls,
        algorithm: Algorithm,
        config_updates: dict,
        frameworks: Optional[List[Framework]] = None,
        n_iter=2,
        threshold=1.0,
    ) -> List["TestAgentParams"]:
        return cls.for_frameworks(
            algorithm=algorithm,
            config_updates=config_updates,
            env="CartPole-v1",
            frameworks=frameworks,
            n_iter=n_iter,
            threshold=threshold,
        )

    @classmethod
    def for_pendulum(
        cls,
        algorithm: Algorithm,
        config_updates: dict,
        frameworks: Optional[List[Framework]] = None,
        n_iter=2,
        threshold=-3000.0,
    ) -> List["TestAgentParams"]:
        return cls.for_frameworks(
            algorithm=algorithm,
            config_updates=config_updates,
            env="Pendulum-v0",
            frameworks=frameworks,
            n_iter=n_iter,
            threshold=threshold,
        )

    @classmethod
    def for_cart_pole_and_pendulum(cls, *args, **kwargs) -> List["TestAgentParams"]:
        return list(
            chain(cls.for_cart_pole(*args, **kwargs), cls.for_pendulum(*args, **kwargs))
        )

    def astuple(self):
        return (
            self.algorithm,
            self.config_updates,
            self.env,
            self.framework,
            self.n_iter,
            self.threshold,
        )


test_compilation_params: List[Tuple[Algorithm, dict, str, Framework, int, float]] = [
    x.astuple()
    for x in chain(
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.APEX_DQN,
            config_updates={
                "num_workers": 3,
                "prioritized_replay": True,
                "timesteps_per_iteration": 100,
                "min_iter_time_s": 1,
                "optimizer": {"num_replay_buffer_shards": 1},
            },
        ),
        TestAgentParams.for_cart_pole_and_pendulum(
            algorithm=ContinuousActionSpaceAlgorithm.APEX_SAC,
            config_updates={
                "num_workers": 4,
                "prioritized_replay": True,
                "timesteps_per_iteration": 100,
                "min_iter_time_s": 1,
                "optimizer": {"num_replay_buffer_shards": 1},
            },
        ),
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.DQN,
            config_updates={"num_workers": 0},
        ),
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.SIMPLE_Q,
            config_updates={"num_workers": 0},
        ),
        TestAgentParams.for_pendulum(
            algorithm=ContinuousActionSpaceAlgorithm.APEX_DDPG,
            config_updates={"num_workers": 4,},
        ),
        TestAgentParams.for_pendulum(
            algorithm=ContinuousActionSpaceAlgorithm.DDPG,
            config_updates={"num_workers": 0},
            frameworks=[Framework.TensorFlow, Framework.Torch],
        ),
        TestAgentParams.for_pendulum(
            algorithm=ContinuousActionSpaceAlgorithm.TD3,
            config_updates={"num_workers": 0},
            frameworks=[Framework.TensorFlow],
        ),
        TestAgentParams.for_cart_pole_and_pendulum(
            algorithm=DiscreteActionSpaceAlgorithm.PPO,
            config_updates={"num_workers": 0},
        ),
        TestAgentParams.for_cart_pole_and_pendulum(
            algorithm=DiscreteActionSpaceAlgorithm.DDPPO,
            config_updates={"num_gpus_per_worker": 0},
            frameworks=[Framework.Torch],
        ),
        TestAgentParams.for_cart_pole_and_pendulum(
            algorithm=DiscreteActionSpaceAlgorithm.APPO,
            config_updates={"num_workers": 1},
            frameworks=[Framework.TensorFlow, Framework.Torch],
        ),
        TestAgentParams.for_cart_pole_and_pendulum(
            algorithm=DiscreteActionSpaceAlgorithm.APPO,
            config_updates={"num_workers": 1, "vtrace": True},
            frameworks=[Framework.TensorFlow, Framework.Torch],
        ),
        TestAgentParams.for_cart_pole_and_pendulum(
            algorithm=DiscreteActionSpaceAlgorithm.SAC,
            config_updates={
                "num_workers": 0,
                "twin_q": True,
                "soft_horizon": True,
                "clip_actions": False,
                "normalize_actions": True,
                "learning_starts": 0,
                "prioritized_replay": True,
            },
        ),
        TestAgentParams.for_frameworks(
            algorithm=DiscreteActionSpaceAlgorithm.SAC,
            config_updates={
                "num_workers": 0,
                "twin_q": True,
                "soft_horizon": True,
                "clip_actions": False,
                "normalize_actions": True,
                "learning_starts": 0,
                "prioritized_replay": True,
            },
            env="MsPacmanNoFrameskip-v4",
            # TensorFlow returns Nan mean ep reward for the first few epoch
            frameworks=[Framework.Torch],
            n_iter=1,
            threshold=1.0,
        ),
    )
]

test_convergence_params: List[Tuple[Algorithm, dict, str, Framework, int, float]] = [
    x.astuple()
    for x in chain(
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.DQN,
            config_updates={"num_workers": 0},
            n_iter=20,
            threshold=100.0,
        ),
    )
]

test_monotonic_convergence_params: List[
    Tuple[Algorithm, dict, str, Framework, int, float]
] = [
    x.astuple()
    for x in chain(
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.PPO,
            config_updates={
                "num_gpus": 2,
                "_fake_gpus": True,
                "num_workers": 1,
                "lr": 0.0003,
                "observation_filter": "MeanStdFilter",
                "num_sgd_iter": 6,
                "vf_share_layers": True,
                "vf_loss_coeff": 0.01,
                "model": {"fcnet_hiddens": [32], "fcnet_activation": "linear"},
            },
            n_iter=200,
            threshold=150.0,
        ),
        TestAgentParams.for_pendulum(
            algorithm=ContinuousActionSpaceAlgorithm.APEX_DDPG,
            config_updates={
                "use_huber": True,
                "clip_rewards": False,
                "num_workers": 4,
                "n_step": 1,
                "target_network_update_freq": 50000,
                "tau": 1.0,
            },
            n_iter=200,
            threshold=-750.0,
        ),
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.APEX_DQN,
            config_updates={
                "target_network_update_freq": 20000,
                "num_workers": 4,
                "num_envs_per_worker": 8,
                "train_batch_size": 64,
                "gamma": 0.95,
            },
            n_iter=200,
            threshold=150.0,
        ),
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.SAC,
            config_updates={
                "num_workers": 4,
                "twin_q": True,
                "soft_horizon": True,
                "clip_actions": False,
                "normalize_actions": True,
                "learning_starts": 0,
                "prioritized_replay": True,
                "Q_model": {"fcnet_hiddens": [64, 64]},
                "policy_model": {"fcnet_hiddens": [64, 64],},
            },
            n_iter=200,
            threshold=100.0,
        ),
        TestAgentParams.for_cart_pole(
            algorithm=ContinuousActionSpaceAlgorithm.APEX_SAC,
            config_updates={
                "seed": 42,
                "num_workers": 8,
            },
            n_iter=100,
            threshold=175.0,
        ),
        TestAgentParams.for_pendulum(
            algorithm=ContinuousActionSpaceAlgorithm.APEX_SAC,
            config_updates={
                "num_workers": 8,
                "exploration_config": {"type": "StochasticSampling"},
                "prioritized_replay": False,
                "no_done_at_end": False,
            },
            n_iter=200,
            threshold=-350.,
        ),
        TestAgentParams.for_pendulum(
            algorithm=ContinuousActionSpaceAlgorithm.APEX_SAC,
            config_updates={
                "num_workers": 8,
                "exploration_config": {"type": "StochasticSampling"},
                "prioritized_replay": True,
                "no_done_at_end": True
            },
            n_iter=200,
            threshold=-350.,
        ),
        TestAgentParams.for_pendulum(
            algorithm=DiscreteActionSpaceAlgorithm.SAC,
            config_updates={
                "horizon": 200,
                "soft_horizon": True,
                "Q_model": {"fcnet_activation": "relu", "fcnet_hiddens": [256, 256]},
                "policy_model": {
                    "fcnet_activation": "relu",
                    "fcnet_hiddens": [256, 256],
                },
                "tau": 0.005,
                "target_entropy": "auto",
                "no_done_at_end": True,
                "n_step": 1,
                "rollout_fragment_length": 1,
                "prioritized_replay": True,
                "train_batch_size": 256,
                "target_network_update_freq": 1,
                "timesteps_per_iteration": 1000,
                "learning_starts": 256,
                "optimization": {
                    "actor_learning_rate": 0.0003,
                    "critic_learning_rate": 0.0003,
                    "entropy_learning_rate": 0.0003,
                },
                "num_workers": 4,
                "num_gpus": 0,
                "clip_actions": False,
                "normalize_actions": True,
                "metrics_smoothing_episodes": 5,
            },
            n_iter=200,
            threshold=-750.0,
        ),
    )
]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
