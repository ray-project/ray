"""Contains the pytest params used in `test_agents` tests."""
import logging
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
from ray.rllib.env.wrappers.moab_wrapper import MOAB_MOVE_TO_CENTER_ENV_NAME


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
            # frameworks = [Framework.TensorFlow]
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
        version=1,
    ) -> List["TestAgentParams"]:
        return cls.for_frameworks(
            algorithm=algorithm,
            config_updates=config_updates,
            env=f"CartPole-v{version}",
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

    @classmethod
    def for_moab_move_to_center(
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
            env=MOAB_MOVE_TO_CENTER_ENV_NAME,
            frameworks=frameworks,
            n_iter=n_iter,
            threshold=threshold,
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
        # TestAgentParams.for_frameworks(
        #     algorithm=DiscreteActionSpaceAlgorithm.SAC,
        #     config_updates={
        #         "num_workers": 0,
        #         "twin_q": True,
        #         "soft_horizon": True,
        #         "clip_actions": False,
        #         "normalize_actions": True,
        #         "learning_starts": 0,
        #         "prioritized_replay": True,
        #     },
        #     env="MsPacmanNoFrameskip-v4",
        #     # TensorFlow returns Nan mean ep reward for the first few epoch
        #     # Torch is also failing in latest RLlib
        #     frameworks=[Framework.Torch],
        #     n_iter=1,
        #     threshold=1.0,
        # ),
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.PPO,
            config_updates={"num_workers": 0},
        ),
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.CQL_SAC,
            config_updates={
                # Common Configs
                "num_workers": 0,
                "input": "tests/data/moab/*.json",
                "train_batch_size": 256,  # 10
                "learning_starts": 0,
                "clip_actions": False,
                "normalize_actions": True,
                "input_evaluation": [],
                "evaluation_config": {
                    "input": "sampler",
                    "explore": False,
                },
                "evaluation_interval": 1,
                "evaluation_num_episodes": 10,
                "evaluation_num_workers": 1,
                "log_level": logging.ERROR,
                # SAC Configs
                "twin_q": True,
                "prioritized_replay": False,
                # CQL Configs
                "bc_iters": 5,
                "temperature": 1.0,
                "num_actions": 10,
                "lagrangian": True,  # False
                "lagrangian_thresh": 5.0,
                "min_q_weight": 5.0,
                "initial_alpha_prime": 1.0,
            },
            frameworks=[Framework.TensorFlow, Framework.Eager],
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
        # This is here for reference only of how to run vanilla SAC
        # in offline mode in a test. Keep in mind that SAC could hit
        # the threshold just by luck by being overconfident (extreme
        # Q-values) for some state-action pairs. A high threshold or
        # a strict evaluation criteria is required to uncover this issue.
        # With a high threshold, the reward performance will be high reward
        # with an extreme variance at the begining, after it will degrade
        # over time.
        # TestAgentParams.for_moab_move_to_center(
        #     algorithm=ContinuousActionSpaceAlgorithm.SAC,
        #     config_updates={
        #         # Common Configs
        #         "num_workers": 0,
        #         "input": "tests/data/moab/*.json",
        #         "train_batch_size": 256,  # 10
        #         "learning_starts": 0,
        #         "clip_actions": False,
        #         "normalize_actions": True,
        #         "input_evaluation": [],
        #         "evaluation_config": {
        #             "input": "sampler",
        #             "explore": False,
        #         },
        #         "evaluation_interval": 1,
        #         "evaluation_num_episodes": 10,
        #         "evaluation_num_workers": 1,
        #         "log_level": logging.ERROR,
        #         # SAC Configs
        #         "twin_q": True,
        #         "prioritized_replay": False,
        #     },
        #     n_iter=25, #3000, #500, #100, #25,  # 250,
        #     threshold=150.0,
        #     frameworks=[Framework.TensorFlow],
        # ),
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.CQL_SAC,
            config_updates={
                # Common Configs
                "num_workers": 0,
                "input": "tests/data/moab/*.json",
                "train_batch_size": 256,  # 10
                "learning_starts": 0,
                "clip_actions": False,
                "normalize_actions": True,
                "input_evaluation": [],
                "evaluation_config": {
                    "input": "sampler",
                    "explore": False,
                },
                "evaluation_interval": 1,
                "evaluation_num_episodes": 10,
                "evaluation_num_workers": 1,
                "log_level": logging.WARNING,
                # SAC Configs
                "twin_q": True,
                "prioritized_replay": False,
                # CQL Configs
                "bc_iters": 100,  # 5,
                "temperature": 1.0,
                "num_actions": 10,
                "lagrangian": True,  # False
                "lagrangian_thresh": 5.0,
                "min_q_weight": 5.0,
                "initial_alpha_prime": 1.0,
            },
            n_iter=25, #3000, #500, #100, #25,  # 250,
            threshold=150.0,
            frameworks=[Framework.TensorFlow],
        ),
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.CQL_SAC,
            config_updates={
                # Common Configs
                "num_workers": 0,
                "input": "tests/data/moab/*.json",
                "train_batch_size": 256,  # 10
                "learning_starts": 0,
                "clip_actions": False,
                "normalize_actions": True,
                "input_evaluation": [],
                "evaluation_config": {
                    "input": "sampler",
                    "explore": False,
                },
                "evaluation_interval": 1,
                "evaluation_num_episodes": 10,
                "evaluation_num_workers": 1,
                "log_level": logging.WARNING,
                # SAC Configs
                "twin_q": True,
                "prioritized_replay": False,
                # CQL Configs
                "bc_iters": 100,  # 5,
                "temperature": 1.0,
                "num_actions": 10,
                "lagrangian": True,  # False
                "lagrangian_thresh": 5.0,
                "min_q_weight": 5.0,
                "initial_alpha_prime": 1.0,
                "l2_regularizer": True,
            },
            n_iter=25, #3000, #500, #100, #25,  # 250,
            threshold=150.0,
            frameworks=[Framework.TensorFlow],
        ),
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.CQL_SAC,
            config_updates={
                # Common Configs
                "num_workers": 0,
                "input": "tests/data/moab/*.json",
                "train_batch_size": 256,  # 10
                "learning_starts": 0,
                "clip_actions": False,
                "normalize_actions": True,
                "input_evaluation": [],
                "evaluation_config": {
                    "input": "sampler",
                    "explore": False,
                },
                "evaluation_interval": 1,
                "evaluation_num_episodes": 10,
                "evaluation_num_workers": 1,
                "log_level": logging.WARNING,
                # SAC Configs
                "twin_q": True,
                "prioritized_replay": False,
                # CQL Configs
                "bc_iters": 100,  # 5,
                "temperature": 1.0,
                "num_actions": 10,
                "lagrangian": True,  # False
                "lagrangian_thresh": 5.0,
                "min_q_weight": 5.0,
                "initial_alpha_prime": 1.0,
                "Q_model": {
                    "fcnet_hiddens": [256, 256],
                    "fcnet_activation": "relu",
                    "post_fcnet_hiddens": [],
                    "post_fcnet_activation": None,
                    "custom_model": None,  # Use this to define custom Q-model(s).
                    "custom_model_config": {},
                    "dropout_rate": 0.5,
                    # VIB regularization parameters
                    "vib_regularizer": {
                        "enable_regularizer": False,
                        "decoder_layers": [256],
                        "beta": 1
                    },
                },
            },
            n_iter=25, #3000, #500, #100, #25,  # 250,
            threshold=150.0,
            frameworks=[Framework.TensorFlow],
        ),
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.CQL_SAC,
            config_updates={
                # Common Configs
                "num_workers": 0,
                "input": "tests/data/moab/*.json",
                "train_batch_size": 256,  # 10
                "learning_starts": 0,
                "clip_actions": False,
                "normalize_actions": True,
                "input_evaluation": [],
                "evaluation_config": {
                    "input": "sampler",
                    "explore": False,
                },
                "evaluation_interval": 1,
                "evaluation_num_episodes": 10,
                "evaluation_num_workers": 1,
                "log_level": logging.WARNING,
                # SAC Configs
                "twin_q": True,
                "prioritized_replay": False,
                # CQL Configs
                "bc_iters": 100,  # 5,
                "temperature": 1.0,
                "num_actions": 10,
                "lagrangian": True,  # False
                "lagrangian_thresh": 5.0,
                "min_q_weight": 5.0,
                "initial_alpha_prime": 1.0,
                "Q_model": {
                    "fcnet_hiddens": [256, 256],
                    "fcnet_activation": "relu",
                    "post_fcnet_hiddens": [],
                    "post_fcnet_activation": None,
                    "custom_model": None,  # Use this to define custom Q-model(s).
                    "custom_model_config": {},
                    "dropout_rate": 0.0,
                    # VIB regularization parameters
                    "vib_regularizer": {
                        "enable_regularizer": True,
                        "decoder_layers": [256],
                        "beta": 1
                    },
                },
            },
            n_iter=25, #3000, #500, #100, #25,  # 250,
            threshold=150.0,
            frameworks=[Framework.TensorFlow],
        ),
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.CQL_APEX_SAC,
            config_updates={
                "num_workers": 8,
                # Number of iterations with Behavior Cloning Pretraining
                "bc_iters": 0,
            },
            n_iter=100,
            threshold=140.0,
            frameworks=[Framework.TensorFlow],
        ),
        # This is here for reference only of how to run vanilla DQN
        # in offline mode in a test. Keep in mind that DQN could hit
        # the threshold just by luck by being overconfident (extreme
        # Q-values) for some state-action pairs. A high threshold or
        # a strict evaluation criteria is required to uncover this issue.
        # With a high threshold, the reward performance will be high reward
        # with an extreme variance at the begining, after it will degrade
        # over time.
        # TestAgentParams.for_cart_pole(
        #     algorithm=DiscreteActionSpaceAlgorithm.DQN,
        #     config_updates={
        #         # Common Configs
        #         "num_workers": 0,
        #         "input": "tests/data/cartpole/output-*.json",
        #         "input_evaluation": [],
        #         "evaluation_config": {
        #             "input": "sampler",
        #             "explore": False,
        #         },
        #         "evaluation_interval": 1,
        #         "evaluation_num_episodes": 10,
        #         "evaluation_num_workers": 1,
        #         "log_level": logging.ERROR,
        #     },
        #     n_iter=500, #25, #3000, #500, #100, #25,  # 250,
        #     threshold=150.0,
        #     frameworks=[Framework.TensorFlow],
        #     version=0,
        # ),
        TestAgentParams.for_cart_pole(
            algorithm=DiscreteActionSpaceAlgorithm.CQL_DQN,
            config_updates={
                # Common Configs
                "num_workers": 0,
                "input": "tests/data/cartpole/output-*.json",
                "input_evaluation": [],
                "evaluation_config": {
                    "input": "sampler",
                    "explore": False,
                },
                "evaluation_interval": 1,
                "evaluation_num_episodes": 10,
                "evaluation_num_workers": 1,
                "log_level": logging.WARNING,
                # CQL Configs
                "min_q_weight": 1.0,
            },
            n_iter=500, #25,  # 3000, #500, #100, #25,  # 250,
            threshold=150.0,
            frameworks=[Framework.TensorFlow],
            version=0,
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
        TestAgentParams.for_moab_move_to_center(
            algorithm=ContinuousActionSpaceAlgorithm.PPO,
            config_updates={
                "num_gpus": 2,
                "_fake_gpus": True,
                "num_workers": 3,
                "num_envs_per_worker": 5,
                # Advance configs
                "batch_mode": "complete_episodes",  # default truncate_episodes
                "use_gae": True,  # default True
                "use_critic": True,  # default True
                "shuffle_sequences": True,  # default True
                "entropy_coeff": 0.0,  # default 0.0 - range 0 to 0.01
                "vf_loss_coeff": 1.0,  # default 1.0 - range 0.5, 1
                "kl_coeff": 0.2,  # default 0.2 - range 0.3 to 1
                "kl_target": 0.01,  # default 0.01 - range 0.003 to 0.03
                "clip_param": 0.3,  # default 0.3 - range 0.1, 0.2, 0.3
                # default 10.0 - range sensitive to scale of the rewards
                "vf_clip_param": 100.0,
                "gamma": 0.99,  # default 0.99 - range 0.8 to 0.9997
                "lambda": 1.0,  # default 1.0 - range 0.9 to 1
                # Size of batches collected from each worker
                "rollout_fragment_length": 100,
                "sgd_minibatch_size": 128,  # default 128
                # Num of SGD passes per train batch
                "num_sgd_iter": 15,  # default 30
                # Number of timesteps collected for each SGD round
                "train_batch_size": 6000,
                "log_level": logging.INFO,
            },
            n_iter=250,
            threshold=240.0,
            # frameworks=[Framework.TensorFlow],
        ),
    )
]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
