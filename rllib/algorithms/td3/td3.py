"""A more stable successor to TD3.

By default, this uses a near-identical configuration to that reported in the
TD3 paper.
"""
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ddpg.ddpg import DDPG, DDPGConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.deprecation import DEPRECATED_VALUE


class TD3Config(DDPGConfig):
    """Defines a configuration class from which a TD3 Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.td3 import TD3Config
        >>> config = TD3Config().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run one training iteration.
        >>> algo = config.build(env="Pendulum-v1")  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.td3 import TD3Config
        >>> from ray import air
        >>> from ray import tune
        >>> config = TD3Config()
        >>> # Print out some default values.
        >>> print(config.lr)   # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training(lr=tune.grid_search(  # doctest: +SKIP
        ...     [0.001, 0.0001]))  # doctest: +SKIP
        >>> # Set the config object's env.
        >>> config.environment(env="Pendulum-v1")  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "TD3",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a TD3Config instance."""
        super().__init__(algo_class=algo_class or TD3)

        # fmt: off
        # __sphinx_doc_begin__

        # Override some of DDPG/SimpleQ/Algorithm's default values with TD3-specific
        # values.

        # .training()

        # largest changes: twin Q functions, delayed policy updates, target
        # smoothing, no l2-regularization.
        self.twin_q = True
        self.policy_delay = 2
        self.smooth_target_policy = True,
        self.l2_reg = 0.0
        # Different tau (affecting target network update).
        self.tau = 5e-3
        # Different batch size.
        self.train_batch_size = 100
        # No prioritized replay by default (we may want to change this at some
        # point).
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            "capacity": 1000000,
            "worker_side_prioritization": False,
        }
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config["multiagent"]["count_steps_by"].
        self.num_steps_sampled_before_learning_starts = 10000

        # .exploration()
        # TD3 uses Gaussian Noise by default.
        self.exploration_config = {
            # TD3 uses simple Gaussian noise on top of deterministic NN-output
            # actions (after a possible pure random phase of n timesteps).
            "type": "GaussianNoise",
            # For how many timesteps should we return completely random
            # actions, before we start adding (scaled) noise?
            "random_timesteps": 10000,
            # Gaussian stddev of action noise for exploration.
            "stddev": 0.1,
            # Scaling settings by which the Gaussian noise is scaled before
            # being added to the actions. NOTE: The scale timesteps start only
            # after(!) any random steps have been finished.
            # By default, do not anneal over time (fixed 1.0).
            "initial_scale": 1.0,
            "final_scale": 1.0,
            "scale_timesteps": 1,
        }
        # __sphinx_doc_end__
        # fmt: on


class TD3(DDPG):
    @classmethod
    @override(DDPG)
    def get_default_config(cls) -> AlgorithmConfig:
        return TD3Config()


# Deprecated: Use ray.rllib.algorithms.ddpg..td3.TD3Config instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(TD3Config().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.ddpg.td3::TD3_DEFAULT_CONFIG",
        new="ray.rllib.algorithms.td3.td3::TD3Config(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


TD3_DEFAULT_CONFIG = _deprecated_default_config()
