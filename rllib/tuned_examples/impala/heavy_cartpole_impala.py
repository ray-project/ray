# Non-learning, throughput-only benchmark used to tune and test the usage of
# AggregatorActor in IMPALA and APPO.

# With the current setup below, 27 EnvRunners (+ 2 eval EnvRunners), 0 Learners
# 1 local A10 GPU Learner and 2 Aggregator actors, the achieved training throughput
# reaches ~7k env steps per second.
# The model has ~21M parameters (3x 2048 dense, no LSTM).

# TODO (sven): Add LSTM to this benchmark, make multi-agent, make multi-GPU.

import gymnasium as gym
import numpy as np

from ray import tune
from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args


class EnlargeObs(gym.ObservationWrapper):
    def __init__(self, env):
        super().__init__(env)
        self.observation_space = gym.spaces.Box(
            -10.0,
            10.0,
            shape=(1024,),
            dtype=np.float32,
        )

    def observation(self, observation):
        return np.concatenate(
            [observation, np.random.random(size=(1024 - observation.shape[0]))]
        )


tune.register_env("heavy-cart", lambda cfg: EnlargeObs(gym.make("CartPole-v1")))


parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2000000,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    IMPALAConfig()
    .environment("heavy-cart")
    # .env_runners(compress_observations=True)
    .learners(num_aggregator_actors_per_learner=2)
    .training(
        train_batch_size_per_learner=500,
        # train_batch_size=500,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[2048, 2048, 2048],
            # use_lstm=True,
            # lstm_cell_size=2048,
            vf_share_layers=False,
        ),
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_num_env_runners=2,
        evaluation_duration="auto",
        evaluation_parallel_to_training=True,
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
