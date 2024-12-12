from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray import tune

import gymnasium as gym
import numpy as np


class EnlargeObs(gym.ObservationWrapper):
    def __init__(self, env):
        super().__init__(env)
        os = env.observation_space
        self.observation_space = gym.spaces.Box(
            -10.0, 10.0, shape=(1024,), dtype=np.float32,
        )

    def observation(self, observation):
        return np.concatenate([
            observation, np.random.random(size=(1024 - observation.shape[0]))
        ])


tune.register_env("huge-cart", lambda cfg: EnlargeObs(gym.make("CartPole-v1")))


parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2000000,
)
#parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    IMPALAConfig()
    .environment("huge-cart")
    .api_stack(
        enable_env_runner_and_connector_v2=True,
        enable_rl_module_and_learner=True,
    )
    .env_runners(compress_observations=True)
    .training(
        train_batch_size_per_learner=500,
        train_batch_size=500,
        #grad_clip=40.0,
        #grad_clip_by="global_norm",
        #lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        #vf_loss_coeff=0.05,
        #entropy_coeff=0.0,
        #learner_config_dict={
        #    "_training_step_sample_only": True,
        #},
        model={
            "fcnet_hiddens": [2048, 2048, 2048],
            "use_lstm": True,
            "lstm_cell_size": 2048,
            "vf_share_layers": False,
        },
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[2048, 2048, 2048],
            use_lstm=True,
            lstm_cell_size=2048,
            vf_share_layers=False,
        ),
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_num_env_runners=2,
        evaluation_config=IMPALAConfig.overrides(num_envs_per_env_runner=1),
        evaluation_duration="auto",
        evaluation_parallel_to_training=True,
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
