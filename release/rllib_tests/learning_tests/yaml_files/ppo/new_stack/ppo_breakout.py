import gymnasium as gym

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray import tune


def _make_env_to_module_connector(env):
    return FrameStackingEnvToModule(num_frames=4)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


# Create a custom Atari setup (w/o the usual RLlib-hard-coded framestacking in it).
# We would like our frame stacking connector to do this job.
def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make("ALE/Breakout-v5", **cfg, **{"render_mode": "rgb_array"}),
        # Perform through ConnectorV2 API.
        framestack=None,
    )


tune.register_env("env", _env_creator)

num_gpus = 8

stop = {
    "sampler_results/episode_reward_mean": 100.0,
    "timesteps_total": 10000000,
    "time_total_s": 3600,
}

config = (
    PPOConfig()
    .experimental(_enable_new_api_stack=True)
    .environment(
        "env",
        env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .rollouts(
        env_to_module_connector=_make_env_to_module_connector,
        num_rollout_workers=95,
        env_runner_cls=SingleAgentEnvRunner,
    )
    .resources(
        num_gpus=0,
        num_learner_workers=num_gpus,
        num_gpus_per_learner_worker=1,
        num_cpus_for_local_worker=1,
    )
    .training(
        learner_connector=_make_learner_connector,
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.1,
        vf_clip_param=10.0,
        entropy_coeff=0.01,
        num_sgd_iter=10,
        lr=0.0001 * num_gpus,
        grad_clip=100.0,
        grad_clip_by="global_norm",
        model={
            "vf_share_layers": True,
            "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            "conv_activation": "relu",
            "post_fcnet_hiddens": [256],
            "uses_new_env_runners": True,
        },
    )
)
