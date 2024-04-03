from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

config = (
    SACConfig()
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        rollout_fragment_length=1,
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=0,
    )
    .environment(env="Pendulum-v1")
    .training(
        initial_alpha=1.001,
        lr=3e-4,
        target_entropy="auto",
        n_step=1,
        tau=0.005,
        train_batch_size=256,
        target_network_update_freq=1,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
        },
        num_steps_sampled_before_learning_starts=256,
        model={
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "relu",
            "post_fcnet_hiddens": [],
            "post_fcnet_activation": None,
            "post_fcnet_weights_initializer": "orthogonal_",
            "post_fcnet_weights_initializer_config": {"gain": 0.01},
        },
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
)

stop = {
    "sampler_results/episode_reward_mean": -250,
    "timesteps_total": 20000,
}


print(config.build().train())
