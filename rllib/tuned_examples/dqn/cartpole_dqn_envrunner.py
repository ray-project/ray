from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .framework(framework="torch")
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=0,
    )
    .resources(
        num_learner_workers=0,
    )
    .rl_module(
        # Settings identical to old stack.
        model_config_dict={
            "fcnet_hiddens": [256],
            "fcnet_activation": "relu",
            "epsilon": [(0, 1.0), (10000, 0.05)],
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
        },
    )
    .training(
        # Settings identical to old stack.
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        double_q=True,
        num_atoms=1,
        noisy=False,
        dueling=True,
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
        evaluation_num_workers=1,
        evaluation_duration="auto",
        evaluation_config={
            "explore": False,
        },
    )
)

stop = {
    "evaluation/sampler_results/episode_reward_mean": 500.0,
    "timesteps_total": 100000,
}
