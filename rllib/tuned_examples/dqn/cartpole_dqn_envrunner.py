from ray.rllib.algorithms.dqn import DQNConfig

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .framework(framework="torch")
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .rl_module(
        # Settings identical to old stack.
        model_config_dict={
            "fcnet_hiddens": [256],
            "fcnet_activation": "relu",
            "epsilon": [(0, 1.0), (50000, 0.05)],
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
        evaluation_num_env_runners=1,
        evaluation_duration="auto",
        evaluation_config={"explore": False},
    )
)

stop = {
    "evaluation_results/env_runner_results/episode_return_mean": 450.0,
    "num_env_steps_sampled_lifetime": 100000,
}
