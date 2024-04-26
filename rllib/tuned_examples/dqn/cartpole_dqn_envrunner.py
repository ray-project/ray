from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .framework(framework="torch")
    .experimental(_enable_new_api_stack=True)
    .env_runners(
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
        evaluation_num_env_runners=1,
        evaluation_duration="auto",
        evaluation_config={
            "explore": False,
            # TODO (sven): Add support for window=float(inf) and reduce=mean for
            #  evaluation episode_return_mean reductions (identical to old stack
            #  behavior, which does NOT use a window (100 by default) to reduce
            #  eval episode returns.
            "metrics_num_episodes_for_smoothing": 4,
        },
    )
)

stop = {
    "evaluation_results/env_runner_results/episode_return_mean": 500.0,
    "num_env_steps_sampled_lifetime": 100000,
}
