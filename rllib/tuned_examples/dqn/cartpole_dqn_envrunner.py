from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)

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
            "epsilon": [(0, 1.0), (10000, 0.02)],
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
        },
    )
    .training(
        # Settings identical to old stack.
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
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
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 450.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 100000,
}
