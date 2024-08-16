from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)

parser = add_rllib_example_script_args()
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .rl_module(
        # Settings identical to old stack.
        model_config_dict={
            "fcnet_hiddens": [256],
            "fcnet_activation": "tanh",
            "epsilon": [(0, 1.0), (10000, 0.02)],
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
        },
    )
    .training(
        # Settings identical to old stack.
        train_batch_size_per_learner=32,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        n_step=3,
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
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 500.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 100000,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
