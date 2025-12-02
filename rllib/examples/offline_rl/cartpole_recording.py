"""Example showing how to record expert data from a trained policy.

This example:
    - demonstrates how you can train a single-agent expert PPO Policy (RLModule)
    and checkpoint it.
    - shows how you can then record expert data from the trained PPO Policy to
    disk during evaluation.

How to run this script
----------------------
`python [script file name].py --checkpoint-at-end`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
In the console output you can see that the episode return of 350.0 is reached
before the timestep stop criteria is touched. Afterwards evaluation starts and
runs 10 iterations while recording the data. The number of recorded experiences
might differ from evaluation run to evaluation run because evaluation
`EnvRunner`s sample episodes while recording timesteps and episodes contain
usually different numbers of timesteps. Note, this is different when recording
episodes - in this case each row is one episode.

+-----------------------------+------------+----------------------+
| Trial name                  | status     | loc                  |
|                             |            |                      |
|-----------------------------+------------+----------------------+
| PPO_CartPole-v1_df83f_00000 | TERMINATED | 192.168.0.119:233661 |
+-----------------------------+------------+----------------------+
+--------+------------------+------------------------+------------------------+
|   iter |   total time (s) |   num_training_step_ca |   num_env_steps_sample |
|        |                  |      lls_per_iteration |             d_lifetime |
+--------+------------------+------------------------+------------------------|
|     21 |          25.9162 |                      1 |                  84000 |
+--------+------------------+------------------------+------------------------+

...

Number of experiences recorded: 26644
"""

import ray

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_timesteps=200000,
    default_reward=350.0,
)
parser.set_defaults(
    checkpoint_at_end=True,
    max_concurrent_trials=1,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .env_runners(
        num_env_runners=5,
    )
    .environment("CartPole-v1")
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[32],
            fcnet_activation="linear",
            vf_share_layers=True,
        ),
    )
    .training(
        lr=0.0003,
        num_epochs=6,
        vf_loss_coeff=0.01,
    )
    .evaluation(
        evaluation_num_env_runners=1,
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
        evaluation_config=PPOConfig.overrides(explore=False),
    )
)

stop = {
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": args.stop_timesteps,
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": (
        args.stop_reward
    ),
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    results = run_rllib_example_script_experiment(config, args, stop=stop)

    # Store the best checkpoint for recording.
    best_checkpoint = results.get_best_result(
        metric=f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
        mode="max",
    ).checkpoint.path

    # Configure the algorithm for offline recording.
    config.offline_data(
        output="local:///tmp/cartpole/",
        # Store columnar (tabular) data.
        output_write_episodes=False,
        # Each file should hold 1,000 rows.
        output_max_rows_per_file=1000,
        output_write_remaining_data=True,
        # LZ4-compress columns 'obs', 'new_obs', and 'actions' to
        # save disk space and increase performance. Note, this means
        # that you have to use `input_compress_columns` in the same
        # way when using the data for training in `RLlib`.
        output_compress_columns=[Columns.OBS, Columns.ACTIONS],
    )
    # Change the evaluation settings to sample exactly 50 episodes
    # per evaluation iteration and increase the number of evaluation
    # env-runners to 5.
    config.evaluation(
        evaluation_num_env_runners=5,
        evaluation_duration=50,
        evaluation_duration_unit="episodes",
        evaluation_interval=1,
        evaluation_parallel_to_training=False,
        evaluation_config=PPOConfig.overrides(explore=False),
    )

    # Build the algorithm for evaluation.
    algo = config.build()
    # Load the checkpoint stored above.
    algo.restore_from_path(
        best_checkpoint,
        component=COMPONENT_RL_MODULE,
    )

    # Evaluate over 10 iterations and record the data.
    for i in range(10):
        print(f"Iteration: {i + 1}:\n")
        res = algo.evaluate()
        print(res)

    # Stop the algorithm.
    algo.stop()

    # Check the number of rows in the dataset.
    ds = ray.data.read_parquet("local:///tmp/cartpole")
    print(f"Number of experiences recorded: {ds.count()}")
