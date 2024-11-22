import ray
import time

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

parser = add_rllib_example_script_args()
parser.set_defaults(checkpoint_at_end=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .env_runners(
        rollout_fragment_length="auto",
        num_env_runners=5,
        batch_mode="truncate_episodes",
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
        evaluation_config=PPOConfig.overrides(exploration=False),
    )
)

stop = {
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 200000,
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 350.0,
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
        evaluation_config=PPOConfig.overrides(exploration=False),
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

    # Wait a bit until all actors have been stopped.
    time.sleep(6.0)

    # Check the number of rows in the dataset.
    ds = ray.data.read_parquet("local:///tmp/cartpole")
    print(f"Number of experiences recorded: {ds.count()}")
