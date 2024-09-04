from pathlib import Path

from ray.rllib.algorithms.cql.cql import CQLConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args()
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

assert (
    args.env == "Pendulum-v1" or args.env is None
), "This tuned example works only with `Pendulum-v1`."


base_path = Path(__file__).parents[2]
data_path = base_path / "tests/data/pendulum/pendulum-v1_enormous"

config = (
    CQLConfig()
    .environment("Pendulum-v1")
    .api_stack(
        enable_env_runner_and_connector_v2=True,
        enable_rl_module_and_learner=True,
    )
    .offline_data(
        input_=[data_path.as_posix()],
        actions_in_input_normalized=True,
        dataset_num_iters_per_learner=1 if args.num_gpus == 0 else None,
    )
    .training(
        bc_iters=200,
        tau=9.5e-3,
        min_q_weight=5.0,
        train_batch_size_per_learner=2048,
        twin_q=True,
        actor_lr=1.7e-3 * (args.num_gpus or 1) ** 0.5,
        critic_lr=2.5e-3 * (args.num_gpus or 1) ** 0.5,
        alpha_lr=1e-3 * (args.num_gpus or 1) ** 0.5,
        lr=None,
    )
    .reporting(
        min_time_s_per_iteration=10,
        metrics_num_episodes_for_smoothing=5,
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_num_env_runners=0,
        evaluation_duration=10,
        evaluation_config={
            "explore": False,
        },
    )
)

stop = {
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -700.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 800000,
}

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args, stop=stop)
