from pathlib import Path

from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    TRAINING_ITERATION_TIMER,
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
    args.env == "CartPole-v1" or args.env is None
), "This tuned example works only with `CartPole-v1`."

# Define the data paths.
data_path = "tests/data/cartpole/cartpole-v1_large"
base_path = Path(__file__).parents[2]
print(f"base_path={base_path}")
data_path = "local://" + base_path.joinpath(data_path).as_posix()
print(f"data_path={data_path}")

# Define the BC config.
config = (
    BCConfig()
    .environment(env="CartPole-v1")
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .learners(
        num_learners=args.num_gpus,
    )
    .evaluation(
        evaluation_interval=3,
        evaluation_num_env_runners=1,
        evaluation_duration=5,
        evaluation_parallel_to_training=True,
    )
    .offline_data(input_=[data_path])
    .training(
        # To increase learning speed with multiple learners,
        # increase the learning rate correspondingly.
        lr=0.0008 * max(1, args.num_gpus**0.5),
        train_batch_size_per_learner=2000,
    )
)

stop = {
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 120.0,
    TRAINING_ITERATION_TIMER: 350.0,
}

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args, stop=stop)

# num_iterations = 350
# min_reward = 120.0

# # TODO (simon): Add support for recurrent modules.
algo = config.build()
# learnt = False
# start = time.perf_counter()
# for i in range(num_iterations):
#     results = algo.train()

#     eval_results = results.get("evaluation", {})
#     if eval_results:
#         episode_return_mean = eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
#         print(f"iter={i}, R={episode_return_mean}")
#         if episode_return_mean > min_reward:
#             print("BC has learnt the task!")
#             learnt = True
#             break

# if not learnt:
#     raise ValueError(
#         f"`BC` did not reach {min_reward} reward from expert offline data!"
#     )

# stop = time.perf_counter()
# print(f"Time needed: {(stop - start)} secs.")
# algo.stop()
