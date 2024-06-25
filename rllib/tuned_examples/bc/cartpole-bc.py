from pathlib import Path

from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, EPISODE_RETURN_MEAN

data_path = "tests/data/cartpole/cartpole-v1_large.jsonl/1_000000_000000.json"
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
        num_learners=2,
    )
    .evaluation(
        evaluation_interval=3,
        evaluation_num_env_runners=1,
        evaluation_duration=5,
        evaluation_parallel_to_training=True,
    )
    .offline_data(input_=[data_path])
    .training(
        lr=0.0008,
    )
)

num_iterations = 350
min_reward = 120.0

# ray.init(local_mode=True)
# TODO (simon): Add support for recurrent modules.
algo = config.build()
learnt = False
for i in range(num_iterations):
    results = algo.train()

    eval_results = results.get("evaluation", {})
    if eval_results:
        episode_return_mean = eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        print(f"iter={i}, R={episode_return_mean}")
        if episode_return_mean > min_reward:
            print("BC has learnt the task!")
            learnt = True
            break

if not learnt:
    raise ValueError(
        f"`BC` did not reach {min_reward} reward from expert offline data!"
    )

algo.stop()
