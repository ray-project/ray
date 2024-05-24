from pathlib import Path

from ray.rllib.algorithms.bc import BCConfig

data_path = "tests/data/pendulum/small.json"
base_path = Path(__file__).parents[3]
print(f"base_path={base_path}")
data_path = "local://" + base_path.joinpath(data_path).as_posix()
print(f"data_path={data_path}")


config = (
    BCConfig()
    .environment(env="Pendulum-v1")
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .evaluation(
        evaluation_interval=3,
        evaluation_num_env_runners=1,
        evaluation_duration=5,
        evaluation_parallel_to_training=True,
    )
    .offline_data(input_=[data_path])
    .training(
        train_batch_size=32,
    )
)

algo = config.build()

for _ in range(10):
    results = algo.train()
    print(results)
