# ruff: noqa

# __rllib-in-60s-begin__
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import FlattenObservations

# 1. Configure the algorithm,
config = (
    PPOConfig()
    .environment("Taxi-v3")
    .env_runners(
        num_env_runners=2,
        # Observations are discrete (ints) -> We need to flatten (one-hot) them.
        env_to_module_connector=lambda env: FlattenObservations(),
    )
    .evaluation(evaluation_num_env_runners=1)
)
# 2. build the algorithm ..
algo = config.build()
# 3. .. train it ..
for _ in range(5):
    print(algo.train())
# 4. .. and evaluate it.
algo.evaluate()
# __rllib-in-60s-end__
