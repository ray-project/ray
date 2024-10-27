# flake8: noqa

# __rllib-in-60s-begin__
from ray.rllib.algorithms.ppo import PPOConfig

# 1. Configure the algorithm,
config = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("Taxi-v3")
    .env_runners(num_env_runners=2)
    .framework("torch")
    .training(model={"fcnet_hiddens": [64, 64]})
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
