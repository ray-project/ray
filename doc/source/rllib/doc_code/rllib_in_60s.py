# flake8: noqa

# __rllib-in-60s-begin__
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

config = (  # 1. Configure the algorithm,
    PPOConfig()
    .environment(env="Taxi-v3")
    .env_runners(num_env_runners=2)
    .rl_module(model_config=DefaultModelConfig(fcnet_hiddens=[64, 64]))
    .evaluation(evaluation_num_env_runners=1)
)

algo = config.build()  # 2. build the algorithm,

for _ in range(5):
    print(algo.train())  # 3. train it,

algo.evaluate()  # 4. and evaluate it.
# __rllib-in-60s-end__
