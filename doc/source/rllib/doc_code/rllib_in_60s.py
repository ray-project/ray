# flake8: noqa

# __rllib-in-60s-begin__
from ray.rllib.algorithms.ppo import PPOConfig

config = (  # 1. Configure the algorithm,
    PPOConfig()
    .environment("Taxi-v3")
    .rollouts(num_rollout_workers=2)
    .framework("tf2")
    .training(model={"fcnet_hiddens": [64, 64]})
    .evaluation(evaluation_num_workers=1)
)

algo = config.build()  # 2. build the algorithm,

for _ in range(5):
    print(algo.train())  # 3. train it,

algo.evaluate()  # 4. and evaluate it.
# __rllib-in-60s-end__
