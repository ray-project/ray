# Run this utility to create test checkpoints (usable in the backward compat
# test cases) for all frameworks.
# Checkpoints will be located in ~/ray_results/...

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.test_utils import framework_iterator

# Build a PPOConfig object.
config = (
    PPOConfig()
    .environment("FrozenLake-v1")
    .training(
        num_sgd_iter=2,
        model=dict(
            fcnet_hiddens=[10],
        ),
    )
)

for fw in framework_iterator(config, with_eager_tracing=True):
    algo = config.build()
    results = algo.train()
    algo.save()
    algo.stop()
