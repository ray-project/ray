# Run this utility to create test checkpoints (usable in the backward compat
# test cases) for all frameworks.
# Checkpoints will be located in ~/ray_results/...

from ray.rllib.algorithms.ppo import PPOConfig

# Build a PPOConfig object.
config = (
    PPOConfig()
    .environment("FrozenLake-v1")
    .training(
        num_epochs=2,
        model=dict(
            fcnet_hiddens=[10],
        ),
    )
)

algo = config.build()
results = algo.train()
algo.save()
algo.stop()
