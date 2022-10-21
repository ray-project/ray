# __rllib-in-60s-begin__
# Import the RL algorithm (Algorithm) we would like to use.
from ray.rllib.algorithms.ppo import PPOConfig

# Configure the algorithm.
config = (
    PPOConfig()
    # Environment (RLlib understands openAI gym registered strings).
    .environment("Taxi-v3")
    # Use 2 environment workers (aka "rollout workers") that parallelly
    # collect samples from their own environment clone(s).
    .rollouts(num_rollout_workers=2)
    # Change this to `framework("torch")`, if you are using PyTorch.
    # Use `framework("tf2", eager_tracing=True)` for tf2.x traced execution.
    .framework("tf")
    # Tweak the default model provided automatically by RLlib,
    # given the environment's observation- and action spaces.
    .training(
        model={
            "fcnet_hiddens": [64, 64],
            "fcnet_activation": "relu",
        }
    )
    # Set up a separate evaluation worker set for the
    # `algo.evaluate()` call after training (see below).
    .evaluation(
        evaluation_num_workers=1,
    )
)

# Create our RLlib Trainer from the config object.
algo = config.build()

# Run it for n training iterations. A training iteration includes
# parallel sample collection by the environment workers as well as
# loss calculation on the collected batch and a model update.
for _ in range(3):
    print(algo.train())

# Evaluate the trained Trainer (and render each timestep to the shell's
# output).
algo.evaluate()

# __rllib-in-60s-end__
