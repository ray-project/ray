# __rllib-in-60s-begin__
# Import the RL algorithm (Algorithm) we would like to use.
from ray.rllib.algorithms.ppo import PPO

# Configure the algorithm.
config = {
    # Environment (RLlib understands openAI gym registered strings).
    "env": "Taxi-v3",
    # Set up a separate evaluation worker set for the
    # `algo.evaluate()` call after training (see below).
    "evaluation_num_workers": 1,
    # Only for evaluation runs, render the env.
    "evaluation_config": {
        "render_env": True,
    },
}

# Create our RLlib Trainer.
algo = PPO(config=config)

# Run it for n training iterations. A training iteration includes
# parallel sample collection by the environment workers as well as
# loss calculation on the collected batch and a model update.
for _ in range(3):
    print(algo.train())

# Evaluate the trained Trainer (and render each timestep to the shell's
# output).
algo.evaluate()

# __rllib-in-60s-end__
