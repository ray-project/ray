"""Examples demonstrating the usage of RE3 exploration strategy.

To use RE3 user will need to patch the callbacks with RE3 specific callbacks
and set the `exploration_config` with RE3 configs.

```Python

config["exploration_config"] = {"type": "RE3"}
"""
from functools import partial
import ray
from ray.rllib.agents import sac
from ray.rllib.agents.callbacks import MultiCallbacks, RE3UpdateCallbacks

if __name__ == "__main__":
    ray.init()

    config = sac.DEFAULT_CONFIG.copy()

    # Add a new RE3UpdateCallbacks
    config["callbacks"] = MultiCallbacks(
        [
            config["callbacks"],
            partial(
                RE3UpdateCallbacks,
                embeds_dim=128,
                beta_schedule="linear_decay",
                k_nn=50,
            ),
        ]
    )
    config["env"] = "LunarLanderContinuous-v2"
    config["seed"] = 12345
    # Add type as RE3 in the exploration_config parameter
    config["exploration_config"] = {
        "type": "RE3",
        "sub_exploration": {
            "type": "StochasticSampling",
        },
    }

    num_iterations = 2000
    trainer = sac.SACTrainer(config=config)
    for i in range(num_iterations):
        result = trainer.train()
        print(result)
    trainer.stop()
    ray.shutdown()
