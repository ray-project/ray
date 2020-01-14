from ray.rllib.agents.ppo import PPOAgent
from ray import tune
import ray

if __name__ == "__main__":
    ray.init()
    # Test legacy *Agent classes work (renamed to Trainer)
    tune.run(
        PPOAgent,
        config={"env": "CartPole-v0"},
        stop={"training_iteration": 2})
