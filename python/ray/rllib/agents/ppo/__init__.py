from ray.rllib.agents.ppo.ppo import PPOTrainer, DEFAULT_CONFIG
from ray.rllib.agents.ppo.appo import APPOTrainer

# TODO remove the legacy agent names
PPOAgent = PPOTrainer

__all__ = ["PPOAgent", "APPOTrainer", "PPOTrainer", "DEFAULT_CONFIG"]
