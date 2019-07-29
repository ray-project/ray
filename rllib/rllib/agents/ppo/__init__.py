from ...agents.ppo.ppo import PPOTrainer, DEFAULT_CONFIG
from ...agents.ppo.appo import APPOTrainer
from ...utils import renamed_agent

PPOAgent = renamed_agent(PPOTrainer)

__all__ = ["PPOAgent", "APPOTrainer", "PPOTrainer", "DEFAULT_CONFIG"]
