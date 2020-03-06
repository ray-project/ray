from ray.rllib.agents.trainer import Trainer
from ray.rllib.utils import renamed_agent

Agent = renamed_agent(Trainer)
