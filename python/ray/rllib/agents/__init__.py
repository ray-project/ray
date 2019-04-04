from ray.rllib.agents.trainer import Trainer, with_common_config

# TODO: remove the legacy agent name
Agent = Trainer

__all__ = ["Agent", "Trainer", "with_common_config"]
