from ray.rllib.agents.pg.pg import PGTrainer, DEFAULT_CONFIG
from ray.rllib.agents.pg.tf_pg_policy import tf_pg_loss, \
    post_process_advantages
from ray.rllib.agents.pg.torch_pg_policy import torch_pg_loss
from ray.rllib.utils import renamed_agent

# `Agent`=Legacy: Use PGTrainer instead.
PGAgent = renamed_agent(PGTrainer)

__all__ = ["PGAgent", "PGTrainer", "tf_pg_loss", "torch_pg_loss",
           "post_process_advantages", "DEFAULT_CONFIG"]
