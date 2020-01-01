from ray.rllib.agents.pg.pg import PGTrainer, DEFAULT_CONFIG, \
    post_process_advantages
from ray.rllib.agents.pg.pg_tf_policy import pg_tf_loss
from ray.rllib.agents.pg.pg_torch_policy import pg_torch_loss
from ray.rllib.utils import renamed_agent

__all__ = ["PGTrainer", "pg_tf_loss", "pg_torch_loss",
           "post_process_advantages", "DEFAULT_CONFIG"]
