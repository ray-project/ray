from rllib_pg.pg.pg import PG, PGConfig
from rllib_pg.pg.pg_tf_policy import PGTF1Policy, PGTF2Policy
from rllib_pg.pg.pg_torch_policy import PGTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["PGConfig", "PG", "PGTF1Policy", "PGTF2Policy", "PGTorchPolicy"]

register_trainable("rllib-contrib-pg", PG)
