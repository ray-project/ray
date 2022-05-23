import logging
from ray.rllib.algorithms.cql import CQL_DEFAULT_CONFIG # TODO: ??
from ray.rllib.algorithms.crr import CRR
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

from ray.rllib.utils.framework import try_import_tf, try_import_tfp
tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()
logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
CRR_DEFAULT_CONFIG = merge_dicts(
    CQL_DEFAULT_CONFIG, {
        # You should override this to point to an offline dataset.
        "input": "sampler",
        # Switch off off-policy evaluation.
        "input_evaluation": [],
        # Number of iterations with Behavior Cloning Pretraining.
        "bc_iters": 20000,
        # CQL loss temperature.
        "temperature": 1.0,
        # Number of actions to sample for CQL loss.
        "num_actions": 10,
        # Whether to use the Lagrangian for Alpha Prime (in CQL loss).
        "lagrangian": False,
        # Lagrangian threshold.
        "lagrangian_thresh": 5.0,
        # Min Q weight multiplier.
        "min_q_weight": 5.0,
        # Reporting: As CQL is offline (no sampling steps), we need to limit
        # `self.train()` reporting by the number of steps trained (not sampled).
        "min_sample_timesteps_per_reporting": 0,
        "min_train_timesteps_per_reporting": 100,

        # Deprecated keys.
        # Use `min_sample_timesteps_per_reporting` and
        # `min_train_timesteps_per_reporting` instead.
        "timesteps_per_iteration": DEPRECATED_VALUE,
    })
# __sphinx_doc_end__
# fmt: on
# TODO
class CRRConfig:

    def __init__(self):
        super().__init__()
        self.trainer_class = CRR
