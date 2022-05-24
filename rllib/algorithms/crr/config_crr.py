import logging
from ray.rllib.algorithms.ddpg import DDPGConfig
from ray.rllib.algorithms.crr import CRR
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

from ray.rllib.utils.framework import try_import_tf, try_import_tfp
tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()
logger = logging.getLogger(__name__)


class CRRConfig(DDPGConfig):

    def __init__(self, trainer_class=None):
        super().__init__(trainer_class=trainer_class or CRR)
