from typing import Optional
import logging
from ray.rllib.algorithms.ddpg import DDPGConfig
from ray.rllib.algorithms.crr import CRR
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.agents.trainer_config import TrainerConfig

from ray.rllib.utils.framework import try_import_tf, try_import_tfp
tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()
logger = logging.getLogger(__name__)


class CRRConfig(TrainerConfig):

    def __init__(self, trainer_class=None):
        super().__init__(trainer_class=trainer_class or CRR)

        # fmt: off
        # __sphinx_doc_begin__
        # CRR-specific settings.
        self.weight_type = 'bin'  # weight type to use `bin` | `exp`
        self.temperature = 1.0  # the exponent temperature used in exp weight type
        self.max_weight = 20.0  # the max weight limit for exp weight type
        self.advantage_type = 'mean'  # the way we reduce q values to v_t values `max` | `mean`
        self.n_action_sample = 20  # the number of actions to sample for v_t estimation
        self.twin_q = True
        self.target_network_update_freq = 500
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            "capacity": 50000,
            # How many steps of the model to sample before learning starts.
            "learning_starts": 1000,
            "replay_batch_size": 32,
            # The number of contiguous environment steps to replay at once. This
            # may be set to greater than 1 to support recurrent models.
            "replay_sequence_length": 1,
        }
        # __sphinx_doc_end__
        # fmt: on

    def training(
        self,
        *,
        weight_type: Optional[str] = None,
        temperature: Optional[float] = None,
        max_weight: Optional[float] = None,
        advantage_type: Optional[str] = None,
        n_action_sample: Optional[int] = None,
        twin_q: Optional[bool] = None,
        target_network_update_freq: Optional[int] = None,
        replay_buffer_config: Optional[dict] = None,
        **kwargs,
    ) -> "CRRConfig":

        """
        === CRR configs

        Args:
            weight_type (str): weight type to use `bin` | `exp`
            temperature (float): the exponent temperature used in exp weight type
            max_weight (float): the max weight limit for exp weight type
            advantage_type (str): the way we reduce q values to v_t values `max` | `mean`
            n_action_sample (int): the number of actions to sample for v_t estimation
            twin_q (bool): if True, uses pessimistic q estimation

            **kwargs:

        Returns:
            This updated CRRConfig object.
        """
        super().training(**kwargs)

        if weight_type is not None:
            self.weight_type = weight_type
        if temperature is not None:
            self.temperature = temperature
        if max_weight is not None:
            self.max_weight = max_weight
        if advantage_type is not None:
            self.advantage_type = advantage_type
        if n_action_sample is not None:
            self.n_action_sample = n_action_sample
        if twin_q is not None:
            self.twin_q = twin_q
        if target_network_update_freq is not None:
            self.target_network_update_freq = target_network_update_freq
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config

        return self
