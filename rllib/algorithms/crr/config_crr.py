from typing import Optional, List
import logging
from ray.rllib.algorithms.crr import CRR
from ray.rllib.agents.trainer_config import TrainerConfig

logger = logging.getLogger(__name__)


class CRRConfig(TrainerConfig):
    def __init__(self, trainer_class=None):
        super().__init__(trainer_class=trainer_class or CRR)

        # fmt: off
        # __sphinx_doc_begin__
        # CRR-specific settings.
        self.weight_type = 'bin'
        self.temperature = 1.0
        self.max_weight = 20.0
        self.advantage_type = 'mean'
        self.n_action_sample = 20
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
        self.actor_hiddens = [400, 300]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [400, 300]
        self.critic_hidden_activation = "relu"
        self.critic_lr = 3e-4
        self.actor_lr = 3e-4
        self.tau = 0.002

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
        actor_hiddens: Optional[List[int]] = None,
        actor_hidden_activation: Optional[str] = None,
        critic_hiddens: Optional[List[int]] = None,
        critic_hidden_activation: Optional[str] = None,
        tau: Optional[float] = None,
        **kwargs,
    ) -> "CRRConfig":

        # TODO: complete the documentation
        """
        === CRR configs

        Args:
            weight_type (str): weight type to use `bin` | `exp`
            temperature (float): the exponent temperature used in exp weight type
            max_weight (float): the max weight limit for exp weight type
            advantage_type (str):
                the way we reduce q values to v_t values `max` | `mean`
            n_action_sample (int): the number of actions to sample for v_t estimation
            twin_q (bool): if True, uses pessimistic q estimation
            target_network_update_freq (int):
            replay_buffer_config (dict[str, Any]):
            actor_hiddens
            actor_hidden_activation
            critic_hiddens
            critic_hidden_activation
            tau (float):
                Polyak averaging coefficient
                (making it 1 is reduces it to a hard update)
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
        if actor_hiddens is not None:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not None:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not None:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not None:
            self.critic_hidden_activation = critic_hidden_activation
        if tau is not None:
            self.tau = tau

        return self
