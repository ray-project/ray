import logging
from typing import Optional, List

from ray.rllib.algorithms.crr import CRR
from ray.rllib.agents.trainer_config import TrainerConfig

logger = logging.getLogger(__name__)


class CRRConfig(TrainerConfig):
    def __init__(self, trainer_class=None):
        super().__init__(trainer_class=trainer_class or CRR)

        # fmt: off
        # __sphinx_doc_begin__
        # CRR-specific settings.
        self.weight_type = "bin"
        self.temperature = 1.0
        self.max_weight = 20.0
        self.advantage_type = "mean"
        self.n_action_sample = 4
        self.twin_q = True
        self.target_update_grad_intervals = 100
        self.replay_buffer_config = {
            "type": "ReplayBuffer",
            "capacity": 50000,
            # How many steps of the model to sample before learning starts.
            "learning_starts": 1000,
            "replay_batch_size": 32,
            # The number of contiguous environment steps to replay at once. This
            # may be set to greater than 1 to support recurrent models.
            "replay_sequence_length": 1,
        }
        self.actor_hiddens = [256, 256]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [256, 256]
        self.critic_hidden_activation = "relu"
        self.critic_lr = 3e-4
        self.actor_lr = 3e-4
        self.tau = 5e-3
        # __sphinx_doc_end__
        # fmt: on

        # overriding the trainer config default
        self.num_workers = 0  # offline RL does not need rollout workers

    def training(
        self,
        *,
        weight_type: Optional[str] = None,
        temperature: Optional[float] = None,
        max_weight: Optional[float] = None,
        advantage_type: Optional[str] = None,
        n_action_sample: Optional[int] = None,
        twin_q: Optional[bool] = None,
        target_update_grad_intervals: Optional[int] = None,
        replay_buffer_config: Optional[dict] = None,
        actor_hiddens: Optional[List[int]] = None,
        actor_hidden_activation: Optional[str] = None,
        critic_hiddens: Optional[List[int]] = None,
        critic_hidden_activation: Optional[str] = None,
        tau: Optional[float] = None,
        **kwargs,
    ) -> "CRRConfig":

        """
        === CRR configs

        Args:
            weight_type: weight type to use `bin` | `exp`.
            temperature: the exponent temperature used in exp weight type.
            max_weight: the max weight limit for exp weight type.
            advantage_type: The way we reduce q values to v_t values `max` | `mean`.
            n_action_sample: the number of actions to sample for v_t estimation.
            twin_q: if True, uses pessimistic q estimation.
            target_update_grad_intervals: The frequency at which we update the
                target copy of the model in terms of the number of gradient updates
                applied to the main model.
            replay_buffer_config: The config dictionary for replay buffer.
            actor_hiddens: The number of hidden units in the actor's fc network.
            actor_hidden_activation: The activation used in the actor's fc network.
            critic_hiddens: The number of hidden units in the critic's fc network.
            critic_hidden_activation: The activation used in the critic's fc network.
            tau: Polyak averaging coefficient
                (making it 1 is reduces it to a hard update).
            **kwargs: forward compatibility kwargs

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
        if target_update_grad_intervals is not None:
            self.target_update_grad_intervals = target_update_grad_intervals
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
