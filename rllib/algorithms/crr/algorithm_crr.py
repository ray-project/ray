import copy
from typing import Type, List

from ray.rllib.policy import Policy
from ray.rllib.utils.typing import ResultDict, TrainerConfigDict
from ray.rllib.utils.annotations import override
from ray.rllib.algorithms.ddpg import DDPGTrainer
from ray.rllib.utils.typing import PartialTrainerConfigDict
from ray.rllib.offline.shuffled_input import ShuffledInput
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)

from ray.rllib.agents.trainer import Trainer, TrainerConfig

# from ray.rllib.algorithms.crr.config_crr import CRRConfig

from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
    TARGET_NET_UPDATE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
)

from ray.rllib.utils.replay_buffers.utils import update_priorities_in_replay_buffer


import numpy as np


from typing import Optional
import logging
from ray.rllib.algorithms.ddpg import DDPGConfig
# from ray.rllib.algorithms.crr import CRR
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.models.catalog import MODEL_DEFAULTS

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
        self.actor_hiddens = [400, 300]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [400, 300]
        self.critic_hidden_activation = "relu"

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
        if actor_hiddens is not None:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not None:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not None:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not None:
            self.critic_hidden_activation = critic_hidden_activation

        return self



class CRR(Trainer):

    # TODO: we have a circular dependency for get default config. config -> Trainer -> config

    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)
        # initial setup for handling the offline data in form of a replay buffer
        # Add the entire dataset to Replay Buffer (global variable)
        reader = self.workers.local_worker().input_reader

        # For d4rl, add the D4RLReaders' dataset to the buffer.
        if isinstance(self.config["input"], str) and "d4rl" in self.config["input"]:
            dataset = reader.dataset
            self.local_replay_buffer.add(dataset)
        # For a list of files, add each file's entire content to the buffer.
        elif isinstance(reader, ShuffledInput):
            num_batches = 0
            total_timesteps = 0
            for batch in reader.child.read_all_files():
                num_batches += 1
                total_timesteps += len(batch)
                # Add NEXT_OBS if not available. This is slightly hacked
                # as for the very last time step, we will use next-obs=zeros
                # and therefore force-set DONE=True to avoid this missing
                # next-obs to cause learning problems.
                if SampleBatch.NEXT_OBS not in batch:
                    obs = batch[SampleBatch.OBS]
                    batch[SampleBatch.NEXT_OBS] = np.concatenate(
                        [obs[1:], np.zeros_like(obs[0:1])]
                    )
                    batch[SampleBatch.DONES][-1] = True
                self.local_replay_buffer.add_batch(batch)
            print(
                f"Loaded {num_batches} batches ({total_timesteps} ts) into the"
                " replay buffer, which has capacity "
                f"{self.local_replay_buffer.capacity}."
            )
        else:
            raise ValueError(
                "Unknown offline input! config['input'] must either be list of"
                " offline files (json) or a D4RL-specific InputReader "
                "specifier (e.g. 'd4rl.hopper-medium-v0')."
            )

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return CRRConfig().to_dict()

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.crr.torch.policy_torch_crr import CRRTorchPolicy
            return CRRTorchPolicy
        else:
            raise ValueError('Other frameworks are not supported yet!')

    @override(Trainer)
    def training_iteration(self) -> ResultDict:

        # Sample training batch from replay buffer.
        train_batch = self.local_replay_buffer.sample(self.config["train_batch_size"])

        # Old-style replay buffers return None if learning has not started
        if not train_batch:
            return {}

        # Postprocess batch before we learn on it.
        post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
        train_batch = post_fn(train_batch, self.workers, self.config)

        # Learn on training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer", False):
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # Update replay buffer priorities.
        update_priorities_in_replay_buffer(
            self.local_replay_buffer,
            self.config,
            train_batch,
            train_results,
        )

        # Update target network every `target_network_update_freq` training steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_TRAINED if self._by_agent_steps else NUM_ENV_STEPS_TRAINED
        ]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update >= self.config["target_network_update_freq"]:
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = self.workers.local_worker().get_policies_to_train()
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        # Update remote workers's weights after learning on local worker
        if self.workers.remote_workers():
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.workers.sync_weights()

        # Return all collected metrics for the iteration.
        return train_results
