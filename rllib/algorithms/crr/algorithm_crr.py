from typing import Type

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


class CRR(DDPGTrainer):

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

    @override(DDPGTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.crr.torch.policy_torch_crr import CRRTorchPolicy
            return CRRTorchPolicy
        else:
            raise ValueError('Other frameworks are not supported yet!')

    @override(DDPGTrainer)
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
        if self.config.get("simple_optimizer") is True:
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
