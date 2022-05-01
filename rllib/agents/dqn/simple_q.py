"""
Simple Q-Learning
=================

This module provides a basic implementation of the DQN algorithm without any
optimizations.

This file defines the distributed Trainer class for the Simple Q algorithm.
See `simple_q_[tf|torch]_policy.py` for the definition of the policy loss.
"""

import logging
from typing import Optional, Type

from ray.rllib.agents.dqn.simple_q_tf_policy import SimpleQTFPolicy
from ray.rllib.agents.dqn.simple_q_torch_policy import SimpleQTorchPolicy
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.utils.metrics import SYNCH_WORKER_WEIGHTS_TIMER
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.execution.rollout_ops import (
    ParallelRollouts,
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    TrainOneStep,
    MultiGPUTrainOneStep,
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.execution.train_ops import (
    UpdateTargetNetwork,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    TARGET_NET_UPDATE_TIMER,
)
from ray.rllib.utils.typing import (
    ResultDict,
    TrainerConfigDict,
)
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
)

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Exploration Settings ===
    "exploration_config": {
        # The Exploration class to use.
        "type": "EpsilonGreedy",
        # Config for the Exploration class' constructor:
        "initial_epsilon": 1.0,
        "final_epsilon": 0.02,
        "epsilon_timesteps": 10000,  # Timesteps over which to anneal epsilon.

        # For soft_q, use:
        # "exploration_config" = {
        #   "type": "SoftQ"
        #   "temperature": [float, e.g. 1.0]
        # }
    },
    # Switch to greedy actions in evaluation workers.
    "evaluation_config": {
        "explore": False,
    },

    # Minimum env steps to optimize for per train call. This value does
    # not affect learning, only the length of iterations.
    "timesteps_per_iteration": 1000,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 500,

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": DEPRECATED_VALUE,
    # The following values have moved because of the new ReplayBuffer API
    "prioritized_replay": DEPRECATED_VALUE,
    "learning_starts": DEPRECATED_VALUE,
    "replay_batch_size": DEPRECATED_VALUE,
    "replay_sequence_length": DEPRECATED_VALUE,
    "prioritized_replay_alpha": DEPRECATED_VALUE,
    "prioritized_replay_beta": DEPRECATED_VALUE,
    "prioritized_replay_eps": DEPRECATED_VALUE,
    "replay_buffer_config": {
        # Use the new ReplayBuffer API here
        "_enable_replay_buffer_api": True,
        # How many steps of the model to sample before learning starts.
        "learning_starts": 1000,
        "type": "MultiAgentReplayBuffer",
        "capacity": 50000,
        "replay_batch_size": 32,
        # The number of contiguous environment steps to replay at once. This
        # may be set to greater than 1 to support recurrent models.
        "replay_sequence_length": 1,
    },
    # Set this to True, if you want the contents of your buffer(s) to be
    # stored in any saved checkpoints as well.
    # Warnings will be created if:
    # - This is True AND restoring from a checkpoint that contains no buffer
    #   data.
    # - This is False AND restoring from a checkpoint that does contain
    #   buffer data.
    "store_buffer_in_checkpoints": False,

    # === Optimization ===
    # Learning rate for adam optimizer
    "lr": 5e-4,
    # Learning rate schedule.
    # In the format of [[timestep, value], [timestep, value], ...]
    # A schedule should normally start from timestep 0.
    "lr_schedule": None,
    # Adam epsilon hyper parameter
    "adam_epsilon": 1e-8,
    # If not None, clip gradients during optimization at this value
    "grad_clip": 40,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    "rollout_fragment_length": 4,
    # Size of a batch sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 32,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Prevent reporting frequency from going lower than this time span.
    "min_time_s_per_reporting": 1,

    # Experimental flag.
    # If True, the execution plan API will not be used. Instead,
    # a Trainer's `training_iteration` method will be called as-is each
    # training iteration.
    "_disable_execution_plan_api": True,
})
# __sphinx_doc_end__
# fmt: on


class SimpleQTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        """Checks and updates the config based on settings."""
        # Call super's validation method.
        super().validate_config(config)

        if config["exploration_config"]["type"] == "ParameterNoise":
            if config["batch_mode"] != "complete_episodes":
                logger.warning(
                    "ParameterNoise Exploration requires `batch_mode` to be "
                    "'complete_episodes'. Setting batch_mode="
                    "complete_episodes."
                )
                config["batch_mode"] = "complete_episodes"
            if config.get("noisy", False):
                raise ValueError(
                    "ParameterNoise Exploration and `noisy` network cannot be"
                    " used at the same time!"
                )

        validate_buffer_config(config)

        # Multi-agent mode and multi-GPU optimizer.
        if config["multiagent"]["policies"] and not config["simple_optimizer"]:
            logger.info(
                "In multi-agent mode, policies will be optimized sequentially"
                " by the multi-GPU optimizer. Consider setting "
                "`simple_optimizer=True` if this doesn't work for you."
            )

    @override(Trainer)
    def get_default_policy_class(
        self, config: TrainerConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return SimpleQTorchPolicy
        else:
            return SimpleQTFPolicy

    @ExperimentalAPI
    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        """Simple Q training iteration function.

        Simple Q consists of the following steps:
        - Sample n MultiAgentBatches from n workers synchronously.
        - Store new samples in the replay buffer.
        - Sample one training MultiAgentBatch from the replay buffer.
        - Learn on the training batch.
        - Update the target network every `target_network_update_freq` steps.
        - Return all collected training metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        batch_size = self.config["train_batch_size"]
        local_worker = self.workers.local_worker()

        # Sample n MultiAgentBatches from n workers.
        new_sample_batches = synchronous_parallel_sample(
            worker_set=self.workers, concat=False
        )

        for batch in new_sample_batches:
            # Update sampling step counters.
            self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()
            self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
            # Store new samples in the replay buffer
            self.local_replay_buffer.add(batch)

        # Sample one training MultiAgentBatch from replay buffer.
        train_batch = self.local_replay_buffer.sample(batch_size)

        # Learn on the training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # TODO: Move training steps counter update outside of `train_one_step()` method.
        # # Update train step counters.
        # self._counters[NUM_ENV_STEPS_TRAINED] += train_batch.env_steps()
        # self._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

        # Update target network every `target_network_update_freq` steps.
        cur_ts = self._counters[NUM_ENV_STEPS_SAMPLED]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update >= self.config["target_network_update_freq"]:
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = local_worker.get_policies_to_train()
                local_worker.foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        # Update weights and global_vars - after learning on the local worker - on all
        # remote workers.
        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            self.workers.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results

    @staticmethod
    @override(Trainer)
    def execution_plan(workers, config, **kwargs):
        assert (
            "local_replay_buffer" in kwargs
        ), "GenericOffPolicy execution plan requires a local replay buffer."

        local_replay_buffer = kwargs["local_replay_buffer"]

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # (1) Generate rollouts and store them in our local replay buffer.
        store_op = rollouts.for_each(
            StoreToReplayBuffer(local_buffer=local_replay_buffer)
        )

        if config["simple_optimizer"]:
            train_step_op = TrainOneStep(workers)
        else:
            train_step_op = MultiGPUTrainOneStep(
                workers=workers,
                sgd_minibatch_size=config["train_batch_size"],
                num_sgd_iter=1,
                num_gpus=config["num_gpus"],
                _fake_gpus=config["_fake_gpus"],
            )

        # (2) Read and train on experiences from the replay buffer.
        replay_op = (
            Replay(local_buffer=local_replay_buffer)
            .for_each(train_step_op)
            .for_each(
                UpdateTargetNetwork(workers, config["target_network_update_freq"])
            )
        )

        # Alternate deterministically between (1) and (2).
        train_op = Concurrently(
            [store_op, replay_op], mode="round_robin", output_indexes=[1]
        )

        return StandardMetricsReporting(train_op, workers, config)
