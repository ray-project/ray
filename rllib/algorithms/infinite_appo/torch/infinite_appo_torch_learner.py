import random

import ray
from ray.rllib.algorithms.appo.torch.appo_torch_learner import APPOTorchLearner
from ray.rllib.algorithms.appo.utils import CircularBuffer
from ray.rllib.algorithms.infinite_appo.infinite_appo_aggregator_actor import (
    InfiniteAPPOAggregatorActor
)
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.utils.metrics import NUM_ENV_STEPS_TRAINED_LIFETIME
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class InfiniteAPPOTorchLearner(APPOTorchLearner):
    def __init__(self, *, config, module_spec):
        super().__init__(config=config, module_spec=module_spec)
        self._num_batches = 0
        self._timesteps = {NUM_ENV_STEPS_TRAINED_LIFETIME: 0}

        # Create child aggregator actors.
        node_id = ray.get_runtime_context().get_node_id()
        strategy = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
        self.aggregator_actors = [
            InfiniteAPPOAggregatorActor.options(
                scheduling_strategy=strategy,
            ).remote(
                config=self.config,
                rl_module_spec=module_spec,
                sync_freq=self.config.pipeline_sync_freq,
            )
            for _ in range(self.config.num_aggregator_actors_per_learner)
        ]

    def build(self) -> None:
        super().build()
        # Stop the Learner thread again and delete.
        self._learner_thread.stopped = True
        del self._learner_thread

        # Recreate the circular buffer with K-1 (b/c we use the incoming batch right
        # away for 1 update, only then add it to the buffer).
        self._learner_thread_in_queue = CircularBuffer(
            num_batches=self.config.circular_buffer_num_batches,
            iterations_per_batch=self.config.circular_buffer_iterations_per_batch - 1,
        )

    # Synchronization helper method.
    def set_other_actors(self, *, metrics_actor, weights_server_actors, batch_dispatchers, learner_idx):
        self._metrics_actor = metrics_actor
        self._weights_server_actors = weights_server_actors

        for agg in self.aggregator_actors:
            ray.get(agg.set_other_actors.remote(
                batch_dispatchers=batch_dispatchers,
                metrics_actor=metrics_actor,
                learner_idx=learner_idx,
            ))

    def update(self, batch, timesteps, send_weights=False):
        if timesteps is not None:
            self._timesteps = timesteps

        # Load the batch to the GPU.
        batch_on_gpu = batch.to_device(self._device, pin_memory=True)

        # If buffer is full, pull K batches from it and perform an update on each.
        if (
            self.config.circular_buffer_iterations_per_batch == 1
            or self._learner_thread_in_queue.filled
        ):
            for i in range(self.config.circular_buffer_iterations_per_batch):
                # Don't sample the very first batch, but use the one we just received.
                # This saves an entire sampling step AND makes sure that new batches
                # are consumed right away (at least once) before we even add them to
                # the circular buffer.
                if i > 0:
                    batch_on_gpu = self._learner_thread_in_queue.sample()
                TorchLearner.update(
                    self,
                    training_data=TrainingData(batch=batch_on_gpu),
                    timesteps=self._timesteps,
                    _no_metrics_reduce=True,
                )
                self._num_batches += 1
                self._timesteps[NUM_ENV_STEPS_TRAINED_LIFETIME] += (
                    batch.env_steps() * self.config.num_learners
                )

        if self.config.circular_buffer_iterations_per_batch > 1:
            self._learner_thread_in_queue.add(batch_on_gpu)

        # Figure out, whether we need to send our weights to a weights server.
        if send_weights and self._weights_server_actors:
            learner_state = self.get_state(
                # Only return the state of those RLModules that are trainable.
                components=[
                    COMPONENT_RL_MODULE + "/" + mid
                    for mid in self.module.keys()
                    if self.should_module_be_updated(mid)
                ],
                inference_only=True,
            )
            learner_state[COMPONENT_RL_MODULE] = ray.put(
                learner_state[COMPONENT_RL_MODULE]
            )
            random.choice(self._weights_server_actors).put.remote(
                learner_state, broadcast=True
            )

        # Send metrics to metrics actor.
        if self._num_batches >= 10:
            self._metrics_actor.add.remote(
                learner_metrics=self.metrics.reduce(),
            )
            self._num_batches = 0
