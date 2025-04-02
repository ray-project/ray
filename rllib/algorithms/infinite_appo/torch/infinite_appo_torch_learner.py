import random

from ray.rllib.algorithms.impala.impala_learner import (
    QUEUE_SIZE_GPU_LOADER_QUEUE,
    LEARNER_THREAD_ENV_STEPS_DROPPED,
)
from ray.rllib.algorithms.appo.torch.appo_torch_learner import APPOTorchLearner
from ray.rllib.core import ALL_MODULES, COMPONENT_RL_MODULE


class InfiniteAPPOLearner(APPOTorchLearner):
    def __init__(self, *, config, module_spec):
        super().__init__(config=config, module_spec=module_spec)
        self._num_batches = 0

    # Synchronization helper method.
    def sync(self):
        return None

    def set_other_actors(self, *, metrics_actor, weights_server_actors):
        self._metrics_actor = metrics_actor
        self._weights_server_actors = weights_server_actors

    def update(self, batch, timesteps, send_weights=False):
        global _CURRENT_GLOBAL_TIMESTEPS
        _CURRENT_GLOBAL_TIMESTEPS = timesteps

        # Enqueue the batch, either directly into the learner thread's queue or to the
        # GPU loader threads.
        if self.config.num_gpus_per_learner > 0:
            self._gpu_loader_in_queue.put(batch)
            self.metrics.log_value(
                (ALL_MODULES, QUEUE_SIZE_GPU_LOADER_QUEUE),
                self._gpu_loader_in_queue.qsize(),
            )
        else:
            ts_dropped = self._learner_thread_in_queue.add(batch)
            self.metrics.log_value(
                (ALL_MODULES, LEARNER_THREAD_ENV_STEPS_DROPPED),
                ts_dropped,
                reduce="sum",
            )

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
        if self._num_batches % 10 == 0:
            self._metrics_actor.add.remote(
                learner_metrics=self.metrics.reduce(),
            )

        self._num_batches += 1

