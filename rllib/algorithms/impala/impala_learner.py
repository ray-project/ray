from queue import Queue, Empty
import threading
from typing import Dict, List, Optional

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.impala.impala import (
    ImpalaConfig,
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import Learner
from ray.rllib.connectors.learner import AddOneTsToEpisodesAndTruncate
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.episodes import (
    add_one_ts_to_episodes_and_truncate,
    remove_last_ts_from_data,
    remove_last_ts_from_episodes_and_restore_truncateds,
)
from ray.rllib.utils.postprocessing.value_predictions import extract_bootstrapped_values
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import EpisodeType, ModuleID, ResultDict

torch, _ = try_import_torch()

GPU_LOADER_QUEUE_WAIT_TIMER = "gpu_loader_queue_wait_timer"
GPU_LOADER_LOAD_TO_GPU_TIMER = "gpu_loader_load_to_gpu_timer"
LEARNER_THREAD_IN_QUEUE_WAIT_TIMER = "learner_thread_in_queue_wait_timer"
LEARNER_THREAD_UPDATE_TIMER = "learner_thread_update_timer"


class ImpalaLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Dict mapping module IDs to the respective entropy Scheduler instance.
        self.entropy_coeff_schedulers_per_module: Dict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: Scheduler(
                fixed_value_or_schedule=(
                    self.config.get_config_for_module(module_id).entropy_coeff
                ),
                framework=self.framework,
                device=self._device,
            )
        )

        # Extend all episodes by one artificual timestep to allow the value function net
        # to compute the bootstrap values (and add a mask to the batch to know, which
        # slots to mask out).
        if self.config.add_default_connectors_to_learner_pipeline:
            self._learner_connector.prepend(AddOneTsToEpisodesAndTruncate())

        # Create and start the GPU-loader thread. It picks up train-ready batches from
        # the "GPU-loader queue" and loads them to the GPU, then places the GPU batches
        # on the "update queue" for the actual RLModule forward pass and loss
        # computations.
        self._gpu_loader_in_queue = Queue()
        self._learner_thread_in_queue = Queue()
        self._learner_thread_out_queue = Queue()

        # Create and start the GPU loader thread.
        self._gpu_loader_thread = _GPULoaderThread(
            in_queue=self._gpu_loader_in_queue,
            out_queue=self._learner_thread_in_queue,
            device=self._device,
            metrics_logger=self.metrics,
        )
        self._gpu_loader_thread.start()

        # Create and start the Learner thread.
        self._learner_thread = _LearnerThread(
            update_method=self._update_from_batch_or_episodes,
            in_queue=self._learner_thread_in_queue,
            out_queue=self._learner_thread_out_queue,
            metrics_logger=self.metrics,
        )
        self._learner_thread.start()

    def update_from_episodes(
        self,
        episodes: List[EpisodeType],
        *,
        # TODO (sven): Deprecate these in favor of config attributes for only those
        #  algos that actually need (and know how) to do minibatching.
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        min_total_mini_batches: int = 0,
        reduce_fn=None,  # Deprecated args.
    ) -> ResultDict:
        # Resolve batch/episodes being ray object refs (instead of
        # actual batch/episodes objects).
        episodes = ray.get(episodes)
        episodes = tree.flatten(episodes)

        # Call the learner connector pipeline.
        batch = self._learner_connector(
            rl_module=self.module,
            data={},
            episodes=episodes,
            shared_data={},
        )
        # Convert to a batch (on the CPU).
        # TODO (sven): Try to not require MultiAgentBatch anymore.
        batch = MultiAgentBatch(
            {
                module_id: SampleBatch(module_data)
                for module_id, module_data in batch.items()
            },
            env_steps=sum(len(e) for e in episodes),
        )

        # Queue the CPU batch to the GPU-loader thread.
        self._gpu_loader_in_queue.put(batch)

        # Return all queued result dicts thus far (after reducing over them).
        results = {}
        try:
            while True:
                results = self._learner_thread_out_queue.get(block=False)
        except Empty:
            return results

    @override(Learner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.entropy_coeff_schedulers_per_module.pop(module_id)

    @override(Learner)
    def additional_update_for_module(
        self, *, module_id: ModuleID, config: ImpalaConfig, timestep: int
    ) -> None:
        super().additional_update_for_module(
            module_id=module_id, config=config, timestep=timestep
        )

        # Update entropy coefficient via our Scheduler.
        new_entropy_coeff = self.entropy_coeff_schedulers_per_module[module_id].update(
            timestep=timestep
        )
        self.metrics.log_value(
            (module_id, LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY),
            new_entropy_coeff,
            window=1,
        )


class _GPULoaderThread(threading.Thread):
    def __init__(
        self,
        *,
        in_queue: Queue,
        out_queue: Queue,
        device: torch.device,
        metrics_logger: MetricsLogger,
    ):
        super().__init__()
        self.daemon = True

        self._in_queue = in_queue
        self._out_queue = out_queue
        self._device = device
        self.metrics = metrics_logger

    def run(self) -> None:
        while True:
            self._step()

    def _step(self) -> None:
        # Get a new batch from the data (inqueue).
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_QUEUE_WAIT_TIMER)):
            ma_batch = self._in_queue.get()

        # Load the batch onto the GPU device.
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_LOAD_TO_GPU_TIMER)):
            ma_batch_on_gpu = ma_batch.to_device(self._device)
            self._out_queue.put(ma_batch_on_gpu)


class _LearnerThread(threading.Thread):
    def __init__(self, *, update_method, in_queue, out_queue, metrics_logger):
        super().__init__()
        self.daemon = True
        self.metrics = metrics_logger
        self.stopped = False

        self._update_method = update_method
        self._in_queue = in_queue
        self._out_queue = out_queue

    def run(self) -> None:
        while not self.stopped:
            self.step()

    def step(self):
        # Get a new batch from the GPU-data (inqueue).
        with self.metrics.log_time((ALL_MODULES, LEARNER_THREAD_IN_QUEUE_WAIT_TIMER)):
            ma_batch_on_gpu = self._in_queue.get()

        # Call the update method on the batch.
        with self.metrics.log_time((ALL_MODULES, LEARNER_THREAD_UPDATE_TIMER)):
            # TODO (sven): For multi-agent AND SGD iter > 1, we need to make sure
            #  this thread has the information about the min minibatches necessary
            #  (due to different agents taking different steps in the env, e.g.
            #  MA-CartPole).
            results = self._update_method(batch=ma_batch_on_gpu)
            self._out_queue.put(results)
