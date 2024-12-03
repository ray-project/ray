from collections import deque
import queue
import threading
import time
from typing import Any, Dict, List, Union

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.appo.utils import CircularBuffer
from ray.rllib.algorithms.impala.impala import LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import Learner
from ray.rllib.connectors.common import NumpyToTensor
from ray.rllib.connectors.learner import AddOneTsToEpisodesAndTruncate
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import EpisodeType, ModuleID, ResultDict

torch, _ = try_import_torch()

GPU_LOADER_QUEUE_WAIT_TIMER = "gpu_loader_queue_wait_timer"
GPU_LOADER_LOAD_TO_GPU_TIMER = "gpu_loader_load_to_gpu_timer"
LEARNER_THREAD_IN_QUEUE_WAIT_TIMER = "learner_thread_in_queue_wait_timer"
LEARNER_THREAD_ENV_STEPS_DROPPED = "learner_thread_env_steps_dropped"
LEARNER_THREAD_UPDATE_TIMER = "learner_thread_update_timer"
RAY_GET_EPISODES_TIMER = "ray_get_episodes_timer"
EPISODES_TO_BATCH_TIMER = "episodes_to_batch_timer"

QUEUE_SIZE_GPU_LOADER_QUEUE = "queue_size_gpu_loader_queue"
QUEUE_SIZE_LEARNER_THREAD_QUEUE = "queue_size_learner_thread_queue"
QUEUE_SIZE_RESULTS_QUEUE = "queue_size_results_queue"

_CURRENT_GLOBAL_TIMESTEPS = None


class IMPALALearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # TODO (sven): We replace the dummy RLock here for APPO/IMPALA, b/c these algos
        #  require this for thread safety reasons.
        #  An RLock breaks our current OfflineData and OfflinePreLearner logic, in which
        #  the Learner (which contains a MetricsLogger) is serialized and deserialized.
        #  We will have to fix this offline RL logic first, then can remove this hack
        #  here and return to always using the RLock.
        self.metrics._threading_lock = threading.RLock()

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

        # Extend all episodes by one artificial timestep to allow the value function net
        # to compute the bootstrap values (and add a mask to the batch to know, which
        # slots to mask out).
        if (
            self._learner_connector is not None
            and self.config.add_default_connectors_to_learner_pipeline
        ):
            self._learner_connector.prepend(AddOneTsToEpisodesAndTruncate())
            # Leave all batches on the CPU (they'll be moved to the GPU, if applicable,
            # by the n GPU loader threads).
            numpy_to_tensor_connector = self._learner_connector[NumpyToTensor][0]
            numpy_to_tensor_connector._device = "cpu"  # TODO (sven): Provide API?

        # Create and start the GPU-loader thread. It picks up train-ready batches from
        # the "GPU-loader queue" and loads them to the GPU, then places the GPU batches
        # on the "update queue" for the actual RLModule forward pass and loss
        # computations.
        self._gpu_loader_in_queue = queue.Queue()
        # Default is to have a learner thread.
        if not hasattr(self, "_learner_thread_in_queue"):
            self._learner_thread_in_queue = deque(maxlen=self.config.learner_queue_size)

        # Create and start the GPU loader thread(s).
        if self.config.num_gpus_per_learner > 0:
            self._gpu_loader_threads = [
                _GPULoaderThread(
                    in_queue=self._gpu_loader_in_queue,
                    out_queue=self._learner_thread_in_queue,
                    device=self._device,
                    metrics_logger=self.metrics,
                )
                for _ in range(self.config.num_gpu_loader_threads)
            ]
            for t in self._gpu_loader_threads:
                t.start()

        # Create and start the Learner thread.
        self._learner_thread = _LearnerThread(
            update_method=self._update_from_batch_or_episodes,
            in_queue=self._learner_thread_in_queue,
            metrics_logger=self.metrics,
        )
        self._learner_thread.start()

    @override(Learner)
    def update_from_episodes(
        self,
        episodes: List[EpisodeType],
        *,
        timesteps: Dict[str, Any],
        **kwargs,
    ) -> ResultDict:
        global _CURRENT_GLOBAL_TIMESTEPS
        _CURRENT_GLOBAL_TIMESTEPS = timesteps

        # TODO (sven): IMPALA does NOT call additional update anymore from its
        #  `training_step()` method. Instead, we'll do this here (to avoid the extra
        #  metrics.reduce() call -> we should only call this once per update round).
        self.before_gradient_based_update(timesteps=timesteps)

        with self.metrics.log_time((ALL_MODULES, RAY_GET_EPISODES_TIMER)):
            # Resolve batch/episodes being ray object refs (instead of
            # actual batch/episodes objects).
            # If this fails, it might be because some of the EnvRunners that collected
            # `episodes` are down (ex. SPOT preemption or single EnvRunner crash).
            # In this case, we should ignore those List[EpisodeType] references and not
            # use these for the train batch.
            try:
                episodes = ray.get(episodes)
                episodes_flat = tree.flatten(episodes)
            except ray.exceptions.RayError:
                # Try unreferencing one by one and collect those that are ok.
                episodes_flat = []
                for e in episodes:
                    try:
                        episodes_flat.extend(ray.get(e))
                    # Ignore exceptions and move on with other references.
                    except Exception:
                        pass

            env_steps = sum(map(len, episodes_flat))

        # Only send a batch to the learner pipeline if its size is > 0.
        if env_steps > 0:
            # Call the learner connector pipeline.
            with self.metrics.log_time((ALL_MODULES, EPISODES_TO_BATCH_TIMER)):
                batch = self._learner_connector(
                    rl_module=self.module,
                    batch={},
                    episodes=episodes_flat,
                    shared_data={},
                )

            # Queue the CPU batch to the GPU-loader thread.
            if self.config.num_gpus_per_learner > 0:
                self._gpu_loader_in_queue.put((batch, env_steps))
                self.metrics.log_value(
                    (ALL_MODULES, QUEUE_SIZE_GPU_LOADER_QUEUE),
                    self._gpu_loader_in_queue.qsize(),
                )
            # Add the batch directly to the learner queue (or circular buffer).
            else:
                ma_batch = MultiAgentBatch(
                    {mid: SampleBatch(b) for mid, b in batch.items()},
                    env_steps=env_steps,
                )
                if isinstance(self._learner_thread_in_queue, CircularBuffer):
                    ts_dropped = self._learner_thread_in_queue.add(ma_batch)
                    self.metrics.log_value(
                        (ALL_MODULES, LEARNER_THREAD_ENV_STEPS_DROPPED),
                        ts_dropped,
                        reduce="sum",
                    )
                else:
                    # Enqueue to Learner thread's in-queue.
                    _LearnerThread.enqueue(
                        self._learner_thread_in_queue, ma_batch, self.metrics
                    )

        return self.metrics.reduce()

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def before_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        super().before_gradient_based_update(timesteps=timesteps)

        for module_id in self.module.keys():
            # Update entropy coefficient via our Scheduler.
            new_entropy_coeff = self.entropy_coeff_schedulers_per_module[
                module_id
            ].update(timestep=timesteps.get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0))
            self.metrics.log_value(
                (module_id, LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY),
                new_entropy_coeff,
                window=1,
            )

    @override(Learner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.entropy_coeff_schedulers_per_module.pop(module_id)


ImpalaLearner = IMPALALearner


class _GPULoaderThread(threading.Thread):
    def __init__(
        self,
        *,
        in_queue: queue.Queue,
        out_queue: deque,
        device: torch.device,
        metrics_logger: MetricsLogger,
    ):
        super().__init__()
        self.daemon = True

        self._in_queue = in_queue
        self._out_queue = out_queue
        self._ts_dropped = 0
        self._device = device
        self.metrics = metrics_logger

    def run(self) -> None:
        while True:
            self._step()

    def _step(self) -> None:
        # Only measure time, if we have a `metrics` instance.
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_QUEUE_WAIT_TIMER)):
            # Get a new batch from the data (inqueue).
            batch_on_cpu, env_steps = self._in_queue.get()

        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_LOAD_TO_GPU_TIMER)):
            # Load the batch onto the GPU device.
            batch_on_gpu = tree.map_structure_with_path(
                lambda path, t: (
                    t
                    if isinstance(path, tuple) and Columns.INFOS in path
                    else t.to(self._device, non_blocking=True)
                ),
                batch_on_cpu,
            )
            ma_batch_on_gpu = MultiAgentBatch(
                policy_batches={mid: SampleBatch(b) for mid, b in batch_on_gpu.items()},
                env_steps=env_steps,
            )

            if isinstance(self._out_queue, CircularBuffer):
                ts_dropped = self._out_queue.add(ma_batch_on_gpu)
                self.metrics.log_value(
                    (ALL_MODULES, LEARNER_THREAD_ENV_STEPS_DROPPED),
                    ts_dropped,
                    reduce="sum",
                )
            else:
                # Enqueue to Learner thread's in-queue.
                _LearnerThread.enqueue(self._out_queue, ma_batch_on_gpu, self.metrics)


class _LearnerThread(threading.Thread):
    def __init__(
        self,
        *,
        update_method,
        in_queue: deque,
        metrics_logger,
    ):
        super().__init__()
        self.daemon = True
        self.metrics: MetricsLogger = metrics_logger
        self.stopped = False

        self._update_method = update_method
        self._in_queue: Union[deque, CircularBuffer] = in_queue

    def run(self) -> None:
        while not self.stopped:
            self.step()

    def step(self):
        global _CURRENT_GLOBAL_TIMESTEPS

        # Get a new batch from the GPU-data (deque.pop -> newest item first).
        with self.metrics.log_time((ALL_MODULES, LEARNER_THREAD_IN_QUEUE_WAIT_TIMER)):
            # Get a new batch from the GPU-data (learner queue OR circular buffer).
            if isinstance(self._in_queue, CircularBuffer):
                ma_batch_on_gpu = self._in_queue.sample()
            else:
                # Queue is empty: Sleep a tiny bit to avoid CPU-thrashing.
                if not self._in_queue:
                    time.sleep(0.001)
                    return
                # Consume from the left (oldest batches first).
                # If we consumed from the right, we would run into the danger of
                # learning from newer batches (left side) most times, BUT sometimes
                # grabbing older batches (right area of deque).
                ma_batch_on_gpu = self._in_queue.popleft()

        # Call the update method on the batch.
        with self.metrics.log_time((ALL_MODULES, LEARNER_THREAD_UPDATE_TIMER)):
            # TODO (sven): For multi-agent AND SGD iter > 1, we need to make sure
            #  this thread has the information about the min minibatches necessary
            #  (due to different agents taking different steps in the env, e.g.
            #  MA-CartPole).
            self._update_method(
                batch=ma_batch_on_gpu,
                timesteps=_CURRENT_GLOBAL_TIMESTEPS,
            )

    @staticmethod
    def enqueue(learner_queue: deque, batch, metrics):
        # Right-append to learner queue (a deque). If full, drops the leftmost
        # (oldest) item in the deque.
        # Note that we consume from the left (oldest first), which is why the queue size
        # should probably always be small'ish (<< 10), otherwise we run into the danger
        # of training with very old samples.
        # If we consumed from the right, we would run into the danger of learning
        # from newer batches (left side) most times, BUT sometimes grabbing a
        # really old batches (right area of deque).
        if len(learner_queue) == learner_queue.maxlen:
            metrics.log_value(
                (ALL_MODULES, LEARNER_THREAD_ENV_STEPS_DROPPED),
                learner_queue.popleft().env_steps(),
                reduce="sum",
            )
        learner_queue.append(batch)

        # Log current queue size.
        metrics.log_value(
            (ALL_MODULES, QUEUE_SIZE_LEARNER_THREAD_QUEUE),
            len(learner_queue),
        )
