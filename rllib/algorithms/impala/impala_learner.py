import logging
import queue
import threading
import time
from collections import deque
from typing import Any, Dict, Union

import ray
from ray.rllib.algorithms.appo.utils import CircularBuffer
from ray.rllib.algorithms.impala.impala import LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.metrics.ray_metrics import (
    DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
    TimerAndPrometheusLogger,
)
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ModuleID, ResultDict
from ray.util.metrics import Gauge, Histogram

logger = logging.getLogger(__name__)

torch, _ = try_import_torch()

GPU_LOADER_QUEUE_WAIT_TIMER = "gpu_loader_queue_wait_timer"
GPU_LOADER_LOAD_TO_GPU_TIMER = "gpu_loader_load_to_gpu_timer"
LEARNER_THREAD_IN_QUEUE_WAIT_TIMER = "learner_thread_in_queue_wait_timer"
LEARNER_THREAD_ENV_STEPS_DROPPED = "learner_thread_env_steps_dropped"
LEARNER_THREAD_UPDATE_TIMER = "learner_thread_update_timer"
RAY_GET_EPISODES_TIMER = "ray_get_episodes_timer"

QUEUE_SIZE_GPU_LOADER_QUEUE = "queue_size_gpu_loader_queue"
QUEUE_SIZE_LEARNER_THREAD_QUEUE = "queue_size_learner_thread_queue"
QUEUE_SIZE_RESULTS_QUEUE = "queue_size_results_queue"

_CURRENT_GLOBAL_TIMESTEPS = None


class IMPALALearner(Learner):
    @override(Learner)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Ray metrics
        self._metrics_learner_impala_update = Histogram(
            name="rllib_learner_impala_update_time",
            description="Time spent in the 'IMPALALearner.update()' method.",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_learner_impala_update.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_learner_impala_update_solve_refs = Histogram(
            name="rllib_learner_impala_update_solve_refs_time",
            description="Time spent on resolving refs in the 'Learner.update()'",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_learner_impala_update_solve_refs.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_learner_impala_update_make_batch_if_necessary = Histogram(
            name="rllib_learner_impala_update_make_batch_if_necessary_time",
            description="Time spent on making a batch in the 'Learner.update()'.",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_learner_impala_update_make_batch_if_necessary.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_learner_impala_get_learner_state_time = Histogram(
            name="rllib_learner_impala_get_learner_state_time",
            description="Time spent on get_state() in IMPALALearner.update().",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_learner_impala_get_learner_state_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

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
        self._num_updates = 0
        self._num_updates_lock = threading.Lock()

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

        # Create and start the GPU-loader thread. It picks up train-ready batches from
        # the "GPU-loader queue" and loads them to the GPU, then places the GPU batches
        # on the "update queue" for the actual RLModule forward pass and loss
        # computations.
        self._gpu_loader_in_queue = queue.Queue()

        # Default is to have a learner thread.
        if not hasattr(self, "_learner_thread_in_queue"):
            self._learner_thread_in_queue = deque(maxlen=self.config.learner_queue_size)

        # TODO (sven): Figure out a way to use a results queue instaad of the "reduce
        #  metrics each 20 updates" logic right now.
        # # Results queue for reduced Learner metrics.
        # # self._learner_thread_out_queue = deque(maxlen=1)

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
            update_method=Learner.update,
            in_queue=self._learner_thread_in_queue,
            # TODO (sven): Figure out a way to use a results queue instaad of the "reduce
            #  metrics each 20 updates" logic right now.
            # out_queue=self._learner_thread_out_queue,
            learner=self,
        )
        self._learner_thread.start()

    @override(Learner)
    def update(
        self,
        training_data: TrainingData,
        *,
        timesteps: Dict[str, Any],
        return_state: bool = False,
        **kwargs,
    ) -> ResultDict:
        """

        Args:
            batch:
            timesteps:
            return_state: Whether to include one of the Learner worker's state from
                after the update step in the returned results dict (under the
                `_rl_module_state_after_update` key). Note that after an update, all
                Learner workers' states should be identical, so we use the first
                Learner's state here. Useful for avoiding an extra `get_weights()` call,
                e.g. for synchronizing EnvRunner weights.
            **kwargs:

        Returns:

        """
        global _CURRENT_GLOBAL_TIMESTEPS
        _CURRENT_GLOBAL_TIMESTEPS = timesteps or {}

        with TimerAndPrometheusLogger(self._metrics_learner_impala_update):
            # Get the train batch from the object store.
            with TimerAndPrometheusLogger(
                self._metrics_learner_impala_update_solve_refs
            ):
                training_data.solve_refs()

            with TimerAndPrometheusLogger(
                self._metrics_learner_impala_update_make_batch_if_necessary
            ):
                batch = self._make_batch_if_necessary(training_data=training_data)
                assert batch is not None

            if self.config.num_gpus_per_learner > 0:
                self._gpu_loader_in_queue.put(batch)
                self.metrics.log_value(
                    (ALL_MODULES, QUEUE_SIZE_GPU_LOADER_QUEUE),
                    self._gpu_loader_in_queue.qsize(),
                )
            else:
                if isinstance(self._learner_thread_in_queue, CircularBuffer):
                    ts_dropped = self._learner_thread_in_queue.add(batch)
                    self.metrics.log_value(
                        (ALL_MODULES, LEARNER_THREAD_ENV_STEPS_DROPPED),
                        ts_dropped,
                        reduce="sum",
                    )
                else:
                    # Enqueue to Learner thread's in-queue.
                    _LearnerThread.enqueue(
                        self._learner_thread_in_queue, batch, self.metrics
                    )

            # TODO (sven): Find a better way to limit the number of (mostly) unnecessary
            #  metrics reduces.
            with self._num_updates_lock:
                count = self._num_updates
            result = {}

            if count >= 20:
                with self._num_updates_lock:
                    self._num_updates = 0
                result = self.metrics.reduce()

            if return_state:
                with TimerAndPrometheusLogger(
                    self._metrics_learner_impala_get_learner_state_time
                ):
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
                    result["_rl_module_state_after_update"] = learner_state

        return result

        # TODO (sven): Figure out a way to use a results queue instaad of the "reduce
        #  metrics each 20 updates" logic right now.
        # try:
        #    result = self._learner_thread_out_queue.popleft()
        # except IndexError:
        #    result = {}

        # if return_state:
        #    learner_state = self.get_state(
        #        # Only return the state of those RLModules that are trainable.
        #        components=[
        #            COMPONENT_RL_MODULE + "/" + mid
        #            for mid in self.module.keys()
        #            if self.should_module_be_updated(mid)
        #        ],
        #        inference_only=True,
        #    )
        #    learner_state[COMPONENT_RL_MODULE] = ray.put(
        #        learner_state[COMPONENT_RL_MODULE]
        #    )
        #    result["_rl_module_state_after_update"] = learner_state

        # return result

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

    @classmethod
    @override(Learner)
    def rl_module_required_apis(cls) -> list[type]:
        # In order for a PPOLearner to update an RLModule, it must implement the
        # following APIs:
        return [ValueFunctionAPI]


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
        super().__init__(name="_GPULoaderThread")
        self.daemon = True

        self._in_queue = in_queue
        self._out_queue = out_queue
        self._ts_dropped = 0
        self._device = device
        self.metrics = metrics_logger

        self._metrics_impala_gpu_loader_thread_step_time = Histogram(
            name="rllib_learner_impala_gpu_loader_thread_step_time",
            description="Time taken in seconds for gpu loader thread _step.",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_impala_gpu_loader_thread_step_time.set_default_tags(
            {"rllib": "IMPALA/GPULoaderThread"}
        )

        self._metrics_impala_gpu_loader_thread_step_in_queue_get_time = Histogram(
            name="rllib_learner_impala_gpu_loader_thread_step_get_time",
            description="Time taken in seconds for gpu loader thread _step _in_queue.get().",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_impala_gpu_loader_thread_step_in_queue_get_time.set_default_tags(
            {"rllib": "IMPALA/GPULoaderThread"}
        )

        self._metrics_impala_gpu_loader_thread_step_load_to_gpu_time = Histogram(
            name="rllib_learner_impala_gpu_loader_thread_step_load_to_gpu_time",
            description="Time taken in seconds for GPU loader thread _step to load batch to GPU.",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_impala_gpu_loader_thread_step_load_to_gpu_time.set_default_tags(
            {"rllib": "IMPALA/GPULoaderThread"}
        )

        self._metrics_impala_gpu_loader_thread_in_qsize_beginning_of_step = Gauge(
            name="rllib_impala_gpu_loader_thread_in_qsize_beginning_of_step",
            description="Size of the _GPULoaderThread in-queue size, at the beginning of the step.",
            tag_keys=("rllib",),
        )
        self._metrics_impala_gpu_loader_thread_in_qsize_beginning_of_step.set_default_tags(
            {"rllib": "IMPALA/GPULoaderThread"}
        )

    def run(self) -> None:
        while True:
            with TimerAndPrometheusLogger(
                self._metrics_impala_gpu_loader_thread_step_time
            ):
                self._step()

    def _step(self) -> None:
        self._metrics_impala_gpu_loader_thread_in_qsize_beginning_of_step.set(
            value=self._in_queue.qsize()
        )
        # Get a new batch from the data (in-queue).
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_QUEUE_WAIT_TIMER)):
            with TimerAndPrometheusLogger(
                self._metrics_impala_gpu_loader_thread_step_in_queue_get_time
            ):
                ma_batch_on_cpu = self._in_queue.get()

        # Load the batch onto the GPU device.
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_LOAD_TO_GPU_TIMER)):
            with TimerAndPrometheusLogger(
                self._metrics_impala_gpu_loader_thread_step_load_to_gpu_time
            ):
                ma_batch_on_gpu = ma_batch_on_cpu.to_device(
                    self._device, pin_memory=False
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
        in_queue: Union[deque, CircularBuffer],
        # TODO (sven): Figure out a way to use a results queue instaad of the "reduce
        #  metrics each 20 updates" logic right now.
        # out_queue: deque,
        learner,
    ):
        super().__init__(name="_LearnerThread")
        self.daemon = True
        self.learner = learner
        self.stopped = False

        self._update_method = update_method
        self._in_queue: Union[deque, CircularBuffer] = in_queue
        # TODO (sven): Figure out a way to use a results queue instaad of the "reduce
        #  metrics each 20 updates" logic right now.
        # self._out_queue: deque = out_queue

        # Ray metrics
        self._metrics_learner_impala_thread_step = Histogram(
            name="rllib_learner_impala_learner_thread_step_time",
            description="Time taken in seconds for learner thread _step.",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_learner_impala_thread_step.set_default_tags(
            {"rllib": "IMPALA/LearnerThread"}
        )

        self._metrics_learner_impala_thread_step_update = Histogram(
            name="rllib_learner_impala_learner_thread_step_update_time",
            description="Time taken in seconds for learner thread _step update.",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_learner_impala_thread_step_update.set_default_tags(
            {"rllib": "IMPALA/LearnerThread"}
        )

    def run(self) -> None:
        while not self.stopped:
            with TimerAndPrometheusLogger(self._metrics_learner_impala_thread_step):
                self.step()

    def step(self):
        global _CURRENT_GLOBAL_TIMESTEPS

        # Get a new batch from the GPU-data (deque.pop -> newest item first).
        with self.learner.metrics.log_time(
            (ALL_MODULES, LEARNER_THREAD_IN_QUEUE_WAIT_TIMER)
        ):
            # Get a new batch from the GPU-data (learner queue OR circular buffer).
            if isinstance(self._in_queue, CircularBuffer):
                ma_batch_on_gpu = self._in_queue.sample()
            else:
                # Queue is empty: Sleep a tiny bit to avoid CPU-thrashing.
                while not self._in_queue:
                    time.sleep(0.0001)
                # Consume from the left (oldest batches first).
                # If we consumed from the right, we would run into the danger of
                # learning from newer batches (left side) most times, BUT sometimes
                # grabbing older batches (right area of deque).
                ma_batch_on_gpu = self._in_queue.popleft()

        # Add this check here in case thread has been stopped while we were waiting for
        # a batch from the queue/buffer.
        if self.stopped:
            return

        # Call the update method on the batch.
        with self.learner.metrics.log_time((ALL_MODULES, LEARNER_THREAD_UPDATE_TIMER)):
            # TODO (sven): For multi-agent AND SGD iter > 1, we need to make sure
            #  this thread has the information about the min minibatches necessary
            #  (due to different agents taking different steps in the env, e.g.
            #  MA-CartPole).
            with TimerAndPrometheusLogger(
                self._metrics_learner_impala_thread_step_update
            ):
                self._update_method(
                    self=self.learner,
                    training_data=TrainingData(batch=ma_batch_on_gpu),
                    timesteps=_CURRENT_GLOBAL_TIMESTEPS,
                    _no_metrics_reduce=True,
                )
            # TODO (sven): Figure out a way to use a results queue instaad of the "reduce
            #  metrics each 20 updates" logic right now.
            # self._out_queue.append(results)
            with self.learner._num_updates_lock:
                self.learner._num_updates += 1

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
