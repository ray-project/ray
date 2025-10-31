import logging
import queue
import threading
from typing import Any, Dict

import ray
from ray.rllib.algorithms.impala.impala import LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
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

# Aggregation cycle size.
BATCHES_PER_AGGREGATION = 20


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

        # APPO/IMPALA require RLock for thread safety around metrics.
        self.metrics._threading_lock = threading.RLock()

        # Aggregation signaling (replaces condition-variable contention) ---
        self._agg_event = threading.Event()
        self._submitted_updates = 0  # producer-side counter (update thread(s))
        self._num_updates = 0  # learner-side counter
        self._num_updates_lock = threading.Lock()

        # Dict mapping module IDs to entropy Scheduler instances.
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

        # Create queues as bounded queues to create real back-pressure & stabilize
        # GPU memory usage.

        # Small loader in-queue to keep threads busy without flooding.
        loader_qsize = max(2, 2 * self.config.num_gpu_loader_threads)
        # Note, we are passing now the timesteps dictionary through the queue.
        self._gpu_loader_in_queue: "queue.Queue[tuple[TrainingData, Dict[str, Any]]]" = queue.Queue(
            maxsize=loader_qsize
        )

        # Learner in-queue must be tiny. 1 strictly serializes GPU-resident batches.
        self._learner_thread_in_queue: "queue.Queue[tuple[Any, Dict[str, Any]]]" = (
            queue.Queue(maxsize=2)
        )

        # Create and start `_GPULoaderThread`(s).
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

        # Create and start the `_LearnerThread`.
        self._learner_thread = _LearnerThread(
            update_method=Learner.update,
            in_queue=self._learner_thread_in_queue,
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

        # Enqeue the batch (bounded backpressure).
        if self.config.num_gpus_per_learner > 0:
            # Pass timesteps alongside batch (no globals).
            self._gpu_loader_in_queue.put((batch, timesteps))
            # Only occasionally log loader queue size.
            if (self._submitted_updates & 0x3FF) == 0:
                self.metrics.log_value(
                    (ALL_MODULES, QUEUE_SIZE_GPU_LOADER_QUEUE),
                    self._gpu_loader_in_queue.qsize(),
                    window=1,
                )
        else:
            # No GPU loader: directly enqueue to learner queue.
            _LearnerThread.enqueue(
                self._learner_thread_in_queue, (batch, timesteps), self.metrics
            )

        # Every 20th block call we submit results. Otherwise we keep the
        # thread running without interruption to avoid thread contention.
        self._submitted_updates += 1
        if (self._submitted_updates % BATCHES_PER_AGGREGATION) != 0:
            return {}

        # 20th submission: wait until learner finished 20 updates (blocking).
        self._agg_event.wait()
        # Reset the aggregation event to keep the `_LearnerThread` running.
        self._agg_event.clear()

        # Reduce metrics outside of any locks.
        # TODO (simon): Check, if we need to call this before we call `clear`.
        result = self.metrics.reduce()

        # Return the module state, if requested.
        if return_state:
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
        in_queue: "queue.Queue[tuple[TrainingData, Dict[str, Any]]]",
        out_queue: "queue.Queue[tuple[Any, Dict[str, Any]]]",
        device: "torch.device",
        metrics_logger: MetricsLogger,
    ):
        super().__init__(name="_GPULoaderThread")
        self.daemon = True

        self._in_queue = in_queue
        self._out_queue = out_queue
        self._device = device
        self.metrics = metrics_logger

        # Use a single CUDA stream for each loader thread.
        self._use_cuda_stream = (
            torch is not None
            and hasattr(torch, "cuda")
            and device is not None
            and getattr(device, "type", None) == "cuda"
        )
        self._stream = (
            torch.cuda.Stream(device=self._device) if self._use_cuda_stream else None
        )

    # Robust pinned-memory copy: fall back if batch contains CUDA tensors already.
    # TODO (simon): Find a more compliant solution.
    def _to_device_safe(self, batch):
        try:
            return batch.to_device(self._device, pin_memory=True)
        except RuntimeError as e:
            msg = str(e)
            if "only dense CPU tensors can be pinned" in msg or "pin_memory" in msg:
                return batch.to_device(self._device, pin_memory=False)
            raise

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
        # Get a new batch (CPU) and the global timesteps from the loader in--queue (blocking).
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_QUEUE_WAIT_TIMER)):
            with TimerAndPrometheusLogger(
                self._metrics_impala_gpu_loader_thread_step_in_queue_get_time
            ):
                ma_batch_on_cpu, timesteps = self._in_queue.get()

        # Load the batch onto the GPU device; enable pinned memory for async copies.
        with self.metrics.log_time((ALL_MODULES, GPU_LOADER_LOAD_TO_GPU_TIMER)):
            if self._use_cuda_stream and self._stream is not None:
                # Issue copies on a non-default stream so they can overlap with compute.
                with torch.cuda.stream(self._stream):
                    ma_batch_on_gpu = self._to_device_safe(ma_batch_on_cpu)
                    # TODO (simon): Maybe use the `use_stream` in `convert_to_tensor`.
                # No explicit synching here. Consumer will naturally serialize when needed.
            else:
                ma_batch_on_gpu = self._to_device_safe(ma_batch_on_cpu)

        # Enqueue to Learner thread’s in-queue (GPU-resident batch and global timesteps).
        _LearnerThread.enqueue(
            self._out_queue, (ma_batch_on_gpu, timesteps), self.metrics
        )


# Put this once near the top of the file (module-level):
_STOP_SENTINEL = object()


class _LearnerThread(threading.Thread):
    def __init__(
        self,
        *,
        update_method,
        in_queue: "queue.Queue[tuple[Any, Dict[str, Any]]]",
        learner: IMPALALearner,
    ):
        super().__init__(name="_LearnerThread")
        self.daemon = True
        self.learner = learner
        self._update_method = update_method
        # Note, we pass now the timesteps dictionary through the queue.
        self._in_queue: "queue.Queue[tuple[Any, Dict[str, Any]]]" = in_queue

        self._stop_event = threading.Event()

    # Keeps compatibility, but thread-safe.
    @property
    def stopped(self) -> bool:
        return self._stop_event.is_set()

    # Call this to stop the thread and wake it if it's blocked on .get()
    def request_stop(self) -> None:
        self._stop_event.set()
        # Wake the consumer if it's blocked on an empty queue
        try:
            self._in_queue.put_nowait(_STOP_SENTINEL)
        except queue.Full:
            # If the queue is full, the consumer will wake soon anyway.
            pass

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
        while True:
            # Returns always `True` until stop-signal/sentinel is sent.
            if not self.step():
                break

    def step(self) -> bool:
        # Get a batch and wait, if the input queue is empty (blocking; no polling).
        with self.learner.metrics.log_time(
            (ALL_MODULES, LEARNER_THREAD_IN_QUEUE_WAIT_TIMER)
        ):
            item = self._in_queue.get()

        # Handle the stop/sentinel signal(s).
        # TODO (simon): Check, if we need `None` for belt-and-suspenders/comp.
        if item is _STOP_SENTINEL or self.stopped:
            try:
                self._in_queue.task_done()
            except Exception:
                pass
            # Signal `run` to exit.
            return False

        # Extract the multi-agent batch and the timesteps dictionary.
        ma_batch_on_gpu, timesteps = item

        # Update the `RLModule`, but do not reduce metrics.
        with self.learner.metrics.log_time((ALL_MODULES, LEARNER_THREAD_UPDATE_TIMER)):
            with TimerAndPrometheusLogger(
                self._metrics_learner_impala_thread_step_update
            ):
                self._update_method(
                    self=self.learner,
                    training_data=TrainingData(batch=ma_batch_on_gpu),
                    timesteps=timesteps,
                    _no_metrics_reduce=True,
                )

        # Signal queue done (unblocks producer’s put when bounded)
        try:
            self._in_queue.task_done()
        finally:
            # Set the Aggregation counter and signal this event (atomic).
            with self.learner._num_updates_lock:
                self.learner._num_updates += 1
                if self.learner._num_updates == BATCHES_PER_AGGREGATION:
                    self.learner._num_updates = 0
                    self.learner._agg_event.set()

        # Keep running (see `run` method).
        return True

    @staticmethod
    def enqueue(
        learner_queue: "queue.Queue[tuple[Any, Dict[str, Any]]]",
        batch_with_ts,
        metrics: MetricsLogger,
    ):
        try:
            # Put the batch into the queue (blocking if thread is updating).
            learner_queue.put(batch_with_ts, block=True)
        except queue.Full:
            # TODO (simon): Write mechanism to drop the oldest batch.
            batch, _ts = batch_with_ts
            try:
                env_steps = batch.env_steps() if hasattr(batch, "env_steps") else 0
                metrics.log_value(
                    (ALL_MODULES, LEARNER_THREAD_ENV_STEPS_DROPPED),
                    env_steps,
                    reduce="sum",
                )
            except Exception:
                pass
            return
