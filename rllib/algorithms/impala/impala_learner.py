import logging
import queue
import threading
from queue import Queue
from typing import Any, Dict, List

import ray
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

# Aggregation cycle size.
BATCHES_PER_AGGREGATION = 10

# Stop sentinel for the `_LearnerThread`
_STOP_SENTINEL = object()


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

        # Set the aggregation threshold to the broadcast interval. We return
        # a state at the same time the metrics are aggregated.
        global BATCHES_PER_AGGREGATION
        BATCHES_PER_AGGREGATION = self.config.broadcast_interval

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
        # Set the update kwargs passed in the main thread for use in the learner thread.
        self._update_kwargs = {}

        self._model_io_lock = threading.RLock()

        self._learner_state_queue = Queue(maxsize=1)
        self._learner_state_lock = threading.Lock()
        self._learner_state = None

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
        # TODO (simon): Do extensive testing to find an optimal queue size.
        loader_qsize = max(2, 10 * self.config.num_gpu_loader_threads)
        # Note, we are passing now the timesteps dictionary through the queue.
        self._gpu_loader_in_queue: "Queue[tuple[TrainingData, Dict[str, Any]]]" = Queue(
            maxsize=loader_qsize
        )

        # Learner in-queue must be tiny. 1 strictly serializes GPU-resident batches.
        # TODO (simon): Add a parameter to define queue size.
        if not hasattr(self, "_learner_thread_in_queue"):
            self._learner_thread_in_queue: "Queue[tuple[Any, Dict[str, Any]]]" = Queue(
                maxsize=self.config.learner_queue_size
            )

        # Get the rank of this learner, if necessary.
        self._rank: int = (
            torch.distributed.get_rank() if torch.distributed.is_initialized() else 0
        )

        # Define the out-queue for the metrics from the `_LearnerThread`.
        # TODO (simon): Add types for items.
        self._learner_thread_out_queue: "Queue[Dict[str, Any]]" = Queue()

        # Create and start `_GPULoaderThread`(s).
        if self.config.num_gpus_per_learner > 0:
            self._gpu_loader_threads: List[threading.Thread] = [
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
        self._learner_thread: threading.Thread = _LearnerThread(
            update_method=Learner.update,
            in_queue=self._learner_thread_in_queue,
            out_queue=self._learner_thread_out_queue,
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
        # Set the update kwargs passed in the main thread for use in the learner thread.
        self._update_kwargs = kwargs

        with TimerAndPrometheusLogger(self._metrics_learner_impala_update):
            # Get the train batch from the object store.
            with TimerAndPrometheusLogger(
                self._metrics_learner_impala_update_solve_refs
            ):
                # Resolve object refs and ensure we have a proper batch object.
                # TODO (simon): Check, if we can resolve the object references and
                # run the pipeline on the GPULoaderThreads.
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
            if (self._submitted_updates & 0xFF) == 0:
                self.metrics.log_value(
                    (ALL_MODULES, QUEUE_SIZE_GPU_LOADER_QUEUE),
                    self._gpu_loader_in_queue.qsize(),
                    window=1,
                )
                # TODO (simon): Check, if we want to get here stats from the
                # RingBuffer.
        else:
            # No GPU loader: directly enqueue to learner queue.
            _LearnerThread.enqueue(
                self._learner_thread_in_queue, (batch, timesteps), self.metrics
            )

        # Return the module state, if requested and available.
        if return_state:
            try:
                with self._learner_state_lock:
                    self._learner_state = self._learner_state_queue.get_nowait()
            except queue.Empty:
                logger.debug("No learner state available in the queue yet.")

        # Every 20th block call we submit results. Otherwise we keep the
        # thread running without interruption to avoid thread contention.
        self._submitted_updates += 1
        if (self._submitted_updates % BATCHES_PER_AGGREGATION) != 0:
            result = {}
            if return_state and self._learner_state:
                result["_rl_module_state_after_update"] = self._learner_state
            return result

        # Result submission: wait until learner finished BATCHES_PER_AGGREGATION updates (blocking).
        self._agg_event.wait()
        # Reset the aggregation event to keep the `_LearnerThread` running.
        self._agg_event.clear()

        if self._learner_thread_out_queue:
            try:
                result = self._learner_thread_out_queue.get(timeout=0.001)
            except queue.Empty:
                result = {}

        # Return the module state, if requested and existent.
        if return_state and self._learner_state:
            result["_rl_module_state_after_update"] = self._learner_state

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
        in_queue: "Queue[tuple[TrainingData, Dict[str, Any]]]",
        out_queue: "Queue[tuple[Any, Dict[str, Any]]]",
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


class _LearnerThread(threading.Thread):
    def __init__(
        self,
        *,
        update_method,
        in_queue: "Queue[tuple[Any, Dict[str, Any]]]",
        out_queue: "Queue[Dict[str, Any]]",
        learner: IMPALALearner,
    ):
        super().__init__(name="_LearnerThread")
        self.daemon = True
        self.learner = learner
        self._update_method = update_method
        # Note, we pass now the timesteps dictionary through the queue.
        self._in_queue: "queue.Queue[tuple[Any, Dict[str, Any]]]" = in_queue
        # TODO (simon): Type hints.
        self._out_queue = out_queue
        self._stop_event = threading.Event()

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
            logger.warning(
                "_LearnerThread.request_stop(): in_queue is full; cannot enqueue stop sentinel."
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
                logger.warning(
                    "_LearnerThread._in_queue.task_done() failed during stop handling."
                )
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
                    # Include the learner update kwargs set in the main thread.
                    **self.learner._update_kwargs,
                )

        # Signal queue done (unblocks producer’s put when bounded)
        try:
            self._in_queue.task_done()
        finally:
            # Set the Aggregation counter and signal this event (atomic).
            with self.learner._num_updates_lock:
                self.learner._num_updates += 1
                # Check, if we need to aggregate.
                do_agg = self.learner._num_updates == BATCHES_PER_AGGREGATION
                if do_agg:
                    # Reset the update counter inside the lock.
                    self.learner._num_updates = 0

            # If we need to aggregate, reduce metrics and queue them.
            if do_agg:
                # If in multi-learner setup, safeguard state retrieval within barriers.
                if torch.distributed.is_initialized():
                    torch.distributed.barrier()
                # Only the first rank retrieves the state.
                if self.learner._rank == 0:
                    with self.learner._model_io_lock, torch.inference_mode():
                        learner_state = self.learner.get_state(
                            # Only return the state of those RLModules that are trainable.
                            components=[
                                COMPONENT_RL_MODULE + "/" + mid
                                for mid in self.learner.module.keys()
                                if self.learner.should_module_be_updated(mid)
                            ],
                            inference_only=True,
                        )
                        learner_state[COMPONENT_RL_MODULE] = ray.put(
                            learner_state[COMPONENT_RL_MODULE]
                        )
                    try:
                        if (self.learner._submitted_updates & ~0xFF) != (
                            (self.learner._submitted_updates - BATCHES_PER_AGGREGATION)
                            & ~0xFF
                        ):
                            with self.learner._learner_state_lock:
                                self.learner.metrics.log_value(
                                    (ALL_MODULES, "learner_thread_state_queue_size"),
                                    self.learner._learner_state_queue.qsize(),
                                    window=1,
                                )
                        # Remove any old learner state in the queue.
                        self.learner._learner_state_queue.get_nowait()
                    except queue.Empty:
                        logger.debug("No old learner state to remove from the queue.")

                    # Pass the learner state into the queue to the main process.
                    self.learner._learner_state_queue.put_nowait(learner_state)
                self.learner.metrics.log_value(
                    (ALL_MODULES, "learner_thread_out_queue_size"),
                    self._out_queue.qsize(),
                    window=1,
                )

                # Reduce metrics and pass them into the queue for the main process.
                self._out_queue.put(self.learner.metrics.reduce())
                # Notify all listeners that aggregation is done and results can be
                # retrieved.
                self.learner._agg_event.set()
                if torch.distributed.is_initialized():
                    torch.distributed.barrier()

        # Keep running (see `run` method).
        return True

    @staticmethod
    def enqueue(
        learner_queue: "queue.Queue[tuple[Any, Dict[str, Any]]]",
        batch_with_ts,
        metrics: MetricsLogger,
    ):
        # Put the batch into the queue (blocking if thread is updating).
        learner_queue.put(batch_with_ts, block=True)
