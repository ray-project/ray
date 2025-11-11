import logging
import queue
import threading

from ray._common.deprecation import deprecation_warning
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.minibatch_buffer import MinibatchBuffer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.util.timer import _Timer

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


@OldAPIStack
class MultiGPULearnerThread(LearnerThread):
    """Learner that can use multiple GPUs and parallel loading.

    This class is used for async sampling algorithms.

    Example workflow: 2 GPUs and 3 multi-GPU tower stacks.
    -> On each GPU, there are 3 slots for batches, indexed 0, 1, and 2.

    Workers collect data from env and push it into inqueue:
    Workers -> (data) -> self.inqueue

    We also have two queues, indicating, which stacks are loaded and which
    are not.
    - idle_tower_stacks = [0, 1, 2]  <- all 3 stacks are free at first.
    - ready_tower_stacks = []  <- None of the 3 stacks is loaded with data.

    `ready_tower_stacks` is managed by `ready_tower_stacks_buffer` for
    possible minibatch-SGD iterations per loaded batch (this avoids a reload
    from CPU to GPU for each SGD iter).

    n _MultiGPULoaderThreads: self.inqueue -get()->
    policy.load_batch_into_buffer() -> ready_stacks = [0 ...]

    This thread: self.ready_tower_stacks_buffer -get()->
    policy.learn_on_loaded_batch() -> if SGD-iters done,
    put stack index back in idle_tower_stacks queue.
    """

    def __init__(
        self,
        local_worker: RolloutWorker,
        num_gpus: int = 1,
        lr=None,  # deprecated.
        train_batch_size: int = 500,
        num_multi_gpu_tower_stacks: int = 1,
        num_sgd_iter: int = 1,
        learner_queue_size: int = 16,
        learner_queue_timeout: int = 300,
        num_data_load_threads: int = 16,
        _fake_gpus: bool = False,
        # Deprecated arg, use
        minibatch_buffer_size=None,
    ):
        """Initializes a MultiGPULearnerThread instance.

        Args:
            local_worker: Local RolloutWorker holding
                policies this thread will call `load_batch_into_buffer` and
                `learn_on_loaded_batch` on.
            num_gpus: Number of GPUs to use for data-parallel SGD.
            train_batch_size: Size of batches (minibatches if
                `num_sgd_iter` > 1) to learn on.
            num_multi_gpu_tower_stacks: Number of buffers to parallelly
                load data into on one device. Each buffer is of size of
                `train_batch_size` and hence increases GPU memory usage
                accordingly.
            num_sgd_iter: Number of passes to learn on per train batch
                (minibatch if `num_sgd_iter` > 1).
            learner_queue_size: Max size of queue of inbound
                train batches to this thread.
            num_data_load_threads: Number of threads to use to load
                data into GPU memory in parallel.
        """
        # Deprecated: No need to specify as we don't need the actual
        # minibatch-buffer anyways.
        if minibatch_buffer_size:
            deprecation_warning(
                old="MultiGPULearnerThread.minibatch_buffer_size",
                error=True,
            )
        super().__init__(
            local_worker=local_worker,
            minibatch_buffer_size=0,
            num_sgd_iter=num_sgd_iter,
            learner_queue_size=learner_queue_size,
            learner_queue_timeout=learner_queue_timeout,
        )
        # Delete reference to parent's minibatch_buffer, which is not needed.
        # Instead, in multi-GPU mode, we pull tower stack indices from the
        # `self.ready_tower_stacks_buffer` buffer, whose size is exactly
        # `num_multi_gpu_tower_stacks`.
        self.minibatch_buffer = None

        self.train_batch_size = train_batch_size

        self.policy_map = self.local_worker.policy_map
        self.devices = next(iter(self.policy_map.values())).devices

        logger.info("MultiGPULearnerThread devices {}".format(self.devices))
        assert self.train_batch_size % len(self.devices) == 0
        assert self.train_batch_size >= len(self.devices), "batch too small"

        self.tower_stack_indices = list(range(num_multi_gpu_tower_stacks))

        # Two queues for tower stacks:
        # a) Those that are loaded with data ("ready")
        # b) Those that are ready to be loaded with new data ("idle").
        self.idle_tower_stacks = queue.Queue()
        self.ready_tower_stacks = queue.Queue()
        # In the beginning, all stacks are idle (no loading has taken place
        # yet).
        for idx in self.tower_stack_indices:
            self.idle_tower_stacks.put(idx)
        # Start n threads that are responsible for loading data into the
        # different (idle) stacks.
        for i in range(num_data_load_threads):
            self.loader_thread = _MultiGPULoaderThread(self, share_stats=(i == 0))
            self.loader_thread.start()

        # Create a buffer that holds stack indices that are "ready"
        # (loaded with data). Those are stacks that we can call
        # "learn_on_loaded_batch" on.
        self.ready_tower_stacks_buffer = MinibatchBuffer(
            self.ready_tower_stacks,
            num_multi_gpu_tower_stacks,
            learner_queue_timeout,
            num_sgd_iter,
        )

    @override(LearnerThread)
    def step(self) -> None:
        if not self.loader_thread.is_alive():
            raise RuntimeError(
                "The `_MultiGPULoaderThread` has died! Will therefore also terminate "
                "the `MultiGPULearnerThread`."
            )

        with self.load_wait_timer:
            buffer_idx, released = self.ready_tower_stacks_buffer.get()

        get_num_samples_loaded_into_buffer = 0
        with self.grad_timer:
            # Use LearnerInfoBuilder as a unified way to build the final
            # results dict from `learn_on_loaded_batch` call(s).
            # This makes sure results dicts always have the same structure
            # no matter the setup (multi-GPU, multi-agent, minibatch SGD,
            # tf vs torch).
            learner_info_builder = LearnerInfoBuilder(num_devices=len(self.devices))

            for pid in self.policy_map.keys():
                # Not a policy-to-train.
                if (
                    self.local_worker.is_policy_to_train is not None
                    and not self.local_worker.is_policy_to_train(pid)
                ):
                    continue
                policy = self.policy_map[pid]
                default_policy_results = policy.learn_on_loaded_batch(
                    offset=0, buffer_index=buffer_idx
                )
                learner_info_builder.add_learn_on_batch_results(
                    default_policy_results, policy_id=pid
                )
                self.policy_ids_updated.append(pid)
                get_num_samples_loaded_into_buffer += (
                    policy.get_num_samples_loaded_into_buffer(buffer_idx)
                )

            self.learner_info = learner_info_builder.finalize()

        if released:
            self.idle_tower_stacks.put(buffer_idx)

        # Put tuple: env-steps, agent-steps, and learner info into the queue.
        self.outqueue.put(
            (
                get_num_samples_loaded_into_buffer,
                get_num_samples_loaded_into_buffer,
                self.learner_info,
            )
        )
        self.learner_queue_size.push(self.inqueue.qsize())


class _MultiGPULoaderThread(threading.Thread):
    def __init__(
        self, multi_gpu_learner_thread: MultiGPULearnerThread, share_stats: bool
    ):
        threading.Thread.__init__(self)
        self.multi_gpu_learner_thread = multi_gpu_learner_thread
        self.daemon = True
        if share_stats:
            self.queue_timer = multi_gpu_learner_thread.queue_timer
            self.load_timer = multi_gpu_learner_thread.load_timer
        else:
            self.queue_timer = _Timer()
            self.load_timer = _Timer()

    def run(self) -> None:
        while True:
            self._step()

    def _step(self) -> None:
        s = self.multi_gpu_learner_thread
        policy_map = s.policy_map

        # Get a new batch from the data (inqueue).
        with self.queue_timer:
            batch = s.inqueue.get()

        # Get next idle stack for loading.
        buffer_idx = s.idle_tower_stacks.get()

        # Load the batch into the idle stack.
        with self.load_timer:
            for pid in policy_map.keys():
                if (
                    s.local_worker.is_policy_to_train is not None
                    and not s.local_worker.is_policy_to_train(pid, batch)
                ):
                    continue
                policy = policy_map[pid]
                if isinstance(batch, SampleBatch):
                    policy.load_batch_into_buffer(
                        batch=batch,
                        buffer_index=buffer_idx,
                    )
                elif pid in batch.policy_batches:
                    policy.load_batch_into_buffer(
                        batch=batch.policy_batches[pid],
                        buffer_index=buffer_idx,
                    )

        # Tag just-loaded stack as "ready".
        s.ready_tower_stacks.put(buffer_idx)
