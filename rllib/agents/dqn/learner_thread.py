import queue
import threading

from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.policy.policy import LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat

LEARNER_QUEUE_MAX_SIZE = 16

tf1, tf, tfv = try_import_tf()


class APEXLearnerThread(threading.Thread):
    """Background thread that updates the local model from replay data.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.

    The different queues in particular are:
    inqueue: Holds tuples of (replay actor (which shard?), multi-agent batch).
    outqueue: Populated after an update with tuple:
      (replay actor, prio-weights dict, timesteps used for update).
    """

    def __init__(self, local_worker):
        """
        Args:
            local_worker (RolloutWorker): The local worker holding the central
                model to be updated from incoming replay data.
        """
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStat("size", 50)
        self.local_worker = local_worker
        self.inqueue = queue.Queue(maxsize=LEARNER_QUEUE_MAX_SIZE)
        self.outqueue = queue.Queue()
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.load_timer = TimerStat()
        self.load_wait_timer = TimerStat()
        self.overall_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False
        self.stopped = False
        self.stats = {}

    def run(self):
        """Runs the task of this thread until self.stopped is set to True."""

        # Switch on eager mode if configured.
        if self.local_worker.policy_config.get("framework") in ["tf2", "tfe"]:
            tf1.enable_eager_execution()

        # Run till self.stopped is set to True.
        while not self.stopped:
            self.step()

    def step(self):
        """Executes the actual logic for one step.

        Get batch from in-queue, call `learn_on_batch()` on the local worker,
        update some stats, and send results (new prio weights, steps updated)
        to out-queue (for metrics and prio-weights updates in the prioritized
        replay buffer).
        """

        with self.overall_timer:
            with self.queue_timer:
                replay_actor, ma_batch = self.inqueue.get()
            if ma_batch is not None:
                prio_dict = {}
                with self.grad_timer:
                    grad_out = self.local_worker.learn_on_batch(ma_batch)
                    for pid, info in grad_out.items():
                        td_error = info.get(
                            "td_error",
                            info[LEARNER_STATS_KEY].get("td_error"))
                        # Switch off auto-conversion from numpy to torch/tf
                        # tensors for the indices. This may lead to errors
                        # when sent to the buffer for processing
                        # (may get manipulated if they are part of a tensor).
                        ma_batch.policy_batches[pid].set_get_interceptor(None)
                        prio_dict[pid] = (
                            ma_batch.policy_batches[pid].get("batch_indexes"),
                            td_error)
                        self.stats[pid] = get_learner_stats(info)
                    self.grad_timer.push_units_processed(ma_batch.count)
                self.outqueue.put((replay_actor, prio_dict, ma_batch.count))
                self.weights_updated = True

            self.learner_queue_size.push(self.inqueue.qsize())
            self.overall_timer.push_units_processed(ma_batch and ma_batch.count
                                                    or 0)


# For backward compatibility.
LearnerThread = APEXLearnerThread


class APEXMultiGPULearnerThread(APEXLearnerThread):
    """Background thread that updates the local model from replay data.

     Thereby utilizing multiple GPUs via pre data loading, then updating
     from the pre-loaded data.
    """

    def __init__(
            self,
            local_worker: RolloutWorker,
            train_batch_size: int = 500,
            num_multi_gpu_tower_stacks: int = 1,
            learner_queue_size: int = 16,
            learner_queue_timeout: int = 300,
            num_data_load_threads: int = 16,
            _fake_gpus: bool = False):
        """Initializes a MultiGPULearnerThread instance.

        Args:
            local_worker (RolloutWorker): Local RolloutWorker holding
                policies this thread will call load_data() and optimizer() on.
            num_gpus (int): Number of GPUs to use for data-parallel SGD.
            train_batch_size (int): Size of batches (minibatches if
                `num_sgd_iter` > 1) to learn on.
            num_multi_gpu_tower_stacks (int): Number of buffers to parallelly
                load data into on one device. Each buffer is of size
                `train_batch_size` and hence increases GPU memory usage
                accordingly.
            minibatch_buffer_size (int): Max number of train batches to store
                in the minibatch buffer.
            num_sgd_iter (int): Number of passes to learn on per train batch
                (minibatch if `num_sgd_iter` > 1).
            learner_queue_size (int): Max size of queue of inbound
                train batches to this thread.
            num_data_load_threads (int): Number of threads to use to load
                data into GPU memory in parallel.
        """
        APEXLearnerThread.__init__(self, local_worker)
        self.train_batch_size = train_batch_size

        # TODO: (sven) Allow multi-GPU to work for multi-agent as well.
        self.policy = self.local_worker.policy_map[DEFAULT_POLICY_ID]

        logger.info("APEXMultiGPULearnerThread devices {}".format(
            self.policy.devices))
        assert self.train_batch_size % len(self.policy.devices) == 0
        assert self.train_batch_size >= len(self.policy.devices), \
            "batch too small"

        if set(self.local_worker.policy_map.keys()) != {DEFAULT_POLICY_ID}:
            raise NotImplementedError("Multi-gpu mode for multi-agent")

        self.tower_stack_indices = list(range(num_multi_gpu_tower_stacks))

        self.idle_tower_stacks = queue.Queue()
        self.ready_tower_stacks = queue.Queue()
        for idx in self.tower_stack_indices:
            self.idle_tower_stacks.put(idx)
        for i in range(num_data_load_threads):
            self.loader_thread = _MultiGPULoaderThread(
                self, share_stats=(i == 0))
            self.loader_thread.start()

    @override(APEXLearnerThread)
    def step(self) -> None:
        assert self.loader_thread.is_alive()
        with self.load_wait_timer:
            buffer_idx, released = self.inqueue.get()

        with self.grad_timer:
            fetches = self.policy.learn_on_loaded_batch(
                offset=0, buffer_index=buffer_idx)
            self.weights_updated = True
            self.stats = {DEFAULT_POLICY_ID: get_learner_stats(fetches)}

        if released:
            self.idle_tower_stacks.put(buffer_idx)

        self.outqueue.put(
            (self.policy.get_num_samples_loaded_into_buffer(buffer_idx),
             self.stats))
        self.learner_queue_size.push(self.inqueue.qsize())
