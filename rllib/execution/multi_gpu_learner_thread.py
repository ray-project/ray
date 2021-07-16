import logging
import threading
import math

from six.moves import queue

from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.minibatch_buffer import MinibatchBuffer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.timer import TimerStat
from ray.rllib.evaluation.rollout_worker import RolloutWorker

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class MultiGPULearnerThread(LearnerThread):
    """Learner that can use multiple GPUs and parallel loading.

    This class is used for async sampling algorithms.
    """

    def __init__(
            self,
            local_worker: RolloutWorker,
            num_gpus: int = 1,
            lr=None,  # deprecated 
            train_batch_size: int = 500,
            num_multi_gpu_tower_stacks: int = 1,
            minibatch_buffer_size: int = 1,
            num_sgd_iter: int = 1,
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
            num_multi_gpu_tower_stacks (int): Number of buffers to load data into
                in parallel on one device. Each buffer is of size of
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
        LearnerThread.__init__(self, local_worker, minibatch_buffer_size,
                               num_sgd_iter, learner_queue_size,
                               learner_queue_timeout)
        self.train_batch_size = train_batch_size
        if not num_gpus:
            self.devices = ["/cpu:0"]
        elif _fake_gpus:
            self.devices = [
                "/cpu:{}".format(i) for i in range(int(math.ceil(num_gpus)))
            ]
        else:
            self.devices = [
                "/gpu:{}".format(i) for i in range(int(math.ceil(num_gpus)))
            ]
        logger.info("MultiGPULearnerThread devices {}".format(self.devices))
        assert self.train_batch_size % len(self.devices) == 0
        assert self.train_batch_size >= len(self.devices), "batch too small"

        if set(self.local_worker.policy_map.keys()) != {DEFAULT_POLICY_ID}:
            raise NotImplementedError("Multi-gpu mode for multi-agent")

        self.policy = self.local_worker.policy_map[DEFAULT_POLICY_ID]

        # TODO: Move into DynamicTFPolicy.
        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        self.tower_stack_indices = \
            [i for i in range(num_multi_gpu_tower_stacks)]
        #with self.local_worker.tf_sess.graph.as_default():
        #    with self.local_worker.tf_sess.as_default():
        #        with tf1.variable_scope(
        #                DEFAULT_POLICY_ID, reuse=tf1.AUTO_REUSE):
        #            if self.policy._state_inputs:
        #                rnn_inputs = self.policy._state_inputs + [
        #                    self.policy._seq_lens
        #                ]
        #            else:
        #                rnn_inputs = []
        #            adam = tf1.train.AdamOptimizer(self.lr)
        #            for _ in range(num_multi_gpu_tower_stacks):
        #                self.par_opt.append(
        #                    LocalSyncParallelOptimizer(
        #                        adam,
        #                        self.devices,
        #                        list(
        #                            self.policy._loss_input_dict_no_rnn.values(
        #                            )),
        #                        rnn_inputs,
        #                        999999,  # it will get rounded down
        #                        self.policy.copy))
        #
        #        self.sess = self.local_worker.tf_sess
        #        self.sess.run(tf1.global_variables_initializer())

        self.idle_tower_stacks = queue.Queue()
        self.ready_tower_stacks = queue.Queue()
        for idx in self.tower_stack_indices:
            self.idle_tower_stacks.put(idx)
        for i in range(num_data_load_threads):
            self.loader_thread = _MultiGPULoaderThread(
                self, share_stats=(i == 0))
            self.loader_thread.start()

        self.minibatch_buffer = MinibatchBuffer(
            self.ready_tower_stacks, minibatch_buffer_size,
            learner_queue_timeout, num_sgd_iter)

    @override(LearnerThread)
    def step(self) -> None:
        assert self.loader_thread.is_alive()
        with self.load_wait_timer:
            buffer_idx, released = self.minibatch_buffer.get()

        with self.grad_timer:
            fetches = self.policy.learn_on_loaded_batch(
                offset=0, buffer_index=buffer_idx)
            self.weights_updated = True
            self.stats = get_learner_stats(fetches)

        if released:
            self.idle_tower_stacks.put(buffer_idx)

        self.outqueue.put(
            (self.policy.multi_gpu_tower_stacks[buffer_idx].num_tuples_loaded,
             self.stats))
        self.learner_queue_size.push(self.inqueue.qsize())


class _MultiGPULoaderThread(threading.Thread):
    def __init__(self, multi_gpu_learner_thread: MultiGPULearnerThread,
                 share_stats: bool):
        threading.Thread.__init__(self)
        self.multi_gpu_learner_thread = multi_gpu_learner_thread
        self.daemon = True
        if share_stats:
            self.queue_timer = multi_gpu_learner_thread.queue_timer
            self.load_timer = multi_gpu_learner_thread.load_timer
        else:
            self.queue_timer = TimerStat()
            self.load_timer = TimerStat()

    def run(self) -> None:
        while True:
            self._step()

    def _step(self) -> None:
        s = self.multi_gpu_learner_thread
        policy = s.policy
        with self.queue_timer:
            batch = s.inqueue.get()

        buffer_idx = s.idle_tower_stacks.get()

        with self.load_timer:
            #tuples = policy._get_loss_inputs_dict(batch, shuffle=False)
            #data_keys = list(policy._loss_input_dict_no_rnn.values())
            #if policy._state_inputs:
            #    state_keys = policy._state_inputs + [policy._seq_lens]
            #else:
            #    state_keys = []
            #TODO: implement this in policy (move the above 6 lines into dyn. tf policy!)
            policy.load_batch_into_buffer(batch=batch, buffer_index=buffer_idx)
            #, [tuples[k] for k in data_keys],
            #          [tuples[k] for k in state_keys])

        s.ready_tower_stacks.put(buffer_idx)
