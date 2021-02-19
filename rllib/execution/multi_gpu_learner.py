import logging
import threading
import math

from six.moves import queue

from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.minibatch_buffer import MinibatchBuffer
from ray.rllib.execution.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.timer import TimerStat
from ray.rllib.evaluation.rollout_worker import RolloutWorker

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class TFMultiGPULearner(LearnerThread):
    """Learner that can use multiple GPUs and parallel loading.

    This is for use with AsyncSamplesOptimizer.
    """

    def __init__(self,
                 local_worker: RolloutWorker,
                 num_gpus: int = 1,
                 lr: float = 0.0005,
                 train_batch_size: int = 500,
                 num_data_loader_buffers: int = 1,
                 minibatch_buffer_size: int = 1,
                 num_sgd_iter: int = 1,
                 learner_queue_size: int = 16,
                 learner_queue_timeout: int = 300,
                 num_data_load_threads: int = 16,
                 _fake_gpus: bool = False):
        """Initialize a multi-gpu learner thread.

        Args:
            local_worker (RolloutWorker): process local rollout worker holding
                policies this thread will call learn_on_batch() on
            num_gpus (int): number of GPUs to use for data-parallel SGD
            lr (float): learning rate
            train_batch_size (int): size of batches to learn on
            num_data_loader_buffers (int): number of buffers to load data into
                in parallel. Each buffer is of size of train_batch_size and
                increases GPU memory usage proportionally.
            minibatch_buffer_size (int): max number of train batches to store
                in the minibatching buffer
            num_sgd_iter (int): number of passes to learn on per train batch
            learner_queue_size (int): max size of queue of inbound
                train batches to this thread
            num_data_loader_threads (int): number of threads to use to load
                data into GPU memory in parallel
        """
        LearnerThread.__init__(self, local_worker, minibatch_buffer_size,
                               num_sgd_iter, learner_queue_size,
                               learner_queue_timeout)
        self.lr = lr
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
        logger.info("TFMultiGPULearner devices {}".format(self.devices))
        assert self.train_batch_size % len(self.devices) == 0
        assert self.train_batch_size >= len(self.devices), "batch too small"

        if set(self.local_worker.policy_map.keys()) != {DEFAULT_POLICY_ID}:
            raise NotImplementedError("Multi-gpu mode for multi-agent")
        self.policy = self.local_worker.policy_map[DEFAULT_POLICY_ID]

        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        self.par_opt = []
        with self.local_worker.tf_sess.graph.as_default():
            with self.local_worker.tf_sess.as_default():
                with tf1.variable_scope(
                        DEFAULT_POLICY_ID, reuse=tf1.AUTO_REUSE):
                    if self.policy._state_inputs:
                        rnn_inputs = self.policy._state_inputs + [
                            self.policy._seq_lens
                        ]
                    else:
                        rnn_inputs = []
                    adam = tf1.train.AdamOptimizer(self.lr)
                    for _ in range(num_data_loader_buffers):
                        self.par_opt.append(
                            LocalSyncParallelOptimizer(
                                adam,
                                self.devices,
                                list(
                                    self.policy._loss_input_dict_no_rnn.values(
                                    )),
                                rnn_inputs,
                                999999,  # it will get rounded down
                                self.policy.copy))

                self.sess = self.local_worker.tf_sess
                self.sess.run(tf1.global_variables_initializer())

        self.idle_optimizers = queue.Queue()
        self.ready_optimizers = queue.Queue()
        for opt in self.par_opt:
            self.idle_optimizers.put(opt)
        for i in range(num_data_load_threads):
            self.loader_thread = _LoaderThread(self, share_stats=(i == 0))
            self.loader_thread.start()

        self.minibatch_buffer = MinibatchBuffer(
            self.ready_optimizers, minibatch_buffer_size,
            learner_queue_timeout, num_sgd_iter)

    @override(LearnerThread)
    def step(self) -> None:
        assert self.loader_thread.is_alive()
        with self.load_wait_timer:
            opt, released = self.minibatch_buffer.get()

        with self.grad_timer:
            fetches = opt.optimize(self.sess, 0)
            self.weights_updated = True
            self.stats = get_learner_stats(fetches)

        if released:
            self.idle_optimizers.put(opt)

        self.outqueue.put((opt.num_tuples_loaded, self.stats))
        self.learner_queue_size.push(self.inqueue.qsize())


class _LoaderThread(threading.Thread):
    def __init__(self, learner: LearnerThread, share_stats: bool):
        threading.Thread.__init__(self)
        self.learner = learner
        self.daemon = True
        if share_stats:
            self.queue_timer = learner.queue_timer
            self.load_timer = learner.load_timer
        else:
            self.queue_timer = TimerStat()
            self.load_timer = TimerStat()

    def run(self) -> None:
        while True:
            self._step()

    def _step(self) -> None:
        s = self.learner
        with self.queue_timer:
            batch = s.inqueue.get()

        opt = s.idle_optimizers.get()

        with self.load_timer:
            tuples = s.policy._get_loss_inputs_dict(batch, shuffle=False)
            data_keys = list(s.policy._loss_input_dict_no_rnn.values())
            if s.policy._state_inputs:
                state_keys = s.policy._state_inputs + [s.policy._seq_lens]
            else:
                state_keys = []
            opt.load_data(s.sess, [tuples[k] for k in data_keys],
                          [tuples[k] for k in state_keys])

        s.ready_optimizers.put(opt)
