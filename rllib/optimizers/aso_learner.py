"""Helper class for AsyncSamplesOptimizer."""

import threading

from six.moves import queue

from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.optimizers.aso_minibatch_buffer import MinibatchBuffer
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from sample trajectories.

    This is for use with AsyncSamplesOptimizer.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.
    """

    def __init__(self, local_worker, minibatch_buffer_size, num_sgd_iter,
                 learner_queue_size, learner_queue_timeout):
        """Initialize the learner thread.

        Arguments:
            local_worker (RolloutWorker): process local rollout worker holding
                policies this thread will call learn_on_batch() on
            minibatch_buffer_size (int): max number of train batches to store
                in the minibatching buffer
            num_sgd_iter (int): number of passes to learn on per train batch
            learner_queue_size (int): max size of queue of inbound
                train batches to this thread
            learner_queue_timeout (int): raise an exception if the queue has
                been empty for this long in seconds
        """
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStat("size", 50)
        self.local_worker = local_worker
        self.inqueue = queue.Queue(maxsize=learner_queue_size)
        self.outqueue = queue.Queue()
        self.minibatch_buffer = MinibatchBuffer(
            inqueue=self.inqueue,
            size=minibatch_buffer_size,
            timeout=learner_queue_timeout,
            num_passes=num_sgd_iter,
            init_num_passes=num_sgd_iter)
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.load_timer = TimerStat()
        self.load_wait_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False
        self.stats = {}
        self.stopped = False
        self.num_steps = 0

    def run(self):
        while not self.stopped:
            self.step()

    def step(self):
        with self.queue_timer:
            batch, _ = self.minibatch_buffer.get()

        with self.grad_timer:
            fetches = self.local_worker.learn_on_batch(batch)
            self.weights_updated = True
            self.stats = get_learner_stats(fetches)

        self.num_steps += 1
        self.outqueue.put(batch.count)
        self.learner_queue_size.push(self.inqueue.qsize())
