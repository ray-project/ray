import copy
from six.moves import queue
import threading
from typing import Dict, Optional

from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.minibatch_buffer import MinibatchBuffer
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder, \
    LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.rllib.utils.timer import TimerStat
from ray.util.iter import _NextValueNotReady

tf1, tf, tfv = try_import_tf()


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from sample trajectories.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.
    """

    def __init__(self, local_worker: RolloutWorker, minibatch_buffer_size: int,
                 num_sgd_iter: int, learner_queue_size: int,
                 learner_queue_timeout: int):
        """Initialize the learner thread.

        Args:
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
        self.learner_info = {}
        self.stopped = False
        self.num_steps = 0

    def run(self) -> None:
        # Switch on eager mode if configured.
        if self.local_worker.policy_config.get("framework") in ["tf2", "tfe"]:
            tf1.enable_eager_execution()
        while not self.stopped:
            self.step()

    def step(self) -> Optional[_NextValueNotReady]:
        with self.queue_timer:
            try:
                batch, _ = self.minibatch_buffer.get()
            except queue.Empty:
                return _NextValueNotReady()

        with self.grad_timer:
            # Use LearnerInfoBuilder as a unified way to build the final
            # results dict from `learn_on_loaded_batch` call(s).
            # This makes sure results dicts always have the same structure
            # no matter the setup (multi-GPU, multi-agent, minibatch SGD,
            # tf vs torch).
            learner_info_builder = LearnerInfoBuilder(num_devices=1)
            multi_agent_results = self.local_worker.learn_on_batch(batch)
            for pid, results in multi_agent_results.items():
                learner_info_builder.add_learn_on_batch_results(results, pid)
            self.learner_info = learner_info_builder.finalize()
            learner_stats = {
                pid: info[LEARNER_STATS_KEY]
                for pid, info in self.learner_info.items()
            }
            self.weights_updated = True

        self.num_steps += 1
        self.outqueue.put((batch.count, learner_stats))
        self.learner_queue_size.push(self.inqueue.qsize())

    def add_learner_metrics(self, result: Dict) -> Dict:
        """Add internal metrics to a trainer result dict."""

        def timer_to_ms(timer):
            return round(1000 * timer.mean, 3)

        result["info"].update({
            "learner_queue": self.learner_queue_size.stats(),
            LEARNER_INFO: copy.deepcopy(self.learner_info),
            "timing_breakdown": {
                "learner_grad_time_ms": timer_to_ms(self.grad_timer),
                "learner_load_time_ms": timer_to_ms(self.load_timer),
                "learner_load_wait_time_ms": timer_to_ms(self.load_wait_timer),
                "learner_dequeue_time_ms": timer_to_ms(self.queue_timer),
            }
        })
        return result
