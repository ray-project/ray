import copy
import queue
import threading
from typing import Dict, Optional

from ray.util.timer import _Timer
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.minibatch_buffer import MinibatchBuffer
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder, LEARNER_INFO
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.util.iter import _NextValueNotReady
from ray.util import log_once

tf1, tf, tfv = try_import_tf()


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from sample trajectories.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.
    """

    def __init__(
        self,
        local_worker: RolloutWorker,
        minibatch_buffer_size: int,
        num_sgd_iter: int,
        learner_queue_size: int,
        learner_queue_timeout: int,
    ):
        """Initialize the learner thread.

        Args:
            local_worker: process local rollout worker holding
                policies this thread will call learn_on_batch() on
            minibatch_buffer_size: max number of train batches to store
                in the minibatching buffer
            num_sgd_iter: number of passes to learn on per train batch
            learner_queue_size: max size of queue of inbound
                train batches to this thread
            learner_queue_timeout: raise an exception if the queue has
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
            init_num_passes=num_sgd_iter,
        )
        self.queue_timer = _Timer()
        self.grad_timer = _Timer()
        self.load_timer = _Timer()
        self.load_wait_timer = _Timer()
        self.daemon = True
        self.policy_ids_updated = []
        self.learner_info = {}
        self.stopped = False
        self.num_steps = 0
        if log_once("learner-thread-deprecation-warning"):
            deprecation_warning(old="ray.rllib.execution.learner_thread.LearnerThread")

    def run(self) -> None:
        # Switch on eager mode if configured.
        if self.local_worker.config.framework_str == "tf2":
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
            if self.local_worker.config.policy_states_are_swappable:
                self.local_worker.lock()
            multi_agent_results = self.local_worker.learn_on_batch(batch)
            if self.local_worker.config.policy_states_are_swappable:
                self.local_worker.unlock()
            self.policy_ids_updated.extend(list(multi_agent_results.keys()))
            for pid, results in multi_agent_results.items():
                learner_info_builder.add_learn_on_batch_results(results, pid)
            self.learner_info = learner_info_builder.finalize()

        self.num_steps += 1
        # Put tuple: env-steps, agent-steps, and learner info into the queue.
        self.outqueue.put((batch.count, batch.agent_steps(), self.learner_info))
        self.learner_queue_size.push(self.inqueue.qsize())

    def add_learner_metrics(self, result: Dict, overwrite_learner_info=True) -> Dict:
        """Add internal metrics to a result dict."""

        def timer_to_ms(timer):
            return round(1000 * timer.mean, 3)

        if overwrite_learner_info:
            result["info"].update(
                {
                    "learner_queue": self.learner_queue_size.stats(),
                    LEARNER_INFO: copy.deepcopy(self.learner_info),
                    "timing_breakdown": {
                        "learner_grad_time_ms": timer_to_ms(self.grad_timer),
                        "learner_load_time_ms": timer_to_ms(self.load_timer),
                        "learner_load_wait_time_ms": timer_to_ms(self.load_wait_timer),
                        "learner_dequeue_time_ms": timer_to_ms(self.queue_timer),
                    },
                }
            )
        else:
            result["info"].update(
                {
                    "learner_queue": self.learner_queue_size.stats(),
                    "timing_breakdown": {
                        "learner_grad_time_ms": timer_to_ms(self.grad_timer),
                        "learner_load_time_ms": timer_to_ms(self.load_timer),
                        "learner_load_wait_time_ms": timer_to_ms(self.load_wait_timer),
                        "learner_dequeue_time_ms": timer_to_ms(self.queue_timer),
                    },
                }
            )
        return result
