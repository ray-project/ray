import queue
import threading

from ray.util.timer import _Timer
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.metrics.window_stat import WindowStat

LEARNER_QUEUE_MAX_SIZE = 16

tf1, tf, tfv = try_import_tf()


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from replay data.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.
    """

    def __init__(self, local_worker):
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStat("size", 50)
        self.local_worker = local_worker
        self.inqueue = queue.Queue(maxsize=LEARNER_QUEUE_MAX_SIZE)
        self.outqueue = queue.Queue()
        self.queue_timer = _Timer()
        self.grad_timer = _Timer()
        self.overall_timer = _Timer()
        self.daemon = True
        self.weights_updated = False
        self.stopped = False
        self.learner_info = {}

    def run(self):
        # Switch on eager mode if configured.
        if self.local_worker.policy_config.get("framework") in ["tf2", "tfe"]:
            tf1.enable_eager_execution()
        while not self.stopped:
            self.step()

    def step(self):
        with self.overall_timer:
            with self.queue_timer:
                replay_actor, ma_batch = self.inqueue.get()
            if ma_batch is not None:
                prio_dict = {}
                with self.grad_timer:
                    # Use LearnerInfoBuilder as a unified way to build the
                    # final results dict from `learn_on_loaded_batch` call(s).
                    # This makes sure results dicts always have the same
                    # structure no matter the setup (multi-GPU, multi-agent,
                    # minibatch SGD, tf vs torch).
                    learner_info_builder = LearnerInfoBuilder(num_devices=1)
                    multi_agent_results = self.local_worker.learn_on_batch(ma_batch)
                    for pid, results in multi_agent_results.items():
                        learner_info_builder.add_learn_on_batch_results(results, pid)
                        td_error = results["td_error"]
                        # Switch off auto-conversion from numpy to torch/tf
                        # tensors for the indices. This may lead to errors
                        # when sent to the buffer for processing
                        # (may get manipulated if they are part of a tensor).
                        ma_batch.policy_batches[pid].set_get_interceptor(None)
                        prio_dict[pid] = (
                            ma_batch.policy_batches[pid].get("batch_indexes"),
                            td_error,
                        )
                    self.learner_info = learner_info_builder.finalize()
                    self.grad_timer.push_units_processed(ma_batch.count)
                # Put tuple: replay_actor, prio-dict, env-steps, and agent-steps into
                # the queue.
                self.outqueue.put(
                    (replay_actor, prio_dict, ma_batch.count, ma_batch.agent_steps())
                )
            self.learner_queue_size.push(self.inqueue.qsize())
            self.weights_updated = True
            self.overall_timer.push_units_processed(ma_batch and ma_batch.count or 0)
