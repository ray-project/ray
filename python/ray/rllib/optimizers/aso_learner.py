"""Helper class for AsyncSamplesOptimizer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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

    def __init__(self,
                 local_worker,
                 minibatch_buffer_size,
                 num_sgd_iter,
                 learner_queue_size,
                 old_worker=None,
                 use_kl_loss=False):
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStat("size", 50)
        self.local_worker = local_worker
        self.inqueue = queue.Queue(maxsize=learner_queue_size)
        self.outqueue = queue.Queue()
        self.minibatch_buffer = MinibatchBuffer(
            self.inqueue,
            minibatch_buffer_size,
            num_sgd_iter,
            init_num_passes=num_sgd_iter)
        self.old_worker = old_worker
        self.tracker = None
        if self.old_worker:
            self.tracker = APPOTracker(self.local_worker, self.old_worker,
                                       minibatch_buffer_size, num_sgd_iter,
                                       use_kl_loss)
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.load_timer = TimerStat()
        self.load_wait_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False
        self.stats = {}
        self.stopped = False

    def run(self):
        while not self.stopped:
            self.step()

    def step(self):
        with self.queue_timer:
            # old_worker not None means that APPO agent is being used (the only agent that uses old_worker)
            if self.old_worker:
                batch, _ = self.minibatch_buffer.get()
            else:
                batch = self.inqueue.get()

        if self.tracker:
            batch = self.tracker.update_batch(batch)

        with self.grad_timer:
            fetches = self.local_worker.learn_on_batch(batch)
            self.weights_updated = True
            self.stats = get_learner_stats(fetches)

        if self.tracker:
            self.tracker.step()

        self.outqueue.put(batch.count)
        self.learner_queue_size.push(self.inqueue.qsize())


class APPOTracker(object):
    def __init__(self, local_worker, old_worker, minibatch_buffer_size,
                 num_sgd_iter, use_kl_loss):
        weights = local_worker.get_weights()
        old_worker.set_weights(weights)
        self.local_worker = local_worker
        self.old_worker = old_worker
        self.use_kl_loss = use_kl_loss
        self.minibatch_counter = 0
        self.minibatch_buffer_size = minibatch_buffer_size
        self.old_worker_lag = minibatch_buffer_size * num_sgd_iter
        self.kls = []

    def step(self):
        self.minibatch_counter += 1
        # Start collecting mean KLs during the last pass through Minibatch Buffer
        if self.use_kl_loss and self.minibatch_counter > self.old_worker_lag - self.minibatch_buffer_size:
            self.kls.append(fetches["learner_stats"]["KL"])

        if self.minibatch_counter == self.old_worker_lag:
            if self.use_kl_loss:
                avg_kl = sum(self.kls) / len(self.kls)
                self.local_worker.for_policy(lambda pi: pi.update_kl(avg_kl))
                self.kls = []

            if self.old_worker:
                weights = self.local_worker.get_weights()
                self.old_worker.set_weights(weights)
            self.minibatch_counter = 0

    def update_batch(self, batch):
        if "old_policy_behaviour_logits" not in batch:
            batch["old_policy_behaviour_logits"] = self.old_worker.policy_map[
                'default_policy'].compute_actions(
                    batch["obs"],
                    prev_action_batch=batch["prev_actions"],
                    prev_reward_batch=batch["prev_rewards"])[2][
                        'behaviour_logits']
        return batch
