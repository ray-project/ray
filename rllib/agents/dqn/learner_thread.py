import threading
from six.moves import queue

from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.policy.policy import LEARNER_STATS_KEY
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat

LEARNER_QUEUE_MAX_SIZE = 16


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
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.overall_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False
        self.stopped = False
        self.stats = {}

    def run(self):
        while not self.stopped:
            self.step()

    def step(self):
        with self.overall_timer:
            with self.queue_timer:
                ra, replay = self.inqueue.get()
            if replay is not None:
                prio_dict = {}
                with self.grad_timer:
                    grad_out = self.local_worker.learn_on_batch(replay)
                    for pid, info in grad_out.items():
                        td_error = info.get(
                            "td_error",
                            info[LEARNER_STATS_KEY].get("td_error"))
                        prio_dict[pid] = (replay.policy_batches[pid].data.get(
                            "batch_indexes"), td_error)
                        self.stats[pid] = get_learner_stats(info)
                    self.grad_timer.push_units_processed(replay.count)
                self.outqueue.put((ra, prio_dict, replay.count))
            self.learner_queue_size.push(self.inqueue.qsize())
            self.weights_updated = True
            self.overall_timer.push_units_processed(replay and replay.count
                                                    or 0)
