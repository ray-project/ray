from collections import defaultdict
from typing import Dict, List, DefaultDict

from ray.actor import ActorHandle
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.evaluation.worker_set import WorkerSet


class EpisodeCollector:
    """Class used for collecting and summarizing episode metrics from workers.

    The metrics are smoothed over a given history window.

    Examples:
        >>> collector = EpisodeCollector(...)
        >>> episode_results = collector.collect()
        >>> print(episode_results)
        {"episode_reward_max": ..., "episode_reward_mean": ..., ...}
    """

    def __init__(self,
                 workers: WorkerSet,
                 min_history: int = 100,
                 timeout_seconds: int = 180,
                 selected_workers: List[ActorHandle] = None,
                 by_steps_trained: bool = False,
                 counters: DefaultDict[str, int] = None,
                 ):
        self.workers = workers
        self.episode_history = []
        self.to_be_collected = []
        self.min_history = min_history
        self.timeout_seconds = timeout_seconds
        self.selected_workers = selected_workers
        self.by_steps_trained = by_steps_trained
        self.counters = counters or defaultdict(int)

    def collect(self) -> Dict:
        # Collect worker metrics.
        episodes, self.to_be_collected = collect_episodes(
            self.workers.local_worker(),
            self.selected_workers or self.workers.remote_workers(),
            self.to_be_collected,
            timeout_seconds=self.timeout_seconds)
        orig_episodes = list(episodes)
        missing = self.min_history - len(episodes)
        if missing > 0:
            episodes = self.episode_history[-missing:] + episodes
            assert len(episodes) <= self.min_history
        self.episode_history.extend(orig_episodes)
        self.episode_history = self.episode_history[-self.min_history:]
        res = summarize_episodes(episodes, orig_episodes)

        return res
