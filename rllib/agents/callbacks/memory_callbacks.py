import os
import tracemalloc
from typing import Optional, Dict, TYPE_CHECKING

import psutil

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import PolicyID

if TYPE_CHECKING:
    from ray.rllib.evaluation import RolloutWorker


@PublicAPI
class MemoryTrackingCallbacks(DefaultCallbacks):
    """
    MemoryTrackingCallbacks can be used to trace and track memory usage
    in rollout workers.

    The Memory Tracking Callbacks uses tracemalloc and psutil to track
    python allocations during rollouts,
    in training or evaluation.

    The tracking data is logged to the custom_metrics of an episode and
    can therefore be viewed in tensorboard
    (or in WandB etc..)

    Warning: This class is meant for debugging and should not be used
    in production code as tracemalloc incurs
    a significant slowdown in execution speed.

    Add MemoryTrackingCallbacks callback to the tune config
    e.g. { ...'callbacks': MemoryTrackingCallbacks ...}
    """

    def __init__(self):
        super().__init__()

        # Will track the top 10 lines where memory is allocated
        tracemalloc.start(10)

    def on_episode_end(self,
                       *,
                       worker: "RolloutWorker",
                       base_env: BaseEnv,
                       policies: Dict[PolicyID, Policy],
                       episode: MultiAgentEpisode,
                       env_index: Optional[int] = None,
                       **kwargs) -> None:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        for stat in top_stats[:10]:
            count = stat.count
            size = stat.size

            trace = str(stat.traceback)

            episode.custom_metrics[f"tracemalloc/{trace}/size"] = size
            episode.custom_metrics[f"tracemalloc/{trace}/count"] = count

        process = psutil.Process(os.getpid())
        worker_rss = process.memory_info().rss
        worker_data = process.memory_info().data
        worker_vms = process.memory_info().vms
        episode.custom_metrics[f"tracemalloc/worker/rss"] = worker_rss
        episode.custom_metrics[f"tracemalloc/worker/data"] = worker_data
        episode.custom_metrics[f"tracemalloc/worker/vms"] = worker_vms
