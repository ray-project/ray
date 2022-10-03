import logging
import os

import ray
from ray.tune.search import SearchAlgorithm

logger = logging.getLogger(__name__)


def get_max_pending_trials(search_alg: SearchAlgorithm) -> int:
    max_pending_trials = os.getenv("TUNE_MAX_PENDING_TRIALS_PG", "auto")
    if max_pending_trials == "auto":
        # Auto detect
        if search_alg._max_pending_trials is None:
            # Use a minimum of 16 to trigger fast autoscaling
            # Scale up to at most the number of available cluster CPUs
            cluster_cpus = ray.cluster_resources().get("CPU", 1.0)
            max_pending_trials = max(16, int(cluster_cpus * 1.1))

            if max_pending_trials > 128:
                logger.warning(
                    f"The maximum number of pending trials has been "
                    f"automatically set to the number of available "
                    f"cluster CPUs, which is high "
                    f"({max_pending_trials} CPUs/pending trials). "
                    f"If you're running an experiment with a large number "
                    f"of trials, this could lead to scheduling overhead. "
                    f"In this case, consider setting the "
                    f"`TUNE_MAX_PENDING_TRIALS_PG` environment variable "
                    f"to the desired maximum number of concurrent trials."
                )
        else:
            max_pending_trials = search_alg._max_pending_trials
    else:
        # Manual override
        max_pending_trials = int(max_pending_trials)

    return max_pending_trials
