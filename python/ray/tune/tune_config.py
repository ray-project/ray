from dataclasses import dataclass
from typing import Optional

from ray.tune.schedulers import TrialScheduler
from ray.tune.search import Searcher
from ray.util import PublicAPI


@dataclass
@PublicAPI(stability="alpha")
class TuneConfig:
    """Tune specific configs.

    Args:
        metric: Metric to optimize. This metric should be reported
            with `tune.report()`. If set, will be passed to the search
            algorithm and scheduler.
        mode: Must be one of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute. If set, will be
            passed to the search algorithm and scheduler.
        search_alg: Search algorithm for optimization. Default to
            random search.
        scheduler: Scheduler for executing the experiment.
            Choose among FIFO (default), MedianStopping,
            AsyncHyperBand, HyperBand and PopulationBasedTraining. Refer to
            ray.tune.schedulers for more options.
        num_samples: Number of times to sample from the
            hyperparameter space. Defaults to 1. If `grid_search` is
            provided as an argument, the grid will be repeated
            `num_samples` of times. If this is -1, (virtually) infinite
            samples are generated until a stopping condition is met.
    """

    # Currently this is not at feature parity with `tune.run`, nor should it be.
    # The goal is to reach a fine balance between API flexibility and conciseness.
    # We should carefully introduce arguments here instead of just dumping everything.
    mode: Optional[str] = None
    metric: Optional[str] = None
    search_alg: Optional[Searcher] = None
    scheduler: Optional[TrialScheduler] = None
    num_samples: int = 1
