from ray.tune.schedulers.trial_scheduler import TrialScheduler, FIFOScheduler
from ray.tune.schedulers.hyperband import HyperBandScheduler
from ray.tune.schedulers.hb_bohb import HyperBandForBOHB
from ray.tune.schedulers.async_hyperband import (AsyncHyperBandScheduler,
                                                 ASHAScheduler)
from ray.tune.schedulers.median_stopping_rule import MedianStoppingRule
from ray.tune.schedulers.pbt import (PopulationBasedTraining,
                                     PopulationBasedTrainingReplay)


def create_scheduler(
        scheduler,
        metric=None,
        mode=None,
        **kwargs,
):
    """Instantiate a scheduler based on the given string.

    This is useful for swapping between different schedulers.

    Args:
        scheduler (str): The scheduler to use.
        metric (str): The training result objective value attribute. Stopping
            procedures will use this attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        **kwargs: Additional parameters.
            These keyword arguments will be passed to the initialization
            function of the chosen class.
    Returns:
        ray.tune.schedulers.trial_scheduler.TrialScheduler: The scheduler.
    Example:
        >>> scheduler = tune.create_scheduler('pbt')
    """

    SCHEDULER_IMPORT = {
        "async_hyperband": AsyncHyperBandScheduler,
        "median_stopping_rule": MedianStoppingRule,
        "hyperband": HyperBandScheduler,
        "hb_bohb": HyperBandForBOHB,
        "pbt": PopulationBasedTraining,
    }
    scheduler = scheduler.lower()
    if scheduler not in SCHEDULER_IMPORT:
        raise ValueError(
            f"Search alg must be one of {list(SCHEDULER_IMPORT)}. "
            f"Got: {scheduler}")

    SchedulerClass = SCHEDULER_IMPORT[scheduler]
    return SchedulerClass(metric=metric, mode=mode, **kwargs)


__all__ = [
    "TrialScheduler", "HyperBandScheduler", "AsyncHyperBandScheduler",
    "ASHAScheduler", "MedianStoppingRule", "FIFOScheduler",
    "PopulationBasedTraining", "PopulationBasedTrainingReplay",
    "HyperBandForBOHB"
]
