import inspect

from ray._common.utils import get_function_args
from ray.tune.schedulers.async_hyperband import ASHAScheduler, AsyncHyperBandScheduler
from ray.tune.schedulers.hb_bohb import HyperBandForBOHB
from ray.tune.schedulers.hyperband import HyperBandScheduler
from ray.tune.schedulers.median_stopping_rule import MedianStoppingRule
from ray.tune.schedulers.pbt import (
    PopulationBasedTraining,
    PopulationBasedTrainingReplay,
)
from ray.tune.schedulers.resource_changing_scheduler import ResourceChangingScheduler
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.util import PublicAPI


def _pb2_importer():
    # PB2 is imported lazily since it has additional dependencies.
    from ray.tune.schedulers.pb2 import PB2

    return PB2


# Values in this dictionary will be one two kinds:
#    class of the scheduler object to create
#    wrapper function to support a lazy import of the scheduler class
SCHEDULER_IMPORT = {
    "fifo": FIFOScheduler,
    "async_hyperband": AsyncHyperBandScheduler,
    "asynchyperband": AsyncHyperBandScheduler,
    "median_stopping_rule": MedianStoppingRule,
    "medianstopping": MedianStoppingRule,
    "hyperband": HyperBandScheduler,
    "hb_bohb": HyperBandForBOHB,
    "pbt": PopulationBasedTraining,
    "pbt_replay": PopulationBasedTrainingReplay,
    "pb2": _pb2_importer,
    "resource_changing": ResourceChangingScheduler,
}


@PublicAPI(stability="beta")
def create_scheduler(
    scheduler,
    **kwargs,
):
    """Instantiate a scheduler based on the given string.

    This is useful for swapping between different schedulers.

    Args:
        scheduler: The scheduler to use.
        **kwargs: Scheduler parameters.
            These keyword arguments will be passed to the initialization
            function of the chosen scheduler.
    Returns:
        ray.tune.schedulers.trial_scheduler.TrialScheduler: The scheduler.
    Example:
        >>> from ray import tune
        >>> pbt_kwargs = {}
        >>> scheduler = tune.create_scheduler('pbt', **pbt_kwargs) # doctest: +SKIP
    """

    scheduler = scheduler.lower()
    if scheduler not in SCHEDULER_IMPORT:
        raise ValueError(
            f"The `scheduler` argument must be one of "
            f"{list(SCHEDULER_IMPORT)}. "
            f"Got: {scheduler}"
        )

    SchedulerClass = SCHEDULER_IMPORT[scheduler]

    if inspect.isfunction(SchedulerClass):
        # invoke the wrapper function to retrieve class
        SchedulerClass = SchedulerClass()

    scheduler_args = get_function_args(SchedulerClass)
    trimmed_kwargs = {k: v for k, v in kwargs.items() if k in scheduler_args}

    return SchedulerClass(**trimmed_kwargs)


__all__ = [
    "TrialScheduler",
    "HyperBandScheduler",
    "AsyncHyperBandScheduler",
    "ASHAScheduler",
    "MedianStoppingRule",
    "FIFOScheduler",
    "PopulationBasedTraining",
    "PopulationBasedTrainingReplay",
    "HyperBandForBOHB",
    "ResourceChangingScheduler",
]
