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
  metric="episode_reward_mean",
  mode="max",
  **kwargs,
):
    def _import_async_hyperband_scheduler():
        from ray.tune.schedulers import AsyncHyperBandScheduler
        return AsyncHyperBandScheduler

    def _import_median_stopping_rule_scheduler():
        from ray.tune.schedulers import MedianStoppingRule
        return MedianStoppingRule

    def _import_hyperband_scheduler():
        from ray.tune.schedulers import HyperBandScheduler
        return HyperBandScheduler

    def _import_hb_bohb_scheduler():
        from ray.tune.schedulers import HyperBandForBOHB
        return HyperBandForBOHB

    def _import_pbt_search():
        from ray.tune.schedulers import PopulationBasedTraining
        return PopulationBasedTraining

    SCHEDULER_IMPORT = {
        "async_hyperband": _import_async_hyperband_scheduler,
        "median_stopping_rule": _import_median_stopping_rule_scheduler,
        "hyperband": _import_hyperband_scheduler,
        "hb_bohb": _import_hb_bohb_scheduler,
        "pbt": _import_pbt_search,
    }
    return SCHEDULER_IMPORT[scheduler](metric=metric, mode=mode, **kwargs)

__all__ = [
    "TrialScheduler", "HyperBandScheduler", "AsyncHyperBandScheduler",
    "ASHAScheduler", "MedianStoppingRule", "FIFOScheduler",
    "PopulationBasedTraining", "PopulationBasedTrainingReplay",
    "HyperBandForBOHB"
]
