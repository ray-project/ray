def create_scheduler(
  scheduler,
  metric='episode_reward_mean',
  mode='max',
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
