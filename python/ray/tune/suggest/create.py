def create_searcher(
  search_alg, 
  metric='episode_reward_mean', 
  mode='max', 
  **kwargs,
):
    def _import_ax_search():
        from ray.tune.suggest.ax import AxSearch
        return AxSearch

    def _import_dragonfly_search():
        from ray.tune.suggest.dragonfly import DragonflySearch
        return DragonflySearch

    def _import_skopt_search():
        from ray.tune.suggest.skopt import SkOptSearch
        return SkOptSearch

    def _import_hyperopt_search():
        from ray.tune.suggest import HyperOptSearch
        return HyperOptSearch

    def _import_bayesopt_search():
        from ray.tune.suggest.bayesopt import BayesOptSearch
        return BayesOptSearch

    def _import_bohb_search():
        from ray.tune.suggest.bohb import TuneBOHB
        return TuneBOHB

    def _import_nevergrad_search():
        from ray.tune.suggest.nevergrad import NevergradSearch
        return NevergradSearch

    def _import_optuna_search():
        from ray.tune.suggest.optuna import OptunaSearch
        return OptunaSearch

    def _import_zoopt_search():
        from ray.tune.suggest.zoopt import ZOOptSearch
        return ZOOptSearch

    def _import_sigopt_search():
        from ray.tune.suggest.sigopt import SigOptSearch
        return SigOptSearch

    SEARCH_ALG_IMPORT = {
        "ax": _import_ax_search,
        "dragonfly": _import_dragonfly_search,
        "skopt": _import_skopt_search,
        "hyperopt": _import_hyperopt_search,
        "bayesopt": _import_bayesopt_search,
        "bohb": _import_bohb_search,
        "nevergrad": _import_nevergrad_search,
        "optuna": _import_optuna_search,
        "zoopt": _import_zoopt_search,
        "sigopt": _import_sigopt_search,
    }
    return SEARCH_ALG_IMPORT[search_alg](**kwargs)
