"""BOHB (Bayesian Optimization with HyperBand)"""

import copy
import logging

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class _BOHBJobWrapper():
    """Mock object for HpBandSter to process."""

    def __init__(self, loss, budget, config):
        self.result = {"loss": loss}
        self.kwargs = {"budget": budget, "config": config.copy()}
        self.exception = None


class TuneBOHB(Searcher):
    """BOHB suggestion component.


    Requires HpBandSter and ConfigSpace to be installed. You can install
    HpBandSter and ConfigSpace with: ``pip install hpbandster ConfigSpace``.

    This should be used in conjunction with HyperBandForBOHB.

    Args:
        space (ConfigurationSpace): Continuous ConfigSpace search space.
            Parameters will be sampled from this space which will be used
            to run trials.
        bohb_config (dict): configuration for HpBandSter BOHB algorithm
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.

    Example:

    .. code-block:: python

        import ConfigSpace as CS

        config_space = CS.ConfigurationSpace()
        config_space.add_hyperparameter(
            CS.UniformFloatHyperparameter('width', lower=0, upper=20))
        config_space.add_hyperparameter(
            CS.UniformFloatHyperparameter('height', lower=-100, upper=100))
        config_space.add_hyperparameter(
            CS.CategoricalHyperparameter(
                name='activation', choices=['relu', 'tanh']))

        algo = TuneBOHB(
            config_space, max_concurrent=4, metric='mean_loss', mode='min')
        bohb = HyperBandForBOHB(
            time_attr='training_iteration',
            metric='mean_loss',
            mode='min',
            max_t=100)
        run(MyTrainableClass, scheduler=bohb, search_alg=algo)

    """

    def __init__(self,
                 space,
                 bohb_config=None,
                 max_concurrent=10,
                 metric="neg_mean_loss",
                 mode="max"):
        from hpbandster.optimizers.config_generators.bohb import BOHB
        assert BOHB is not None, "HpBandSter must be installed!"
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self._max_concurrent = max_concurrent
        self.trial_to_params = {}
        self.running = set()
        self.paused = set()
        self._metric = metric
        if mode == "max":
            self._metric_op = -1.
        elif mode == "min":
            self._metric_op = 1.
        bohb_config = bohb_config or {}
        self.bohber = BOHB(space, **bohb_config)
        super(TuneBOHB, self).__init__(metric=self._metric, mode=mode)

    def suggest(self, trial_id):
        if len(self.running) < self._max_concurrent:
            # This parameter is not used in hpbandster implementation.
            config, info = self.bohber.get_config(None)
            self.trial_to_params[trial_id] = copy.deepcopy(config)
            self.running.add(trial_id)
            return config
        return None

    def on_trial_result(self, trial_id, result):
        if trial_id not in self.paused:
            self.running.add(trial_id)
        if "hyperband_info" not in result:
            logger.warning("BOHB Info not detected in result. Are you using "
                           "HyperBandForBOHB as a scheduler?")
        elif "budget" in result.get("hyperband_info", {}):
            hbs_wrapper = self.to_wrapper(trial_id, result)
            self.bohber.new_result(hbs_wrapper)

    def on_trial_complete(self, trial_id, result=None, error=False):
        del self.trial_to_params[trial_id]
        if trial_id in self.paused:
            self.paused.remove(trial_id)
        if trial_id in self.running:
            self.running.remove(trial_id)

    def to_wrapper(self, trial_id, result):
        return _BOHBJobWrapper(self._metric_op * result[self.metric],
                               result["hyperband_info"]["budget"],
                               self.trial_to_params[trial_id])

    def on_pause(self, trial_id):
        self.paused.add(trial_id)
        self.running.remove(trial_id)

    def on_unpause(self, trial_id):
        self.paused.remove(trial_id)
        self.running.add(trial_id)
