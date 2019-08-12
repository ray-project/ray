"""BOHB (Bayesian Optimization with HyperBand)"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
try:
    from hpbandster import BOHB
except ImportError:
    hpbandster = None

from ray.tune.suggest import SuggestionAlgorithm

logger = logging.getLogger(__name__)


class JobWrapper():
    """Dummy class"""

    def __init__(self, loss, budget, config):
        self.result = {"loss": loss}
        self.kwargs = {"budget": budget, "config": config.copy()}
        self.exception = None


class TuneBOHB(SuggestionAlgorithm):
    """BOHB suggestion component.

    Requires HpBandSter and ConfigSpace to be installed.
    You can install HpBandSter and ConfigSpace with:

        `pip install hpbandster ConfigSpace`

    This should be used in conjunction with HyperBandForBOHB.

    Parameters:
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
        CS = ConfigSpace
        config_space = CS.ConfigurationSpace()
        config_space.add_hyperparameter(
            CS.UniformFloatHyperparameter('width', lower=0, upper=20))
        config_space.add_hyperparameter(
            CS.UniformFloatHyperparameter('height', lower=-100, upper=100))
        config_space.add_hyperparameter(
            CS.CategoricalHyperparameter(
                name='activation', choices=['relu', 'tanh']))
        algo = TuneBOHB(
            config_space, max_concurrent=4, metric="mean_loss", mode="min")
        bohb = HyperBandForBOHB(
            time_attr="training_iteration",
            metric="mean_loss",
            mode="min",
            max_t=100)
        run(MyTrainableClass, scheduler=bohb, search_alg=algo)

    """

    def __init__(self,
                 space,
                 bohb_config=None,
                 max_concurrent=10,
                 metric="neg_mean_loss",
                 mode="max"):
        assert hpbandster is not None, "HpBandSter must be installed!"
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self._max_concurrent = max_concurrent
        self.trial_to_params = {}
        self.running = set()
        self.paused = set()
        self.metric = metric
        if mode == "max":
            self._metric_op = -1.
        elif mode == "min":
            self._metric_op = 1.
        bohb_config = bohb_config or {}
        self.bohber = BOHB(space, **bohb_config)
        super(TuneBOHB, self).__init__()

    def _suggest(self, trial_id):
        if len(self.running) < self._max_concurrent:
            config, info = self.bohber.get_config()
            self.trial_to_params[trial_id] = list(config)
            self.running.add(trial_id)
            return config
        return None

    def on_trial_result(self, trial_id, result):
        if trial_id not in self.paused:
            self.running.add(trial_id)
        if "budget" in result.get("hyperband_info", {}):
            hbs_wrapper = self.to_wrapper(trial_id, result)
            self.bohber.new_result(hbs_wrapper)
        else:
            logger.warning("BOHB Info not detected in result. Are you using "
                           "HyperBandForBOHB as a scheduler?")

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        del self.trial_to_params[trial_id]
        if trial_id in self.paused:
            self.paused.remove(trial_id)
        if trial_id in self.running:
            self.running.remove(trial_id)

    def to_wrapper(self, trial_id, result):
        return JobWrapper(
            self._metric_op * result[self.metric],
            result["hyperband_info"]["budget"],
            {k: result["config"][k]
             for k in self.trial_to_params[trial_id]})

    def on_pause(self, trial_id):
        self.paused.add(trial_id)
        self.running.remove(trial_id)

    def on_unpause(self, trial_id):
        self.paused.remove(trial_id)
        self.running.add(trial_id)
