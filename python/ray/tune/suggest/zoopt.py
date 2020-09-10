import copy
import logging
from typing import Dict

import ray.cloudpickle as pickle
from ray.tune.sample import Categorical, Float, Integer, Quantized, Uniform
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils.util import unflatten_dict
from zoopt import ValueType

try:
    import zoopt
except ImportError:
    zoopt = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class ZOOptSearch(Searcher):
    """A wrapper around ZOOpt to provide trial suggestions.

    ZOOptSearch is a library for derivative-free optimization. It is backed by
    the `ZOOpt <https://github.com/polixir/ZOOpt>`__ package. Currently,
    Asynchronous Sequential RAndomized COordinate Shrinking (ASRacos)
    is implemented in Tune.

    To use ZOOptSearch, install zoopt (>=0.4.0): ``pip install -U zoopt``.

    Tune automatically converts search spaces to ZOOpt"s format:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.zoopt import ZOOptSearch

        "config": {
            "iterations": 10,  # evaluation times
            "width": tune.uniform(-10, 10),
            "height": tune.uniform(-10, 10)
        }

        zoopt_search = ZOOptSearch(
            algo="Asracos",  # only support Asracos currently
            budget=20,  # must match `num_samples` in `tune.run()`.
            dim_dict=dim_dict,
            metric="mean_loss",
            mode="min")

        tune.run(my_objective,
            config=config,
            search_alg=zoopt_search,
            name="zoopt_search",
            num_samples=20,
            stop={"timesteps_total": 10})

    If you would like to pass the search space manually, the code would
    look like this:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.zoopt import ZOOptSearch
        from zoopt import ValueType

        dim_dict = {
            "height": (ValueType.CONTINUOUS, [-10, 10], 1e-2),
            "width": (ValueType.DISCRETE, [-10, 10], False)
        }

        "config": {
            "iterations": 10,  # evaluation times
        }

        zoopt_search = ZOOptSearch(
            algo="Asracos",  # only support Asracos currently
            budget=20,  # must match `num_samples` in `tune.run()`.
            dim_dict=dim_dict,
            metric="mean_loss",
            mode="min")

        tune.run(my_objective,
            config=config,
            search_alg=zoopt_search,
            name="zoopt_search",
            num_samples=20,
            stop={"timesteps_total": 10})

    Parameters:
        algo (str): To specify an algorithm in zoopt you want to use.
            Only support ASRacos currently.
        budget (int): Number of samples.
        dim_dict (dict): Dimension dictionary.
            For continuous dimensions: (continuous, search_range, precision);
            For discrete dimensions: (discrete, search_range, has_order).
            More details can be found in zoopt package.
        metric (str): The training result objective value attribute.
            Defaults to "episode_reward_mean".
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
            Defaults to "min".

    """

    optimizer = None

    def __init__(self,
                 algo="asracos",
                 budget=None,
                 dim_dict=None,
                 metric=None,
                 mode=None,
                 **kwargs):
        assert zoopt is not None, "Zoopt not found - please install zoopt."
        assert budget is not None, "`budget` should not be None!"
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        _algo = algo.lower()
        assert _algo in ["asracos", "sracos"
                         ], "`algo` must be in ['asracos', 'sracos'] currently"

        self._algo = _algo
        self._dim_dict = dim_dict
        self._budget = budget

        self._metric = metric
        if mode == "max":
            self._metric_op = -1.
        elif mode == "min":
            self._metric_op = 1.
        self._live_trial_mapping = {}

        self._dim_keys = []
        self.solution_dict = {}
        self.best_solution_list = []
        self.optimizer = None

        super(ZOOptSearch, self).__init__(
            metric=self._metric, mode=mode, **kwargs)

        if self._dim_dict:
            self.setup_zoopt()

    def setup_zoopt(self):
        _dim_list = []
        for k in self._dim_dict:
            self._dim_keys.append(k)
            _dim_list.append(self._dim_dict[k])

        dim = zoopt.Dimension2(_dim_list)
        par = zoopt.Parameter(budget=self._budget)
        if self._algo == "sracos" or self._algo == "asracos":
            from zoopt.algos.opt_algorithms.racos.sracos import SRacosTune
            self.optimizer = SRacosTune(dimension=dim, parameter=par)

    def set_search_properties(self, metric, mode, config):
        if self._dim_dict:
            return False
        space = self.convert_search_space(config)
        self._dim_dict = space

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        if self._mode == "max":
            self._metric_op = -1.
        elif self._mode == "min":
            self._metric_op = 1.

        self.setup_zoopt()
        return True

    def suggest(self, trial_id):
        if not self._dim_dict or not self.optimizer:
            raise RuntimeError(
                "Trying to sample a configuration from {}, but no search "
                "space has been defined. Either pass the `{}` argument when "
                "instantiating the search algorithm, or pass a `config` to "
                "`tune.run()`.".format(self.__class__.__name__, "space"))

        _solution = self.optimizer.suggest()
        if _solution:
            self.solution_dict[str(trial_id)] = _solution
            _x = _solution.get_x()
            new_trial = dict(zip(self._dim_keys, _x))
            self._live_trial_mapping[trial_id] = new_trial
            return unflatten_dict(new_trial)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial."""
        if result:
            _solution = self.solution_dict[str(trial_id)]
            _best_solution_so_far = self.optimizer.complete(
                _solution, self._metric_op * result[self._metric])
            if _best_solution_so_far:
                self.best_solution_list.append(_best_solution_so_far)

        del self._live_trial_mapping[trial_id]

    def save(self, checkpoint_path):
        trials_object = self.optimizer
        with open(checkpoint_path, "wb") as output:
            pickle.dump(trials_object, output)

    def restore(self, checkpoint_path):
        with open(checkpoint_path, "rb") as input:
            trials_object = pickle.load(input)
        self.optimizer = trials_object

    @staticmethod
    def convert_search_space(spec: Dict):
        spec = copy.deepcopy(spec)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if not domain_vars and not grid_vars:
            return []

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a ZOOpt search space.")

        def resolve_value(domain):
            quantize = None

            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                quantize = sampler.q
                sampler = sampler.sampler

            if isinstance(domain, Float):
                precision = quantize or 1e-12
                if isinstance(sampler, Uniform):
                    return (ValueType.CONTINUOUS, [domain.lower, domain.upper],
                            precision)

            elif isinstance(domain, Integer):
                if isinstance(sampler, Uniform):
                    return (ValueType.DISCRETE, [domain.lower, domain.upper],
                            True)

            elif isinstance(domain, Categorical):
                # Categorical variables would use ValjeType.DISCRETE with
                # has_partial_order=False, however, currently we do not
                # keep track of category values and cannot automatically
                # translate back and forth between them.
                raise ValueError(
                    "ZOOpt does not support automatic conversion for "
                    "categorical variables. Please instantiate ZOOpt with "
                    "a manually defined search space.")

            raise ValueError("ZOOpt does not support parameters of type "
                             "`{}` with samplers of type `{}`".format(
                                 type(domain).__name__,
                                 type(domain.sampler).__name__))

        spec = {
            "/".join(path): resolve_value(domain)
            for path, domain in domain_vars
        }

        return spec
