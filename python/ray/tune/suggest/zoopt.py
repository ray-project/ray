import copy
import logging
import ray.cloudpickle as pickle

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

    .. code-block:: python

        from ray.tune import run
        from ray.tune.suggest.zoopt import ZOOptSearch
        from zoopt import ValueType

        dim_dict = {
            "height": (ValueType.CONTINUOUS, [-10, 10], 1e-2),
            "width": (ValueType.DISCRETE, [-10, 10], False)
        }

        config = {
            "num_samples": 200,
            "config": {
                "iterations": 10,  # evaluation times
            },
            "stop": {
                "timesteps_total": 10  # cumstom stop rules
            }
        }

        zoopt_search = ZOOptSearch(
            algo="Asracos",  # only support Asracos currently
            budget=config["num_samples"],
            dim_dict=dim_dict,
            metric="mean_loss",
            mode="min")

        run(my_objective,
            search_alg=zoopt_search,
            name="zoopt_search",
            **config)

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
                 metric="episode_reward_mean",
                 mode="min",
                 **kwargs):
        assert zoopt is not None, "Zoopt not found - please install zoopt."
        assert budget is not None, "`budget` should not be None!"
        assert dim_dict is not None, "`dim_list` should not be None!"
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        _algo = algo.lower()
        assert _algo in ["asracos", "sracos"
                         ], "`algo` must be in ['asracos', 'sracos'] currently"

        self._metric = metric
        if mode == "max":
            self._metric_op = -1.
        elif mode == "min":
            self._metric_op = 1.
        self._live_trial_mapping = {}

        self._dim_keys = []
        _dim_list = []
        for k in dim_dict:
            self._dim_keys.append(k)
            _dim_list.append(dim_dict[k])

        dim = zoopt.Dimension2(_dim_list)
        par = zoopt.Parameter(budget=budget)
        if _algo == "sracos" or _algo == "asracos":
            from zoopt.algos.opt_algorithms.racos.sracos import SRacosTune
            self.optimizer = SRacosTune(dimension=dim, parameter=par)

        self.solution_dict = {}
        self.best_solution_list = []

        super(ZOOptSearch, self).__init__(
            metric=self._metric, mode=mode, **kwargs)

    def suggest(self, trial_id):
        _solution = self.optimizer.suggest()
        if _solution:
            self.solution_dict[str(trial_id)] = _solution
            _x = _solution.get_x()
            new_trial = dict(zip(self._dim_keys, _x))
            self._live_trial_mapping[trial_id] = new_trial
            return copy.deepcopy(new_trial)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial."""
        if result:
            _solution = self.solution_dict[str(trial_id)]
            _best_solution_so_far = self.optimizer.complete(
                _solution, self._metric_op * result[self._metric])
            if _best_solution_so_far:
                self.best_solution_list.append(_best_solution_so_far)

        del self._live_trial_mapping[trial_id]

    def save(self, checkpoint_dir):
        trials_object = self.optimizer
        with open(checkpoint_dir, "wb") as output:
            pickle.dump(trials_object, output)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as input:
            trials_object = pickle.load(input)
        self.optimizer = trials_object
