import copy
import logging
import pickle

try:  # Python 3 only -- needed for lint test.
    from zoopt import Dimension2, Parameter
    from zoopt.algos.opt_algorithms.racos.sracos import SRacosTune
except ImportError:
    zoopt = None

from ray.tune.suggest.suggestion import SuggestionAlgorithm

logger = logging.getLogger(__name__)


class ZOOptSearch(SuggestionAlgorithm):

    optimizer = None

    def __init__(self,
                 algo="asracos",
                 budget=None,
                 dim_dict=None,
                 max_concurrent=10,
                 metric="episode_reward_mean",
                 mode="min",
                 **kwargs):

        assert budget is not None, "`budget` should not be None!"
        assert dim_dict is not None, "`dim_list` should not be None!"
        assert type(max_concurrent) is int and max_concurrent > 0
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        _algo = algo.lower()
        assert _algo in ["asracos", "sracos"
                         ], "`algo` must be in ['asracos', 'sracos'] currently"

        self._max_concurrent = max_concurrent
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

        dim = Dimension2(_dim_list)
        par = Parameter(budget=budget)
        if _algo == "sracos" or _algo == "asracos":
            self.optimizer = SRacosTune(dimension=dim, parameter=par)

        self.solution_dict = {}
        self.best_solution_list = []

        super(ZOOptSearch, self).__init__(
            metric=self._metric, mode=mode, **kwargs)

    def suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None

        _solution = self.optimizer.suggest()
        if _solution:
            self.solution_dict[str(trial_id)] = _solution
            _x = _solution.get_x()
            new_trial = dict(zip(self._dim_keys, _x))
            self._live_trial_mapping[trial_id] = new_trial
            return copy.deepcopy(new_trial)

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Notification for the completion of trial."""
        if result:
            _solution = self.solution_dict[str(trial_id)]
            _best_solution_so_far = self.optimizer.complete(
                _solution, self._metric_op * result[self._metric])
            if _best_solution_so_far:
                self.best_solution_list.append(_best_solution_so_far)
            self._process_result(trial_id, result, early_terminated)

        del self._live_trial_mapping[trial_id]

    def _process_result(self, trial_id, result, early_terminated=False):
        if early_terminated and self._use_early_stopped is False:
            return

    def _num_live_trials(self):
        return len(self._live_trial_mapping)

    def save(self, checkpoint_dir):
        trials_object = self.optimizer
        with open(checkpoint_dir, "wb") as output:
            pickle.dump(trials_object, output)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as input:
            trials_object = pickle.load(input)
        self.optimizer = trials_object
