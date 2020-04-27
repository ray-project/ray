import itertools
import copy

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.search import SearchAlgorithm
from ray.tune.suggest.variant_generator import format_vars, resolve_nested_dict
from ray.tune.trial import Trial
from ray.tune.utils import merge_dicts, flatten_dict


class SuggestionAlgorithm(SearchAlgorithm):
    """Abstract class for suggestion-based algorithms.

    Custom search algorithms can extend this class easily by overriding the
    `suggest` method provide generated parameters for the trials.

    To track suggestions and their corresponding evaluations, the method
    `suggest` will be passed a trial_id, which will be used in
    subsequent notifications.

    .. code-block:: python

        suggester = SuggestionAlgorithm()
        suggester.add_configurations({ ... })
        new_parameters = suggester.suggest()
        suggester.on_trial_complete(trial_id, result)
        better_parameters = suggester.suggest()
    """

    def __init__(self, metric=None, mode="max", use_early_stopped_trials=True):
        """Constructs a generator given experiment specifications."""
        self._parser = make_parser()
        self._trial_generator = []
        self._counter = 0
        self._metric = metric
        assert mode in ["min", "max"]
        self._mode = mode
        self._use_early_stopped = use_early_stopped_trials
        self._finished = False

    def add_configurations(self, experiments):
        """Chains generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        for experiment in experiment_list:
            self._trial_generator = itertools.chain(
                self._trial_generator,
                self._generate_trials(
                    experiment.spec.get("num_samples", 1), experiment.spec,
                    experiment.name))

    def next_trials(self):
        """Provides a batch of Trial objects to be queued into the TrialRunner.

        A batch ends when self._trial_generator returns None.

        Returns:
            trials (list): Returns a list of trials.
        """
        trials = []

        for trial in self._trial_generator:
            if trial is None:
                return trials
            trials += [trial]

        self.set_finished()
        return trials

    def _generate_trials(self, num_samples, experiment_spec, output_path=""):
        """Generates trials with configurations from `suggest`.

        Creates a trial_id that is passed into `suggest`.

        Yields:
            Trial objects constructed according to `spec`
        """
        if "run" not in experiment_spec:
            raise TuneError("Must specify `run` in {}".format(experiment_spec))
        for _ in range(num_samples):
            trial_id = Trial.generate_id()
            while True:
                suggested_config = self.suggest(trial_id)
                if suggested_config is None:
                    yield None
                else:
                    break
            spec = copy.deepcopy(experiment_spec)
            spec["config"] = merge_dicts(spec["config"],
                                         copy.deepcopy(suggested_config))
            flattened_config = resolve_nested_dict(spec["config"])
            self._counter += 1
            tag = "{0}_{1}".format(
                str(self._counter), format_vars(flattened_config))
            yield create_trial_from_spec(
                spec,
                output_path,
                self._parser,
                evaluated_params=flatten_dict(suggested_config),
                experiment_tag=tag,
                trial_id=trial_id)

    def suggest(self, trial_id):
        """Queries the algorithm to retrieve the next set of parameters.

        Arguments:
            trial_id: Trial ID used for subsequent notifications.

        Returns:
            dict|None: Configuration for a trial, if possible.
                Else, returns None, which will temporarily stop the
                TrialRunner from querying.

        Example:
            >>> suggester = SuggestionAlgorithm(max_concurrent=1)
            >>> suggester.add_configurations({ ... })
            >>> parameters_1 = suggester.suggest()
            >>> parameters_2 = suggester.suggest()
            >>> parameters_2 is None
            >>> suggester.on_trial_complete(trial_id, result)
            >>> parameters_2 = suggester.suggest()
            >>> parameters_2 is not None
        """
        raise NotImplementedError

    def save(self, checkpoint_dir):
        raise NotImplementedError

    def restore(self, checkpoint_dir):
        raise NotImplementedError

    @property
    def metric(self):
        """The training result objective value attribute."""
        return self._metric

    @property
    def mode(self):
        """Specifies if minimizing or maximizing the metric."""
        return self._mode


class _MockSuggestionAlgorithm(SuggestionAlgorithm):
    def __init__(self, max_concurrent=2, **kwargs):
        self._max_concurrent = max_concurrent
        self.live_trials = {}
        self.counter = {"result": 0, "complete": 0}
        self.final_results = []
        self.stall = False
        self.results = []
        super(_MockSuggestionAlgorithm, self).__init__(**kwargs)

    def suggest(self, trial_id):
        if len(self.live_trials) < self._max_concurrent and not self.stall:
            self.live_trials[trial_id] = 1
            return {"test_variable": 2}
        return None

    def on_trial_result(self, trial_id, result):
        self.counter["result"] += 1
        self.results += [result]

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        self.counter["complete"] += 1
        if result:
            self._process_result(result, early_terminated)
        del self.live_trials[trial_id]

    def _process_result(self, result, early_terminated):
        if early_terminated and self._use_early_stopped:
            self.final_results += [result]
