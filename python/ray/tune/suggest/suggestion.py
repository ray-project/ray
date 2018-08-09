from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain
import copy

from ray.tune.error import TuneError
from ray.tune.trial import Trial
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.search import SearchAlgorithm
from ray.tune.suggest.variant_generator import format_vars


class SuggestionAlgorithm(SearchAlgorithm):
    """Abstract class for suggestion-based algorithms.

    Custom search algorithms can extend this class easily by overriding the
    `_suggest` method provide generated parameters for the trials.

    To track suggestions and their corresponding evaluations, the method
    `_suggest` will be passed a trial_id, which will be used in
    subsequent notifications.

    Example:
        >>> suggester = SuggestionAlgorithm({ ... })
        >>> new_parameters = suggester._suggest()
        >>> suggester.on_trial_complete(trial_id, result)
        >>> better_parameters = suggester._suggest()
    """

    def __init__(self, experiments=None):
        """Constructs a generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        self._parser = make_parser()
        self._trial_generator = chain.from_iterable([
            self._generate_trials(experiment.spec, experiment.name)
            for experiment in experiment_list
        ])
        self._finished = False

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

        self._finished = True
        return trials

    def _generate_trials(self, experiment_spec, output_path=""):
        """Generates trials with configurations from `_suggest`.

        Creates a trial_id that is passed into `_suggest`.

        Yields:
            Trial objects constructed according to `spec`
        """
        if "run" not in experiment_spec:
            raise TuneError("Must specify `run` in {}".format(experiment_spec))
        for _ in range(experiment_spec.get("repeat", 1)):
            trial_id = Trial.generate_id()
            while True:
                suggested_config = self._suggest(trial_id)
                if suggested_config is None:
                    yield None
                else:
                    break
            spec = copy.deepcopy(experiment_spec)
            spec["config"] = suggested_config
            yield create_trial_from_spec(
                spec,
                output_path,
                self._parser,
                experiment_tag=format_vars(spec["config"]),
                trial_id=trial_id)

    def is_finished(self):
        return self._finished

    def _suggest(self, trial_id):
        """Queries the algorithm to retrieve the next set of parameters.

        Arguments:
            trial_id: Trial ID used for subsequent notifications.

        Returns:
            dict|None: Configuration for a trial, if possible.
                Else, returns None, which will temporarily stop the
                TrialRunner from querying.

        Example:
            >>> suggester = SuggestionAlgorithm({ ... }, max_concurrent=1)
            >>> parameters_1 = suggester._suggest()
            >>> parameters_2 = suggester._suggest()
            >>> parameters_2 is None
            >>> suggester.on_trial_complete(trial_id, result)
            >>> parameters_2 = suggester._suggest()
            >>> parameters_2 is not None
        """
        raise NotImplementedError


class _MockSuggestionAlgorithm(SuggestionAlgorithm):
    def __init__(self, experiments, max_concurrent=2, **kwargs):
        self._max_concurrent = max_concurrent
        self.live_trials = {}
        self.counter = {"result": 0, "complete": 0}
        self.stall = False
        super(_MockSuggestionAlgorithm, self).__init__(experiments, **kwargs)

    def _suggest(self, trial_id):
        if len(self.live_trials) < self._max_concurrent and not self.stall:
            self.live_trials[trial_id] = 1
            return {"test_variable": 2}
        return None

    def on_trial_result(self, trial_id, result):
        self.counter["result"] += 1

    def on_trial_complete(self, trial_id, **kwargs):
        self.counter["complete"] += 1
        del self.live_trials[trial_id]
