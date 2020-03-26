import copy
import cloudpickle as pickle
import itertools
import logging
import numpy as np
import os

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.search import SearchAlgorithm, Searcher
from ray.tune.suggest.variant_generator import format_vars, resolve_nested_dict
from ray.tune.trial import Trial
from ray.tune.utils import merge_dicts, flatten_dict

INDEX = "__index__"

logger = logging.getLogger(__name__)


class SearchGenerator(SearchAlgorithm):
    """Stateful generator for trials to be passed to the TrialRunner.

    Uses the provided ``searcher`` object to generate trials. This class
    transparently handles repeating trials with score aggregation
    without embedding logic into the Searcher.

    Args:
        searcher: Search object that subclasses the Searcher base class. This
            is then used for generating new hyperparameter samples.
    """

    def __init__(self, searcher):
        assert issubclass(
            type(searcher),
            Searcher), ("Searcher should be subclassing Searcher.")
        self.searcher = searcher
        self._parser = make_parser()
        self._trial_generator = []
        self._counter = 0  # Keeps track of number of trials created.
        self._total_samples = 0  # int: total samples to evaluate.
        self._finished = False

    def add_configurations(self, experiments):
        """Chains generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        self._experiment = experiment_list[0]
        assert len(experiment_list) == 1, (
            "SearchAlgorithms can only support 1 experiment at a time.")
        experiment_spec = self._experiment.spec
        self.total_samples = experiment_spec.get("num_samples", 1)
        if "run" not in experiment_spec:
            raise TuneError("Must specify `run` in {}".format(experiment_spec))

    def next_trials(self):
        """Provides a batch of Trial objects to be queued into the TrialRunner.

        Returns:
            List[Trial]: A list of trials for the Runner to consume.
        """
        trials = []
        while not self.is_finished():
            trial = self.create_trial_if_possible(
                experiment_spec, self._experiment.name)
            if trial is None:
                break
            trials.append(trial)

    def create_trial_if_possible(self, experiment_spec, output_path):
        if self.live_trial_groups >= self._max_concurrent:
            return
        trial_id = Trial.generate_id()
        suggested_config = self.searcher.suggest(trial_id)
        if suggested_config is None:
            return
        spec = copy.deepcopy(experiment_spec)
        spec["config"] = merge_dicts(spec["config"],
                                     copy.deepcopy(suggested_config))

        # Create a new trial_id if duplicate trial is created
        flattened_config = resolve_nested_dict(spec["config"])
        self._counter += 1
        tag = "{0}_{1}".format(
            str(self._counter), format_vars(flattened_config))
        trial = create_trial_from_spec(
                spec,
                output_path,
                self._parser,
                evaluated_params=flatten_dict(suggested_config),
                experiment_tag=tag,
                trial_id=trial_id)
        return trial

    def on_trial_result(self, trial_id, result):
        """Notifies the underlying searcher."""
        self.searcher.on_trial_result(trial_id, result)

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          clipped=False):
        self.searcher.on_trial_complete(
            trial_id=trial_id,
            result=result,
            error=error,
            clipped=clipped)
