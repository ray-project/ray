import copy
import cloudpickle as pickle
import itertools
import logging
import numpy as np
import os

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.search import SearchAlgorithm, SearcherInterface
from ray.tune.suggest.variant_generator import format_vars, resolve_nested_dict
from ray.tune.trial import Trial
from ray.tune.utils import merge_dicts, flatten_dict

INDEX = "__index__"

logger = logging.getLogger(__name__)


class _TrialGroup:
    """Class for grouping trials of same parameters.

    This is used when repeating trials for reducing training variance. See
    https://github.com/ray-project/ray/issues/6994.

    Args:
        primary_trial_id (str): Trial ID of the "primary trial".
            This trial is the one that the Searcher is aware of.
        trial_ids (List[str]): List of trial ids of the trials in the group.
            All trials should be using the same hyperparameters.
        config (dict): Suggested configuration shared across all trials
            in the trial group.
        metric (str): The training result objective value attribute.
        use_clipped_trials (bool): Whether to use early terminated trials
            in the calculation of the group aggregate score.

    """

    def __init__(self,
                 primary_trial_id,
                 trial_ids,
                 config,
                 metric,
                 use_clipped_trials=False):
        assert type(trial_ids) is list, (
            "trial_ids is not a list, got {}".format(trial_ids))
        assert type(config) is dict, (
            "config is not a dict, got {}".format(config))
        self.primary_trial_id = primary_trial_id
        self.trial_ids = trial_ids
        self.metric = metric
        self.config = config
        self.config_str = format_vars(self.config)
        self.use_clipped_trials = use_clipped_trials
        self._has_clipped_trial = False
        self._completed = {}
        self._errored = set()

    def score(self):
        """Current aggregated scores over completed trials.

        Returns:
            score (int): Mean over completed trial scores. Returns nan if
            no trials completed.
        """
        if self._completed:
            return np.nanmean(list(self._completed.values()))
        return np.nan

    def on_trial_complete(self, trial_id, result, error=False, clipped=False):
        """Stores the score for and keeps track of a completed trial.

        Stores the metric of a trial as nan if any of the following conditions
        are met:

            1. use_clipped_trials is False but the trial was clipped.
            2. ``result`` is empty or not provided.
            3. ``result`` is provided but no metric was provided.

        Args:
            trial_id (str): Trial ID of a trial in the group.
            result (dict): Result dictionary for the completed trial.
            error (bool): Whether the trial errored.
            clipped (bool): Whether the trial was stopped early by a scheduler.
        """
        assert trial_id in self.trial_ids

        if error:
            self._errored.add(trial_id)

        if clipped:
            self._has_clipped_trial = True
        if (not self.use_clipped_trials
                and clipped) or not result or (self.metric not in result):
            self._completed[trial_id] = np.nan
        else:
            self._completed[trial_id] = result[self.metric]

        logger.debug("Trial {} completed: Trial Group updated to {}".format(
            trial_id, repr(self)))

    def all_trials_errored(self):
        """Returns True if all trials in the group errored."""
        return len(self._errored) == len(self.trial_ids)

    def is_complete(self):
        """Returns True if all trials in the group have been returned."""
        return len(self._completed) == len(self.trial_ids)

    @property
    def clipped(self):
        """Returns True if any of the trials are terminated early."""
        return self._any_clipped

    def __repr__(self):
        return (
            "TrialGroup({config_str}; Total={total}; metric={metric}): "
            "Score={score} [Completed={completed} Errored={errored}]".format(
                config_str=self.config_str,
                metric=self.metric,
                total=len(self.trial_ids),
                score="{:.4f}".format(self.score),
                completed=len(self._completed),
                errored=len(self._errored),
            ))


class SearchGenerator(SearchAlgorithm):
    """Stateful generator for trials to be passed to the TrialRunner.

    Uses the provided ``searcher`` object to generate trials. This class
    transparently handles repeating trials with score aggregation
    without embedding logic into the Searcher.

    Args:
        searcher: Search object that subclasses the SearcherInterface. This
            is then used for generating new hyperparameter samples.

    """

    def __init__(self, searcher):
        assert issubclass(type(searcher), SearcherInterface), (
            "Searcher should be subclassing SearcherInterface.")
        self.searcher = searcher
        self.repeat = searcher.repeat
        self._max_concurrent = searcher.max_concurrent
        self._live_trial_groups = 0
        self._trial_groups = {}  # map primary_trial_id to group.
        self._trial_group_map = {}  # maps trial to corresponding group
        self._parser = make_parser()
        self._trial_generator = []
        self._counter = 0  # Keeps track of number of trials created.
        self._finished = False

    def _generate_trials(self, experiment_spec, output_path=""):
        """Generates trials with configurations from `suggest`.

        Creates a trial_id that is passed into `suggest`.

        Args:
            experiment_spec (dict): Dictionary of tune run configuration
                values generated by the Experiment object.
            output_path (str): Subdirectory under Experiment local_dir
                for trial output.

        Yields:
            Trial object constructed according to `spec`. This can be None
                if the searcher does not have new configurations or is
                at max_concurrency.
        """
        if "run" not in experiment_spec:
            raise TuneError("Must specify `run` in {}".format(experiment_spec))
        for _ in range(experiment_spec.get("num_samples", 1)):
            trials = None
            while trials is None:
                trials = self.create_trials(
                    experiment_spec, output_path, repeat=self.repeat)
                if not trials:
                    # This will repeat if trial is None.
                    yield None
            for trial in trials:
                yield trial

    def _create_trials(self, experiment_spec, output_path, repeat=None):
        if self.live_trial_groups >= self._max_concurrent:
            return
        primary_trial_id = Trial.generate_id()
        suggested_config = self.searcher.suggest(primary_trial_id)
        if suggested_config is None:
            return None

        trials = self._repeated_trial_creation(
            primary_trial_id, experiment_spec, suggested_config, output_path,
            repeat)

        child_trial_ids = [t.trial_id for t in trials]
        trial_group = _TrialGroup(
            primary_trial_id,
            child_trial_ids,
            suggested_config,
            self.searcher.metric,
            use_clipped_trials=self.searcher.use_clipped_trials)
        self._trial_groups[primary_trial_id] = trial_group
        self._live_trial_groups += 1

        for trial_id in child_trial_ids:
            self._trial_group_map[trial_id] = trial_group
        return trials

    def _repeated_trial_creation(self,
                                 primary_trial_id,
                                 experiment_spec,
                                 suggested_config,
                                 output_path,
                                 repeat=1):
        """Creates ``repeat`` number of trials at once.

        Also sets a tune.result.INDEX in the configuration which corresponds
        to the index of the repeated trial. This can be used for seeds.

        """

        trials = []
        for idx in range(repeat):
            spec = copy.deepcopy(experiment_spec)
            spec["config"] = merge_dicts(spec["config"],
                                         copy.deepcopy(suggested_config))

            # Create a new trial_id if duplicate trial is created
            trial_id = Trial.generate_id() if idx > 0 else primary_trial_id
            if repeat > 1:
                spec["config"].setdefault(INDEX, idx)
            flattened_config = resolve_nested_dict(spec["config"])
            self._counter += 1
            tag = "{0}_{1}".format(
                str(self._counter), format_vars(flattened_config))
            trials.append(
                create_trial_from_spec(
                    spec,
                    output_path,
                    self._parser,
                    evaluated_params=flatten_dict(suggested_config),
                    experiment_tag=tag,
                    trial_id=trial_id))
        return trials

    def add_configurations(self, experiments):
        """Chains generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        for experiment in experiment_list:
            self._trial_generator = itertools.chain(
                self._trial_generator,
                self._generate_trials(experiment.spec, experiment.name))

    def on_trial_result(self, trial_id, result):
        """Notifies the underlying searcher if trial_id is a primary trial.

        If repeat=1, all trials will be a primary trial.
        The searcher will not be notified of any trials that are not primary.
        """
        # We use primary trial ids as the key of self._trial_groups.
        if trial_id in self._trial_groups:
            self.searcher.on_trial_result(trial_id, result)

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          clipped=False):
        """Notification for the completion of trial.

        If all trials are complete in the trialgroup, the Searcher
        will receive the aggregated result of the "complete trial".

        The Searcher will receive a "nan" if all trials in the trial group
        errored. If only a few of the trials errored in the trial group,
        the searcher will receive an aggregated value over the valid results.

        If any trials in the trialgroup are clipped, the searcher will receive
        notification. The trial group will include clipped trials in the
        aggregate result only if specified via searcher.use_clipped_trials.
        """
        trial_group = self._trial_group_map[trial_id]
        trial_group.on_trial_complete(
            trial_id, result, error=False, clipped=clipped)

        if trial_group.is_complete():
            result = {self.searcher.metric: trial_group.score()}
            self.searcher.on_trial_complete(
                trial_group.primary_trial_id,
                result,
                error=trial_group.all_trials_errored(),
                clipped=trial_group.clipped)
            self._live_trial_groups -= 1

        assert self._live_trial_groups == (
            len(self._trial_group_map) - sum(
                group.is_complete() for group in self._trial_group_map)
        ), ("Trial group accounting is incorrect. Please report this issue on "
            "https://github.com/ray-project/ray/issues.")

    def is_finished(self):
        """Returns True with the TrialGenerator has no more items."""
        return self._finished

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

    def save(self, checkpoint_dir):
        with open(os.path.join(checkpoint_dir, ".search.data"), "wb") as f:
            pickle.dump(self._trial_groups, f)
        self.searcher.save(checkpoint_dir)

    def restore(self, checkpoint_dir):
        with open(os.path.join(checkpoint_dir, ".search.data"), "rb") as f:
            self._trial_groups = pickle.load(f)
        for group in self._trial_groups.values():
            for trial_id in group.trial_ids:
                self._trial_group_map[trial_id] = group
        self.searcher.restore(checkpoint_dir)
