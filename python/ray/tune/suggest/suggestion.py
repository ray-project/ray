import copy
import logging

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.search import SearchAlgorithm
from ray.tune.suggest.variant_generator import format_vars, resolve_nested_dict
from ray.tune.trial import Trial
from ray.tune.utils import merge_dicts, flatten_dict

logger = logging.getLogger(__name__)


def _warn_on_repeater(searcher, total_samples):
    from ray.tune.suggest.repeater import _warn_num_samples
    _warn_num_samples(searcher, total_samples)


class Searcher:
    """Abstract class for wrapping suggesting algorithms.

    Custom algorithms can extend this class easily by overriding the
    `suggest` method provide generated parameters for the trials.

    Any subclass that implements ``__init__`` must also call the
    constructor of this class: ``super(Subclass, self).__init__(...)``.

    To track suggestions and their corresponding evaluations, the method
    `suggest` will be passed a trial_id, which will be used in
    subsequent notifications.

    Args:
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.

    .. code-block:: python

        class ExampleSearch(Searcher):
            def __init__(self, metric="mean_loss", mode="min", **kwargs):
                super(ExampleSearch, self).__init__(
                    metric=metric, mode=mode, **kwargs)
                self.optimizer = Optimizer()
                self.configurations = {}

            def suggest(self, trial_id):
                configuration = self.optimizer.query()
                self.configurations[trial_id] = configuration

            def on_trial_complete(self, trial_id, result, **kwargs):
                configuration = self.configurations[trial_id]
                if result and self.metric in result:
                    self.optimizer.update(configuration, result[self.metric])

        tune.run(trainable_function, search_alg=ExampleSearch())


    """

    def __init__(self,
                 metric="episode_reward_mean",
                 mode="max",
                 max_concurrent=None,
                 use_early_stopped_trials=None):
        if use_early_stopped_trials is False:
            raise DeprecationWarning(
                "Early stopped trials are now always used. If this is a "
                "problem, file an issue: https://github.com/ray-project/ray.")
        if max_concurrent is not None:
            logger.warning(
                "DeprecationWarning: `max_concurrent` is deprecated for this "
                "search algorithm. Use tune.suggest.ConcurrencyLimiter() "
                "instead. This will raise an error in future versions of Ray.")
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self._metric = metric
        self._mode = mode

    def on_trial_result(self, trial_id, result):
        """Optional notification for result during training.

        Note that by default, the result dict may include NaNs or
        may not include the optimization metric. It is up to the
        subclass implementation to preprocess the result to
        avoid breaking the optimization process.

        Args:
            trial_id (str): A unique string ID for the trial.
            result (dict): Dictionary of metrics for current training progress.
                Note that the result dict may include NaNs or
                may not include the optimization metric. It is up to the
                subclass implementation to preprocess the result to
                avoid breaking the optimization process.
        """
        pass

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        Typically, this method is used for notifying the underlying
        optimizer of the result.

        Args:
            trial_id (str): A unique string ID for the trial.
            result (dict): Dictionary of metrics for current training progress.
                Note that the result dict may include NaNs or
                may not include the optimization metric. It is up to the
                subclass implementation to preprocess the result to
                avoid breaking the optimization process. Upon errors, this
                may also be None.
            error (bool): True if the training process raised an error.

        """
        raise NotImplementedError

    def suggest(self, trial_id):
        """Queries the algorithm to retrieve the next set of parameters.

        Arguments:
            trial_id (str): Trial ID used for subsequent notifications.

        Returns:
            dict|None: Configuration for a trial, if possible.

        """
        raise NotImplementedError

    def save(self, checkpoint_dir):
        """Save function for this object."""
        raise NotImplementedError

    def restore(self, checkpoint_dir):
        """Restore function for this object."""
        raise NotImplementedError

    @property
    def metric(self):
        """The training result objective value attribute."""
        return self._metric

    @property
    def mode(self):
        """Specifies if minimizing or maximizing the metric."""
        return self._mode


class ConcurrencyLimiter(Searcher):
    """A wrapper algorithm for limiting the number of concurrent trials.

    Args:
        searcher (Searcher): Searcher object that the
            ConcurrencyLimiter will manage.

    Example:

    .. code-block:: python

        from ray.tune.suggest import ConcurrencyLimiter
        search_alg = HyperOptSearch(metric="accuracy")
        search_alg = ConcurrencyLimiter(search_alg, max_concurrent=2)
        tune.run(trainable, search_alg=search_alg)
    """

    def __init__(self, searcher, max_concurrent):
        assert type(max_concurrent) is int and max_concurrent > 0
        self.searcher = searcher
        self.max_concurrent = max_concurrent
        self.live_trials = set()
        super(ConcurrencyLimiter, self).__init__(
            metric=self.searcher.metric, mode=self.searcher.mode)

    def suggest(self, trial_id):
        if len(self.live_trials) >= self.max_concurrent:
            return
        self.live_trials.add(trial_id)
        return self.searcher.suggest(trial_id)

    def on_trial_complete(self, trial_id, result=None, error=False):
        if trial_id not in self.live_trials:
            return
        else:
            self.searcher.on_trial_complete(
                trial_id, result=result, error=error)
            self.live_trials.remove(trial_id)

    def save(self, checkpoint_dir):
        self.searcher.save(checkpoint_dir)

    def restore(self, checkpoint_dir):
        self.searcher.restore(checkpoint_dir)


class SearchGenerator(SearchAlgorithm):
    """Generates trials to be passed to the TrialRunner.

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
        self._experiment = None
        self._counter = 0  # Keeps track of number of trials created.
        self._total_samples = 0  # int: total samples to evaluate.
        self._finished = False

    def add_configurations(self, experiments):
        """Registers experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        logger.debug("added configurations")
        experiment_list = convert_to_experiment_list(experiments)
        assert len(experiment_list) == 1, (
            "SearchAlgorithms can only support 1 experiment at a time.")
        self._experiment = experiment_list[0]
        experiment_spec = self._experiment.spec
        self._total_samples = experiment_spec.get("num_samples", 1)

        _warn_on_repeater(self.searcher, self._total_samples)

        if "run" not in experiment_spec:
            raise TuneError("Must specify `run` in {}".format(experiment_spec))

    def next_trials(self):
        """Provides a batch of Trial objects to be queued into the TrialRunner.

        Returns:
            List[Trial]: A list of trials for the Runner to consume.
        """
        trials = []
        while not self.is_finished():
            trial = self.create_trial_if_possible(self._experiment.spec,
                                                  self._experiment.name)
            if trial is None:
                break
            trials.append(trial)
        return trials

    def create_trial_if_possible(self, experiment_spec, output_path):
        logger.debug("creating trial")
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

    def on_trial_complete(self, trial_id, result=None, error=False):
        self.searcher.on_trial_complete(
            trial_id=trial_id, result=result, error=error)

    def is_finished(self):
        return self._counter >= self._total_samples or self._finished


class _MockSearcher(Searcher):
    def __init__(self, **kwargs):
        self.live_trials = {}
        self.counter = {"result": 0, "complete": 0}
        self.final_results = []
        self.stall = False
        self.results = []
        super(_MockSearcher, self).__init__(**kwargs)

    def suggest(self, trial_id):
        if not self.stall:
            self.live_trials[trial_id] = 1
            return {"test_variable": 2}
        return None

    def on_trial_result(self, trial_id, result):
        self.counter["result"] += 1
        self.results += [result]

    def on_trial_complete(self, trial_id, result=None, error=False):
        self.counter["complete"] += 1
        if result:
            self._process_result(result)
        if trial_id in self.live_trials:
            del self.live_trials[trial_id]

    def _process_result(self, result):
        self.final_results += [result]


class _MockSuggestionAlgorithm(SearchGenerator):
    def __init__(self, max_concurrent=None, **kwargs):
        self.searcher = _MockSearcher(**kwargs)
        if max_concurrent:
            self.searcher = ConcurrencyLimiter(
                self.searcher, max_concurrent=max_concurrent)
        super(_MockSuggestionAlgorithm, self).__init__(self.searcher)

    @property
    def live_trials(self):
        return self.searcher.live_trials

    @property
    def results(self):
        return self.searcher.results
