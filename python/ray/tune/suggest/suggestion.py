import copy
import glob
import logging
import os
from typing import Dict, Optional, List

from ray.tune.suggest.util import set_search_properties_backwards_compatible
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

UNRESOLVED_SEARCH_SPACE = str(
    "You passed a `{par}` parameter to {cls} that contained unresolved search "
    "space definitions. {cls} should however be instantiated with fully "
    "configured search spaces only. To use Ray Tune's automatic search space "
    "conversion, pass the space definition as part of the `config` argument "
    "to `tune.run()` instead.")

UNDEFINED_SEARCH_SPACE = str(
    "Trying to sample a configuration from {cls}, but no search "
    "space has been defined. Either pass the `{space}` argument when "
    "instantiating the search algorithm, or pass a `config` to "
    "`tune.run()`.")

UNDEFINED_METRIC_MODE = str(
    "Trying to sample a configuration from {cls}, but the `metric` "
    "({metric}) or `mode` ({mode}) parameters have not been set. "
    "Either pass these arguments when instantiating the search algorithm, "
    "or pass them to `tune.run()`.")


class Searcher:
    """Abstract class for wrapping suggesting algorithms.

    Custom algorithms can extend this class easily by overriding the
    `suggest` method provide generated parameters for the trials.

    Any subclass that implements ``__init__`` must also call the
    constructor of this class: ``super(Subclass, self).__init__(...)``.

    To track suggestions and their corresponding evaluations, the method
    `suggest` will be passed a trial_id, which will be used in
    subsequent notifications.

    Not all implementations support multi objectives.

    Args:
        metric (str or list): The training result objective value attribute. If
            list then list of training result objective value attributes
        mode (str or list): If string One of {min, max}. If list then
            list of max and min, determines whether objective is minimizing
            or maximizing the metric attribute. Must match type of metric.

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
    FINISHED = "FINISHED"
    CKPT_FILE_TMPL = "searcher-state-{}.pkl"

    def __init__(self,
                 metric: Optional[str] = None,
                 mode: Optional[str] = None,
                 max_concurrent: Optional[int] = None,
                 use_early_stopped_trials: Optional[bool] = None):
        if use_early_stopped_trials is False:
            raise DeprecationWarning(
                "Early stopped trials are now always used. If this is a "
                "problem, file an issue: https://github.com/ray-project/ray.")
        if max_concurrent is not None:
            logger.warning(
                "DeprecationWarning: `max_concurrent` is deprecated for this "
                "search algorithm. Use tune.suggest.ConcurrencyLimiter() "
                "instead. This will raise an error in future versions of Ray.")

        self._metric = metric
        self._mode = mode

        if not mode or not metric:
            # Early return to avoid assertions
            return

        assert isinstance(
            metric, type(mode)), "metric and mode must be of the same type"
        if isinstance(mode, str):
            assert mode in ["min", "max"
                            ], "if `mode` is a str must be 'min' or 'max'!"
        elif isinstance(mode, list):
            assert len(mode) == len(
                metric), "Metric and mode must be the same length"
            assert all(mod in ["min", "max", "obs"] for mod in
                       mode), "All of mode must be 'min' or 'max' or 'obs'!"
        else:
            raise ValueError("Mode most either be a list or string")

    def set_search_properties(self, metric: Optional[str], mode: Optional[str],
                              config: Dict, **spec) -> bool:
        """Pass search properties to searcher.

        This method acts as an alternative to instantiating search algorithms
        with their own specific search spaces. Instead they can accept a
        Tune config through this method. A searcher should return ``True``
        if setting the config was successful, or ``False`` if it was
        unsuccessful, e.g. when the search space has already been set.

        Args:
            metric (str): Metric to optimize
            mode (str): One of ["min", "max"]. Direction to optimize.
            config (dict): Tune config dict.
            **spec: Any kwargs for forward compatiblity.
                Info like Experiment.PUBLIC_KEYS is provided through here.
        """
        return False

    def on_trial_result(self, trial_id: str, result: Dict) -> None:
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

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False) -> None:
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

    def suggest(self, trial_id: str) -> Optional[Dict]:
        """Queries the algorithm to retrieve the next set of parameters.

        Arguments:
            trial_id (str): Trial ID used for subsequent notifications.

        Returns:
            dict | FINISHED | None: Configuration for a trial, if possible.
                If FINISHED is returned, Tune will be notified that
                no more suggestions/configurations will be provided.
                If None is returned, Tune will skip the querying of the
                searcher for this step.

        """
        raise NotImplementedError

    def add_evaluated_point(self,
                            parameters: Dict,
                            value: float,
                            error: bool = False,
                            pruned: bool = False,
                            intermediate_values: Optional[List[float]] = None):
        """Pass results from a point that has been evaluated separately.

        This method allows for information from outside the
        suggest - on_trial_complete loop to be passed to the search
        algorithm.
        This functionality depends on the underlying search algorithm
        and may not be always available.

        Args:
            parameters (dict): Parameters used for the trial.
            value (float): Metric value obtained in the trial.
            error (bool): True if the training process raised an error.
            pruned (bool): True if trial was pruned.
            intermediate_values (list): List of metric values for
                intermediate iterations of the result. None if not
                applicable.

        """
        raise NotImplementedError

    def save(self, checkpoint_path: str):
        """Save state to path for this search algorithm.

        Args:
            checkpoint_path (str): File where the search algorithm
                state is saved. This path should be used later when
                restoring from file.

        Example:

        .. code-block:: python

            search_alg = Searcher(...)

            analysis = tune.run(
                cost,
                num_samples=5,
                search_alg=search_alg,
                name=self.experiment_name,
                local_dir=self.tmpdir)

            search_alg.save("./my_favorite_path.pkl")

        .. versionchanged:: 0.8.7
            Save is automatically called by `tune.run`. You can use
            `restore_from_dir` to restore from an experiment directory
            such as `~/ray_results/trainable`.

        """
        raise NotImplementedError

    def restore(self, checkpoint_path: str):
        """Restore state for this search algorithm


        Args:
            checkpoint_path (str): File where the search algorithm
                state is saved. This path should be the same
                as the one provided to "save".

        Example:

        .. code-block:: python

            search_alg.save("./my_favorite_path.pkl")

            search_alg2 = Searcher(...)
            search_alg2 = ConcurrencyLimiter(search_alg2, 1)
            search_alg2.restore(checkpoint_path)
            tune.run(cost, num_samples=5, search_alg=search_alg2)

        """
        raise NotImplementedError

    def get_state(self) -> Dict:
        raise NotImplementedError

    def set_state(self, state: Dict):
        raise NotImplementedError

    def save_to_dir(self, checkpoint_dir: str, session_str: str = "default"):
        """Automatically saves the given searcher to the checkpoint_dir.

        This is automatically used by tune.run during a Tune job.

        Args:
            checkpoint_dir (str): Filepath to experiment dir.
            session_str (str): Unique identifier of the current run
                session.
        """
        tmp_search_ckpt_path = os.path.join(checkpoint_dir,
                                            ".tmp_searcher_ckpt")
        success = True
        try:
            self.save(tmp_search_ckpt_path)
        except NotImplementedError:
            if log_once("suggest:save_to_dir"):
                logger.warning(
                    "save not implemented for Searcher. Skipping save.")
            success = False

        if success and os.path.exists(tmp_search_ckpt_path):
            os.replace(
                tmp_search_ckpt_path,
                os.path.join(checkpoint_dir,
                             self.CKPT_FILE_TMPL.format(session_str)))

    def restore_from_dir(self, checkpoint_dir: str):
        """Restores the state of a searcher from a given checkpoint_dir.

        Typically, you should use this function to restore from an
        experiment directory such as `~/ray_results/trainable`.

        .. code-block:: python

            experiment_1 = tune.run(
                cost,
                num_samples=5,
                search_alg=search_alg,
                verbose=0,
                name=self.experiment_name,
                local_dir="~/my_results")

            search_alg2 = Searcher()
            search_alg2.restore_from_dir(
                os.path.join("~/my_results", self.experiment_name)
        """

        pattern = self.CKPT_FILE_TMPL.format("*")
        full_paths = glob.glob(os.path.join(checkpoint_dir, pattern))
        if not full_paths:
            raise RuntimeError(
                "Searcher unable to find checkpoint in {}".format(
                    checkpoint_dir))  # TODO
        most_recent_checkpoint = max(full_paths)
        self.restore(most_recent_checkpoint)

    @property
    def metric(self) -> str:
        """The training result objective value attribute."""
        return self._metric

    @property
    def mode(self) -> str:
        """Specifies if minimizing or maximizing the metric."""
        return self._mode


class ConcurrencyLimiter(Searcher):
    """A wrapper algorithm for limiting the number of concurrent trials.

    Args:
        searcher (Searcher): Searcher object that the
            ConcurrencyLimiter will manage.
        max_concurrent (int): Maximum concurrent samples from the underlying
            searcher.
        batch (bool): Whether to wait for all concurrent samples
            to finish before updating the underlying searcher.

    Example:

    .. code-block:: python

        from ray.tune.suggest import ConcurrencyLimiter
        search_alg = HyperOptSearch(metric="accuracy")
        search_alg = ConcurrencyLimiter(search_alg, max_concurrent=2)
        tune.run(trainable, search_alg=search_alg)
    """

    def __init__(self,
                 searcher: Searcher,
                 max_concurrent: int,
                 batch: bool = False):
        assert type(max_concurrent) is int and max_concurrent > 0
        self.searcher = searcher
        self.max_concurrent = max_concurrent
        self.batch = batch
        self.live_trials = set()
        self.num_unfinished_live_trials = 0
        self.cached_results = {}

        if not isinstance(searcher, Searcher):
            raise RuntimeError(
                f"The `ConcurrencyLimiter` only works with `Searcher` "
                f"objects (got {type(searcher)}). Please try to pass "
                f"`max_concurrent` to the search generator directly.")

        super(ConcurrencyLimiter, self).__init__(
            metric=self.searcher.metric, mode=self.searcher.mode)

    def suggest(self, trial_id: str) -> Optional[Dict]:
        assert trial_id not in self.live_trials, (
            f"Trial ID {trial_id} must be unique: already found in set.")
        if len(self.live_trials) >= self.max_concurrent:
            logger.debug(
                f"Not providing a suggestion for {trial_id} due to "
                "concurrency limit: %s/%s.", len(self.live_trials),
                self.max_concurrent)
            return

        suggestion = self.searcher.suggest(trial_id)
        if suggestion not in (None, Searcher.FINISHED):
            self.live_trials.add(trial_id)
            self.num_unfinished_live_trials += 1
        return suggestion

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False):
        if trial_id not in self.live_trials:
            return
        elif self.batch:
            self.cached_results[trial_id] = (result, error)
            self.num_unfinished_live_trials -= 1
            if self.num_unfinished_live_trials <= 0:
                # Update the underlying searcher once the
                # full batch is completed.
                for trial_id, (result, error) in self.cached_results.items():
                    self.searcher.on_trial_complete(
                        trial_id, result=result, error=error)
                    self.live_trials.remove(trial_id)
                self.cached_results = {}
                self.num_unfinished_live_trials = 0
            else:
                return
        else:
            self.searcher.on_trial_complete(
                trial_id, result=result, error=error)
            self.live_trials.remove(trial_id)
            self.num_unfinished_live_trials -= 1

    def add_evaluated_point(self,
                            parameters: Dict,
                            value: float,
                            error: bool = False,
                            pruned: bool = False,
                            intermediate_values: Optional[List[float]] = None):
        return self.searcher.add_evaluated_point(parameters, value, error,
                                                 pruned, intermediate_values)

    def get_state(self) -> Dict:
        state = self.__dict__.copy()
        del state["searcher"]
        return copy.deepcopy(state)

    def set_state(self, state: Dict):
        self.__dict__.update(state)

    def save(self, checkpoint_path: str):
        self.searcher.save(checkpoint_path)

    def restore(self, checkpoint_path: str):
        self.searcher.restore(checkpoint_path)

    def on_pause(self, trial_id: str):
        self.searcher.on_pause(trial_id)

    def on_unpause(self, trial_id: str):
        self.searcher.on_unpause(trial_id)

    def set_search_properties(self, metric: Optional[str], mode: Optional[str],
                              config: Dict, **spec) -> bool:
        return set_search_properties_backwards_compatible(
            self.searcher.set_search_properties, metric, mode, config, **spec)
