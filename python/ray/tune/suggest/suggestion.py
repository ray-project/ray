import copy
import glob
import logging
import os
import warnings
from typing import Dict, Optional, List, Union, Any, TYPE_CHECKING

from ray.tune.suggest.util import set_search_properties_backwards_compatible
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.tune.trial import Trial
    from ray.tune.analysis import ExperimentAnalysis

logger = logging.getLogger(__name__)

UNRESOLVED_SEARCH_SPACE = str(
    "You passed a `{par}` parameter to {cls} that contained unresolved search "
    "space definitions. {cls} should however be instantiated with fully "
    "configured search spaces only. To use Ray Tune's automatic search space "
    "conversion, pass the space definition as part of the `config` argument "
    "to `tune.run()` instead."
)

UNDEFINED_SEARCH_SPACE = str(
    "Trying to sample a configuration from {cls}, but no search "
    "space has been defined. Either pass the `{space}` argument when "
    "instantiating the search algorithm, or pass a `config` to "
    "`tune.run()`."
)

UNDEFINED_METRIC_MODE = str(
    "Trying to sample a configuration from {cls}, but the `metric` "
    "({metric}) or `mode` ({mode}) parameters have not been set. "
    "Either pass these arguments when instantiating the search algorithm, "
    "or pass them to `tune.run()`."
)


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
        metric: The training result objective value attribute. If
            list then list of training result objective value attributes
        mode: If string One of {min, max}. If list then
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

    def __init__(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        self._metric = metric
        self._mode = mode

        if not mode or not metric:
            # Early return to avoid assertions
            return

        assert isinstance(
            metric, type(mode)
        ), "metric and mode must be of the same type"
        if isinstance(mode, str):
            assert mode in ["min", "max"], "if `mode` is a str must be 'min' or 'max'!"
        elif isinstance(mode, list):
            assert len(mode) == len(metric), "Metric and mode must be the same length"
            assert all(
                mod in ["min", "max", "obs"] for mod in mode
            ), "All of mode must be 'min' or 'max' or 'obs'!"
        else:
            raise ValueError("Mode most either be a list or string")

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        """Pass search properties to searcher.

        This method acts as an alternative to instantiating search algorithms
        with their own specific search spaces. Instead they can accept a
        Tune config through this method. A searcher should return ``True``
        if setting the config was successful, or ``False`` if it was
        unsuccessful, e.g. when the search space has already been set.

        Args:
            metric: Metric to optimize
            mode: One of ["min", "max"]. Direction to optimize.
            config: Tune config dict.
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
            trial_id: A unique string ID for the trial.
            result: Dictionary of metrics for current training progress.
                Note that the result dict may include NaNs or
                may not include the optimization metric. It is up to the
                subclass implementation to preprocess the result to
                avoid breaking the optimization process.
        """
        pass

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ) -> None:
        """Notification for the completion of trial.

        Typically, this method is used for notifying the underlying
        optimizer of the result.

        Args:
            trial_id: A unique string ID for the trial.
            result: Dictionary of metrics for current training progress.
                Note that the result dict may include NaNs or
                may not include the optimization metric. It is up to the
                subclass implementation to preprocess the result to
                avoid breaking the optimization process. Upon errors, this
                may also be None.
            error: True if the training process raised an error.

        """
        raise NotImplementedError

    def suggest(self, trial_id: str) -> Optional[Dict]:
        """Queries the algorithm to retrieve the next set of parameters.

        Arguments:
            trial_id: Trial ID used for subsequent notifications.

        Returns:
            dict | FINISHED | None: Configuration for a trial, if possible.
                If FINISHED is returned, Tune will be notified that
                no more suggestions/configurations will be provided.
                If None is returned, Tune will skip the querying of the
                searcher for this step.

        """
        raise NotImplementedError

    def add_evaluated_point(
        self,
        parameters: Dict,
        value: float,
        error: bool = False,
        pruned: bool = False,
        intermediate_values: Optional[List[float]] = None,
    ):
        """Pass results from a point that has been evaluated separately.

        This method allows for information from outside the
        suggest - on_trial_complete loop to be passed to the search
        algorithm.
        This functionality depends on the underlying search algorithm
        and may not be always available.

        Args:
            parameters: Parameters used for the trial.
            value: Metric value obtained in the trial.
            error: True if the training process raised an error.
            pruned: True if trial was pruned.
            intermediate_values: List of metric values for
                intermediate iterations of the result. None if not
                applicable.

        """
        raise NotImplementedError

    def add_evaluated_trials(
        self,
        trials_or_analysis: Union["Trial", List["Trial"], "ExperimentAnalysis"],
        metric: str,
    ):
        """Pass results from trials that have been evaluated separately.

        This method allows for information from outside the
        suggest - on_trial_complete loop to be passed to the search
        algorithm.
        This functionality depends on the underlying search algorithm
        and may not be always available (same as ``add_evaluated_point``.)

        Args:
            trials_or_analysis: Trials to pass results form to the searcher.
            metric: Metric name reported by trials used for
                determining the objective value.

        """
        if self.add_evaluated_point == Searcher.add_evaluated_point:
            raise NotImplementedError

        # lazy imports to avoid circular dependencies
        from ray.tune.trial import Trial
        from ray.tune.analysis import ExperimentAnalysis
        from ray.tune.result import DONE

        if isinstance(trials_or_analysis, Trial):
            trials_or_analysis = [trials_or_analysis]
        elif isinstance(trials_or_analysis, ExperimentAnalysis):
            trials_or_analysis = trials_or_analysis.trials

        any_trial_had_metric = False

        def trial_to_points(trial: Trial) -> Dict[str, Any]:
            nonlocal any_trial_had_metric
            has_trial_been_pruned = (
                trial.status == Trial.TERMINATED
                and not trial.last_result.get(DONE, False)
            )
            has_trial_finished = (
                trial.status == Trial.TERMINATED and trial.last_result.get(DONE, False)
            )
            if not any_trial_had_metric:
                any_trial_had_metric = (
                    metric in trial.last_result and has_trial_finished
                )
            if Trial.TERMINATED and metric not in trial.last_result:
                return None
            return dict(
                parameters=trial.config,
                value=trial.last_result.get(metric, None),
                error=trial.status == Trial.ERROR,
                pruned=has_trial_been_pruned,
                intermediate_values=None,  # we do not save those
            )

        for trial in trials_or_analysis:
            kwargs = trial_to_points(trial)
            if kwargs:
                self.add_evaluated_point(**kwargs)

        if not any_trial_had_metric:
            warnings.warn(
                "No completed trial returned the specified metric. "
                "Make sure the name you have passed is correct. "
            )

    def save(self, checkpoint_path: str):
        """Save state to path for this search algorithm.

        Args:
            checkpoint_path: File where the search algorithm
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
            checkpoint_path: File where the search algorithm
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

    def set_max_concurrency(self, max_concurrent: int) -> bool:
        """Set max concurrent trials this searcher can run.

        This method will be called on the wrapped searcher by the
        ``ConcurrencyLimiter``. It is intended to allow for searchers
        which have custom, internal logic handling max concurrent trials
        to inherit the value passed to ``ConcurrencyLimiter``.

        If this method returns False, it signifies that no special
        logic for handling this case is present in the searcher.

        Args:
            max_concurrent: Number of maximum concurrent trials.
        """
        return False

    def get_state(self) -> Dict:
        raise NotImplementedError

    def set_state(self, state: Dict):
        raise NotImplementedError

    def save_to_dir(self, checkpoint_dir: str, session_str: str = "default"):
        """Automatically saves the given searcher to the checkpoint_dir.

        This is automatically used by tune.run during a Tune job.

        Args:
            checkpoint_dir: Filepath to experiment dir.
            session_str: Unique identifier of the current run
                session.
        """
        tmp_search_ckpt_path = os.path.join(checkpoint_dir, ".tmp_searcher_ckpt")
        success = True
        try:
            self.save(tmp_search_ckpt_path)
        except NotImplementedError:
            if log_once("suggest:save_to_dir"):
                logger.warning("save not implemented for Searcher. Skipping save.")
            success = False

        if success and os.path.exists(tmp_search_ckpt_path):
            os.replace(
                tmp_search_ckpt_path,
                os.path.join(checkpoint_dir, self.CKPT_FILE_TMPL.format(session_str)),
            )

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
                "Searcher unable to find checkpoint in {}".format(checkpoint_dir)
            )  # TODO
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

    Certain Searchers have their own internal logic for limiting
    the number of concurrent trials. If such a Searcher is passed to a
    ``ConcurrencyLimiter``, the ``max_concurrent`` of the
    ``ConcurrencyLimiter`` will override the ``max_concurrent`` value
    of the Searcher. The ``ConcurrencyLimiter`` will then let the
    Searcher's internal logic take over.

    Args:
        searcher: Searcher object that the
            ConcurrencyLimiter will manage.
        max_concurrent: Maximum concurrent samples from the underlying
            searcher.
        batch: Whether to wait for all concurrent samples
            to finish before updating the underlying searcher.

    Example:

    .. code-block:: python

        from ray.tune.suggest import ConcurrencyLimiter
        search_alg = HyperOptSearch(metric="accuracy")
        search_alg = ConcurrencyLimiter(search_alg, max_concurrent=2)
        tune.run(trainable, search_alg=search_alg)
    """

    def __init__(self, searcher: Searcher, max_concurrent: int, batch: bool = False):
        assert type(max_concurrent) is int and max_concurrent > 0
        self.searcher = searcher
        self.max_concurrent = max_concurrent
        self.batch = batch
        self.live_trials = set()
        self.num_unfinished_live_trials = 0
        self.cached_results = {}
        self._limit_concurrency = True

        if not isinstance(searcher, Searcher):
            raise RuntimeError(
                f"The `ConcurrencyLimiter` only works with `Searcher` "
                f"objects (got {type(searcher)}). Please try to pass "
                f"`max_concurrent` to the search generator directly."
            )

        self._set_searcher_max_concurrency()

        super(ConcurrencyLimiter, self).__init__(
            metric=self.searcher.metric, mode=self.searcher.mode
        )

    def _set_searcher_max_concurrency(self):
        # If the searcher has special logic for handling max concurrency,
        # we do not do anything inside the ConcurrencyLimiter
        self._limit_concurrency = not self.searcher.set_max_concurrency(
            self.max_concurrent
        )

    def set_max_concurrency(self, max_concurrent: int) -> bool:
        # Determine if this behavior is acceptable, or if it should
        # raise an exception.
        self.max_concurrent = max_concurrent
        return True

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        self._set_searcher_max_concurrency()
        return set_search_properties_backwards_compatible(
            self.searcher.set_search_properties, metric, mode, config, **spec
        )

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._limit_concurrency:
            return self.searcher.suggest(trial_id)

        assert (
            trial_id not in self.live_trials
        ), f"Trial ID {trial_id} must be unique: already found in set."
        if len(self.live_trials) >= self.max_concurrent:
            logger.debug(
                f"Not providing a suggestion for {trial_id} due to "
                "concurrency limit: %s/%s.",
                len(self.live_trials),
                self.max_concurrent,
            )
            return

        suggestion = self.searcher.suggest(trial_id)
        if suggestion not in (None, Searcher.FINISHED):
            self.live_trials.add(trial_id)
            self.num_unfinished_live_trials += 1
        return suggestion

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        if not self._limit_concurrency:
            return self.searcher.on_trial_complete(trial_id, result=result, error=error)

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
                        trial_id, result=result, error=error
                    )
                    self.live_trials.remove(trial_id)
                self.cached_results = {}
                self.num_unfinished_live_trials = 0
            else:
                return
        else:
            self.searcher.on_trial_complete(trial_id, result=result, error=error)
            self.live_trials.remove(trial_id)
            self.num_unfinished_live_trials -= 1

    def on_trial_result(self, trial_id: str, result: Dict) -> None:
        self.searcher.on_trial_result(trial_id, result)

    def add_evaluated_point(
        self,
        parameters: Dict,
        value: float,
        error: bool = False,
        pruned: bool = False,
        intermediate_values: Optional[List[float]] = None,
    ):
        return self.searcher.add_evaluated_point(
            parameters, value, error, pruned, intermediate_values
        )

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
