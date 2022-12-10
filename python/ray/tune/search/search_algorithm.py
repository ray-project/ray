from typing import Dict, List, Optional, Union, TYPE_CHECKING

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.tune.experiment import Experiment


@DeveloperAPI
class SearchAlgorithm:
    """Interface of an event handler API for hyperparameter search.

    Unlike TrialSchedulers, SearchAlgorithms will not have the ability
    to modify the execution (i.e., stop and pause trials).

    Trials added manually (i.e., via the Client API) will also notify
    this class upon new events, so custom search algorithms should
    maintain a list of trials ID generated from this class.

    See also: `ray.tune.search.BasicVariantGenerator`.
    """

    _finished = False

    _metric = None

    @property
    def metric(self):
        return self._metric

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        """Pass search properties to search algorithm.

        This method acts as an alternative to instantiating search algorithms
        with their own specific search spaces. Instead they can accept a
        Tune config through this method.

        The search algorithm will usually pass this method to their
        ``Searcher`` instance.

        Args:
            metric: Metric to optimize
            mode: One of ["min", "max"]. Direction to optimize.
            config: Tune config dict.
            **spec: Any kwargs for forward compatiblity.
                Info like Experiment.PUBLIC_KEYS is provided through here.
        """
        if self._metric and metric:
            return False
        if metric:
            self._metric = metric
        return True

    @property
    def total_samples(self):
        """Get number of total trials to be generated"""
        return 0

    def add_configurations(
        self, experiments: Union["Experiment", List["Experiment"], Dict[str, Dict]]
    ):
        """Tracks given experiment specifications.

        Arguments:
            experiments: Experiments to run.
        """
        raise NotImplementedError

    def next_trial(self):
        """Returns single Trial object to be queued into the TrialRunner.

        Returns:
            trial: Returns a Trial object.
        """
        raise NotImplementedError

    def on_trial_result(self, trial_id: str, result: Dict):
        """Called on each intermediate result returned by a trial.

        This will only be called when the trial is in the RUNNING state.

        Arguments:
            trial_id: Identifier for the trial.
            result: Result dictionary.
        """
        pass

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        """Notification for the completion of trial.

        Arguments:
            trial_id: Identifier for the trial.
            result: Defaults to None. A dict will
                be provided with this notification when the trial is in
                the RUNNING state AND either completes naturally or
                by manual termination.
            error: Defaults to False. True if the trial is in
                the RUNNING state and errors.
        """
        pass

    def is_finished(self) -> bool:
        """Returns True if no trials left to be queued into TrialRunner.

        Can return True before all trials have finished executing.
        """
        return self._finished

    def set_finished(self):
        """Marks the search algorithm as finished."""
        self._finished = True

    def has_checkpoint(self, dirpath: str) -> bool:
        """Should return False if not restoring is not implemented."""
        return False

    def save_to_dir(self, dirpath: str, **kwargs):
        """Saves a search algorithm."""
        pass

    def restore_from_dir(self, dirpath: str):
        """Restores a search algorithm along with its wrapped state."""
        pass
