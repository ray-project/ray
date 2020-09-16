from typing import Dict, List, Optional, Union

from ray.tune.experiment import Experiment
from ray.tune.trial import Trial


class SearchAlgorithm:
    """Interface of an event handler API for hyperparameter search.

    Unlike TrialSchedulers, SearchAlgorithms will not have the ability
    to modify the execution (i.e., stop and pause trials).

    Trials added manually (i.e., via the Client API) will also notify
    this class upon new events, so custom search algorithms should
    maintain a list of trials ID generated from this class.

    See also: `ray.tune.suggest.BasicVariantGenerator`.
    """
    _finished = False

    def set_search_properties(self, metric: Optional[str], mode: Optional[str],
                              config: Dict) -> bool:
        """Pass search properties to search algorithm.

        This method acts as an alternative to instantiating search algorithms
        with their own specific search spaces. Instead they can accept a
        Tune config through this method.

        The search algorithm will usually pass this method to their
        ``Searcher`` instance.

        Args:
            metric (str): Metric to optimize
            mode (str): One of ["min", "max"]. Direction to optimize.
            config (dict): Tune config dict.
        """
        return True

    def add_configurations(
            self,
            experiments: Union[Experiment, List[Experiment], Dict[str, Dict]]):
        """Tracks given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        raise NotImplementedError

    def next_trials(self) -> List[Trial]:
        """Provides Trial objects to be queued into the TrialRunner.

        Returns:
            trials (list): Returns a list of trials.
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

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False):
        """Notification for the completion of trial.

        Arguments:
            trial_id: Identifier for the trial.
            result (dict): Defaults to None. A dict will
                be provided with this notification when the trial is in
                the RUNNING state AND either completes naturally or
                by manual termination.
            error (bool): Defaults to False. True if the trial is in
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
