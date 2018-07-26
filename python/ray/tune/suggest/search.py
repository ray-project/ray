from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class SearchAlgorithm(object):
    """SearchAlgorithm exposes an event handler API for hyperparameter search.

    Unlike TrialSchedulers, SearchAlgorithms will not have the ability
    to modify the execution (i.e., stop and pause trials).

    To track suggestions and their corresponding evaluations, the method
    `try_suggest` will need to generate a trial_id. This trial_id will
    be used in subsequent notifications.

    Trials added manually (i.e., via the Client API) will also notify
    this class upon new events, so custom search algorithms may want to
    maintain a list of trials ID generated from this class.

    Attributes:
        NOT_READY (str): Status string for `try_suggest` if SearchAlgorithm
            currently cannot be queried for parameters (i.e. due to
            constrained concurrency).

    Example:
        >>> suggester = SearchAlgorithm()
        >>> new_parameters, trial_id = suggester.try_suggest()
        >>> suggester.on_trial_complete(trial_id, result)
        >>> better_parameters, trial_id2 = suggester.try_suggest()
    """
    NOT_READY = "NOT_READY"

    def try_suggest(self):
        """Queries the algorithm to retrieve the next set of parameters.

        Returns:
            (dict) Configuration for a trial
            (trial_id): Trial ID used for subsequent notifications.

        Example:
            >>> suggester = SearchAlgorithm(max_concurrent=1)
            >>> parameters_1, trial_id = suggester.try_suggest()
            >>> parameters_2, trial_id2 = suggester.try_suggest()
            >>> parameters_2 == SearchAlgorithm.NOT_READY
            >>> suggester.on_trial_complete(trial_id, result)
            >>> parameters_2, trial_id2 = suggester.try_suggest()
            >>> not(parameters_2 == SearchAlgorithm.NOT_READY)
        """
        return {}, None

    def on_trial_result(self, trial_id, result):
        """Called on each intermediate result returned by a trial.

        This will only be called when the trial is in the RUNNING state.

        Arguments:
            trial_id: Identifier for the trial.
        """
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Notification for the completion of trial.

        Arguments:
            trial_id: Identifier for the trial.
            result (TrainingResult): Defaults to None. A TrainingResult will
                be provided with this notification when the trial is in
                the RUNNING state AND either completes naturally or
                by manual termination.
            error (bool): Defaults to False. True if the trial is in
                the RUNNING state and errors.
            early_terminated (bool): Defaults to False. True if the trial
                is stopped while in PAUSED or PENDING state.
        """
        pass


class _MockAlgorithm(SearchAlgorithm):
    def __init__(self, max_concurrent=2):
        self._id = 0
        self._max_concurrent = max_concurrent
        self.live_trials = {}

    def try_suggest(self):
        if len(self.live_trials) < self._max_concurrent:
            id_str = self._generate_id()
            self.live_trials[id_str] = 1
            return {"a": 1, "b": 2}, id_str
        else:
            return SearchAlgorithm.NOT_READY, None

    def _generate_id(self):
        self._id += 1
        return str(self._id) * 5

    def on_trial_complete(self, trial_id, **kwargs):
        del self.live_trials[trial_id]
