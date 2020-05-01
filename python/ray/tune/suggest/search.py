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

    def add_configurations(self, experiments):
        """Tracks given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        raise NotImplementedError

    def next_trials(self):
        """Provides Trial objects to be queued into the TrialRunner.

        Returns:
            trials (list): Returns a list of trials.
        """
        raise NotImplementedError

    def on_trial_result(self, trial_id, result):
        """Called on each intermediate result returned by a trial.

        This will only be called when the trial is in the RUNNING state.

        Arguments:
            trial_id: Identifier for the trial.
        """
        pass

    def on_trial_complete(self, trial_id, result=None, error=False):
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

    def is_finished(self):
        """Returns True if no trials left to be queued into TrialRunner.

        Can return True before all trials have finished executing.
        """
        return self._finished

    def set_finished(self):
        """Marks the search algorithm as finished."""
        self._finished = True
