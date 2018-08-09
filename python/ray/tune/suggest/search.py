from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class SearchAlgorithm(object):
    """Interface of an event handler API for hyperparameter search.

    Unlike TrialSchedulers, SearchAlgorithms will not have the ability
    to modify the execution (i.e., stop and pause trials).

    Trials added manually (i.e., via the Client API) will also notify
    this class upon new events, so custom search algorithms should
    maintain a list of trials ID generated from this class.

    See also: `ray.tune.suggest.BasicVariantGenerator`.
    """

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

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Notification for the completion of trial.

        Arguments:
            trial_id: Identifier for the trial.
            result (dict): Defaults to None. A dict will
                be provided with this notification when the trial is in
                the RUNNING state AND either completes naturally or
                by manual termination.
            error (bool): Defaults to False. True if the trial is in
                the RUNNING state and errors.
            early_terminated (bool): Defaults to False. True if the trial
                is stopped while in PAUSED or PENDING state.
        """
        pass

    def is_finished(self):
        """Returns True if no trials left to be queued into TrialRunner.

        Can return True before all trials have finished executing.
        """
        raise NotImplementedError
