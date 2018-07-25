from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class SearchAlgorithm():
    """Base class for impelmenting search algorithms."""
    NOT_READY = "NOT_READY"

    def try_suggest(self):
        """Queries the algorithm to retrieve the next set of parameters.

        Returns:
            (dict) Configuration for a trial
            (trial_id): Trial ID used for subsequent notifications.
        """
        return {}, None

    def on_trial_result(self, trial_id, result):
        """Called on each intermediate result returned by a trial.

        This will only be called when the trial is in the RUNNING state.

        Arguments:
            trial_id: Identifier for the trial."""
        pass

    def on_trial_error(self, trial_id):
        """Notification for the error of trial.

        This will only be called when the trial is in the RUNNING state.

        Arguments:
            trial_id: Identifier for the trial."""
        pass

    def on_trial_remove(self, trial_id):
        """Called to remove trial.

        This is called when the trial is in PAUSED or PENDING state. Otherwise,
        call `on_trial_complete`.

        Arguments:
            trial_id: Identifier for the trial."""
        pass

    def on_trial_complete(self, trial_id, result):
        """Notification for the completion of trial.

        This will only be called when the trial is in the RUNNING state and
        either completes naturally or by manual termination.

        Arguments:
            trial_id: Identifier for the trial."""
        pass
