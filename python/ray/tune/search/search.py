from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class SearchAlgorithm():
    """This class is unaware of Tune trials."""
    NOT_READY = "NOT_READY"

    def try_suggest():
        """
        Returns:
            Configuration for a trial
            Suggestion ID
        """
        return {}, None

    def on_trial_result():
        pass

    def on_trial_error():
        pass

    def on_trial_complete():
        pass

