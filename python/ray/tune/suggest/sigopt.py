import copy
import os
import logging
import pickle
try:
    import sigopt as sgo
except ImportError:
    sgo = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class SigOptSearch(Searcher):
    """A wrapper around SigOpt to provide trial suggestions.

    You must install SigOpt and have a SigOpt API key to use this module.
    Store the API token as an environment variable ``SIGOPT_KEY`` as follows:

    .. code-block:: bash

        pip install -U sigopt
        export SIGOPT_KEY= ...

    You will need to use the `SigOpt experiment and space specification
    <https://app.sigopt.com/docs/overview/create>`_.

    This module manages its own concurrency.

    Parameters:
        space (list of dict): SigOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
        name (str): Name of experiment. Required by SigOpt.
        max_concurrent (int): Number of maximum concurrent trials supported
            based on the user's SigOpt plan. Defaults to 1.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.

    Example:

    .. code-block:: python

        space = [
            {
                'name': 'width',
                'type': 'int',
                'bounds': {
                    'min': 0,
                    'max': 20
                },
            },
            {
                'name': 'height',
                'type': 'int',
                'bounds': {
                    'min': -100,
                    'max': 100
                },
            },
        ]
        algo = SigOptSearch(
            space, name="SigOpt Example Experiment",
            max_concurrent=1, metric="mean_loss", mode="min")
    """

    def __init__(self,
                 space,
                 name="Default Tune Experiment",
                 max_concurrent=1,
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 **kwargs):
        assert sgo is not None, "SigOpt must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        assert "SIGOPT_KEY" in os.environ, \
            "SigOpt API key must be stored as environ variable at SIGOPT_KEY"
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        self._max_concurrent = max_concurrent
        self._metric = metric
        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.
        self._live_trial_mapping = {}

        # Create a connection with SigOpt API, requires API key
        self.conn = sgo.Connection(client_token=os.environ["SIGOPT_KEY"])

        self.experiment = self.conn.experiments().create(
            name=name,
            parameters=space,
            parallel_bandwidth=self._max_concurrent,
        )

        super(SigOptSearch, self).__init__(metric=metric, mode=mode, **kwargs)

    def suggest(self, trial_id):
        # Get new suggestion from SigOpt
        suggestion = self.conn.experiments(
            self.experiment.id).suggestions().create()

        self._live_trial_mapping[trial_id] = suggestion

        return copy.deepcopy(suggestion.assignments)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        If a trial fails, it will be reported as a failed Observation, telling
        the optimizer that the Suggestion led to a metric failure, which
        updates the feasible region and improves parameter recommendation.

        Creates SigOpt Observation object for trial.
        """
        if result:
            self.conn.experiments(self.experiment.id).observations().create(
                suggestion=self._live_trial_mapping[trial_id].id,
                value=self._metric_op * result[self._metric],
            )
            # Update the experiment object
            self.experiment = self.conn.experiments(self.experiment.id).fetch()
        elif error:
            # Reports a failed Observation
            self.conn.experiments(self.experiment.id).observations().create(
                failed=True, suggestion=self._live_trial_mapping[trial_id].id)
        del self._live_trial_mapping[trial_id]

    def save(self, checkpoint_dir):
        trials_object = (self.conn, self.experiment)
        with open(checkpoint_dir, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self.conn = trials_object[0]
        self.experiment = trials_object[1]
