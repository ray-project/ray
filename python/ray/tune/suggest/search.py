from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain

from ray.tune.suggest.variant_generator import generate_trials
from ray.tune.experiment import Experiment
from ray.tune.error import TuneError


class SearchAlgorithm(object):
    """Interface of an event handler API for hyperparameter search.

    Unlike TrialSchedulers, SearchAlgorithms will not have the ability
    to modify the execution (i.e., stop and pause trials).

    Trials added manually (i.e., via the Client API) will also notify
    this class upon new events, so custom search algorithms should
    maintain a list of trials ID generated from this class.

    See `ExistingVariants`.
    """

    def next_trial(self):
        """Provides Trial objects to be queued into the TrialRunner.

        Returns:
            Trial|None: If SearchAlgorithm cannot be queried at
                a certain time (i.e. due to constrained concurrency), this will
                return None. Otherwise, return a trial.
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

    def is_finished(self):
        """Returns True if no trials left."""
        raise NotImplementedError


class ExistingVariants(SearchAlgorithm):
    """Uses Tune's variant generation for resolving variables.

    Custom search algorithms can extend this class easily by overriding the
    `try_suggest` method, which will override conflicting fields from
    the initially generated parameters.

    See `ray.tune.suggest.variant_generator`.

    To track suggestions and their corresponding evaluations, the method
    `try_suggest` will need to generate a trial_id. This trial_id will
    be used in subsequent notifications.

    Attributes:
        PASS (str): Status string for `try_suggest` if ExistingVariants
            currently cannot be queried for parameters (i.e. due to
            constrained concurrency).

    Example:
        >>> suggester = ExistingVariants()
        >>> new_parameters, trial_id = suggester.try_suggest()
        >>> suggester.on_trial_complete(trial_id, result)
        >>> better_parameters, trial_id2 = suggester.try_suggest()
    """

    PASS = "PASS"

    def __init__(self, experiments=None):
        """Constructs a generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        exp_list = experiments
        if isinstance(experiments, Experiment):
            exp_list = [experiments]
        elif type(experiments) is dict:
            exp_list = [
                Experiment.from_json(name, spec)
                for name, spec in experiments.items()
            ]
        if (type(exp_list) is list
                and all(isinstance(exp, Experiment) for exp in exp_list)):
            if len(exp_list) > 1:
                print("Warning: All experiments will be"
                      " using the same Search Algorithm.")
        else:
            raise TuneError("Invalid argument: {}".format(experiments))

        self._generator = chain.from_iterable([
            generate_trials(experiment.spec, experiment.name, self)
            for experiment in exp_list
        ])

        self._finished = False

    def next_trial(self):
        try:
            trial = next(self._generator)
        except StopIteration:
            trial = None
            self._finished = True
        return trial

    def try_suggest(self):
        """Queries the algorithm to retrieve the next set of parameters.

        Returns:
            dict|PASS: Partial configuration for a trial, if possible. This
                configuration will override conflicting fields from
                the initially generated parameters. Else, returns
                ExistingVariants.PASS, which will temporarily stop the
                TrialRunner from querying. No trial will be returned to
                the TrialRunner on `next_trial`.
            trial_id: Trial ID used for subsequent notifications.
                If returned None, Trial ID will be generated by Trial.

        Example:
            >>> suggester = ExistingVariants(max_concurrent=1)
            >>> parameters_1, trial_id = suggester.try_suggest()
            >>> parameters_2, trial_id2 = suggester.try_suggest()
            >>> parameters_2 == ExistingVariants.PASS
            >>> suggester.on_trial_complete(trial_id, result)
            >>> parameters_2, trial_id2 = suggester.try_suggest()
            >>> not(parameters_2 == ExistingVariants.PASS)
        """
        return {}, None

    def is_finished(self):
        return self._finished


class _MockAlgorithm(ExistingVariants):
    def __init__(self, max_concurrent=2, **kwargs):
        self._id = 0
        self._max_concurrent = max_concurrent
        self.live_trials = {}
        self.stall = False

        super(_MockAlgorithm, self).__init__(**kwargs)

    def try_suggest(self):
        if len(self.live_trials) < self._max_concurrent and not self.stall:
            id_str = self._generate_id()
            self.live_trials[id_str] = 1
            return {"test_variable": 2}, id_str
        else:
            return ExistingVariants.PASS, None

    def _generate_id(self):
        self._id += 1
        return str(self._id) * 5

    def on_trial_complete(self, trial_id, *args, **kwargs):
        del self.live_trials[trial_id]
