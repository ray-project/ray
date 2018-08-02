from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain
import os
import copy

from ray.tune.suggest.variant_generator import to_argv, generate_variants
from ray.tune.config_parser import make_parser, json_to_resources
from ray.tune.experiment import Experiment
from ray.tune.error import TuneError
from ray.tune.trial import Trial


class SearchAlgorithm(object):
    """Interface of an event handler API for hyperparameter search.

    Unlike TrialSchedulers, SearchAlgorithms will not have the ability
    to modify the execution (i.e., stop and pause trials).

    Trials added manually (i.e., via the Client API) will also notify
    this class upon new events, so custom search algorithms should
    maintain a list of trials ID generated from this class.

    See also: `ExistingVariants`.
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
    `_suggest` method, which will override conflicting fields from
    the initially generated parameters.

    See `ray.tune.suggest.variant_generator`.

    To track suggestions and their corresponding evaluations, the method
    `_suggest` will need to generate a trial_id. This trial_id will
    be used in subsequent notifications.

    Example:
        >>> suggester = ExistingVariants()
        >>> new_parameters, trial_id = suggester._suggest()
        >>> suggester.on_trial_complete(trial_id, result)
        >>> better_parameters, trial_id2 = suggester._suggest()
    """

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

        self._trial_generator = chain.from_iterable([
            self._generate_trials(experiment.spec, experiment.name)
            for experiment in exp_list
        ])

        self._finished = False

    def next_trials(self):
        """Provides Trial objects to be queued into the TrialRunner.

        A batch ends when self._trial_generator returns None.

        Returns:
            trials (list): Returns a list of trials.
        """
        trials = []

        for trial in self._trial_generator:
            if trial is None:
                return trials
            trials += [trial]

        self._finished = True
        return trials

    def _generate_trials(self, unresolved_spec, output_path=''):
        """Wraps `generate_variants()` to return a Trial object for each variant.

        Specified/sampled hyperparameters for the Search Algorithm will be
        used to update the generated configuration.

        See also: `generate_variants()`.

        Arguments:
            unresolved_spec (dict): Experiment spec conforming to the argument
                schema defined in `ray.tune.config_parser`.
            output_path (str): Path where to store experiment outputs.

        Yields:
            Trial|None: If search_alg is specified but cannot be queried at
                a certain time (i.e. due to contrained concurrency), this will
                yield None. Otherwise, it will yield a trial.
        """
        if "run" not in unresolved_spec:
            raise TuneError("Must specify `run` in {}".format(unresolved_spec))
        parser = make_parser()
        i = 0
        for _ in range(unresolved_spec.get("repeat", 1)):
            for resolved_vars, spec in generate_variants(unresolved_spec):
                try:
                    # Special case the `env` param for RLlib by automatically
                    # moving it into the `config` section.
                    if "env" in spec:
                        spec["config"] = spec.get("config", {})
                        spec["config"]["env"] = spec["env"]
                        del spec["env"]
                    args = parser.parse_args(to_argv(spec))
                except SystemExit:
                    raise TuneError(
                        "Error parsing args, see above message", spec)

                new_config = copy.deepcopy(spec.get("config", {}))
                # We hold the other resolved vars until suggestion is ready.
                while True:
                    suggested_config, trial_id = self._suggest(new_config)
                    if suggested_config:
                        break
                    else:
                        yield None

                if resolved_vars:
                    experiment_tag = "{}_{}".format(i, resolved_vars)
                else:
                    experiment_tag = str(i)
                i += 1
                if "trial_resources" in spec:
                    resources = json_to_resources(spec["trial_resources"])
                else:
                    resources = None
                yield Trial(
                    trainable_name=args.run,
                    config=suggested_config,
                    trial_id=trial_id,
                    local_dir=os.path.join(args.local_dir, output_path),
                    experiment_tag=experiment_tag,
                    resources=resources,
                    stopping_criterion=args.stop,
                    checkpoint_freq=args.checkpoint_freq,
                    restore_path=args.restore,
                    upload_dir=args.upload_dir,
                    max_failures=args.max_failures)

    def _suggest(self, generated_params):
        """Queries the algorithm to retrieve the next set of parameters.

        Returns:
            dict|None: Configuration for a trial, if possible.
                Else, returns None, which will temporarily stop the
                TrialRunner from querying.
            trial_id: Trial ID used for subsequent notifications.
                If returned None, Trial ID will be generated by Trial.

        Example:
            >>> suggester = ExistingVariants(max_concurrent=1)
            >>> parameters_1, trial_id = suggester._suggest()
            >>> parameters_2, trial_id2 = suggester._suggest()
            >>> parameters_2 is None
            >>> suggester.on_trial_complete(trial_id, result)
            >>> parameters_2, trial_id2 = suggester._suggest()
            >>> parameters_2 is not None
        """
        return generated_params, None

    def is_finished(self):
        return self._finished


class _MockAlgorithm(ExistingVariants):
    def __init__(self, max_concurrent=2, **kwargs):
        self._id = 0
        self._max_concurrent = max_concurrent
        self.live_trials = {}
        self.stall = False

        super(_MockAlgorithm, self).__init__(**kwargs)

    def _suggest(self):
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
