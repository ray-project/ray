#!/usr/bin/env python
# Author: Rujie Jiang rujie.jrj@antfin.com
# Date: Tue May 15 11:46:20 2018

import time
import copy
import logging

from ray.tune.trial import Trial
from ray.tune.suggest import SearchAlgorithm
from ray.tune.suggest.variant_generator import generate_variants
from ray.tune.config_parser import make_parser, create_trial_from_spec


def merge_config(path, value, config):
    """Merge kv pair into config by expanding path separated by dot."""
    keys = path.split('.')
    for name in keys[:-1]:
        if config.get(name) is None:
            config[name] = {}
        config = config[name]
    # Set value corresponding to the last path name
    config[keys[-1]] = value


class AutoMLSearcher(SearchAlgorithm):
    """Base class for AutoML search algorithm

    It works in a round-by-round way. For each experiment round,
    it generates a bunch of parameter config permutations, submits
    and keeps track of them. Once all of them finish, results will
    be fed back to the algorithm as a whole.
    """

    CONTINUE = "CONTINUE"
    TERMINATE = "TERMINATE"

    def __init__(self, search_space, reward_attr):
        """Initialize AutoMLSearcher.

        Arguments:
            search_space (SearchSpace): The space to search.
            reward_attr: The attribute name of the reward in the result.
        """
        # Pass experiment later to allow construction without this parameter
        super(AutoMLSearcher, self).__init__()

        self.search_space = search_space
        self.reward_attr = reward_attr

        self.experiment = None
        self.summary = None
        self.best_trial = None
        self._is_finished = False
        self._parser = make_parser()
        self._undone_count = 0
        self._running_trials = {}
        self._completed_trials = {}

        self._iteration = 0
        self._total_trial_num = 0
        self._start_ts = 0

    def setup(self, exp, summary):
        self.experiment = exp
        self.summary = summary

    def get_best_trial(self):
        """Returns the Trial object with the best reward_attr"""
        return self.best_trial

    def next_trials(self):
        if self._undone_count > 0:
            # Last round not finished
            return []

        trials = []
        raw_param_list, extra_arg_list = self._select()
        if not extra_arg_list:
            extra_arg_list = [None] * len(raw_param_list)

        for param_config, extra_arg in zip(raw_param_list, extra_arg_list):
            tag = ''
            new_spec = copy.deepcopy(self.experiment.spec)
            for path, value in param_config.items():
                tag += '%s=%s-' % (path.split('.')[-1], value)
                merge_config(path, value, new_spec['config'])

            trial = create_trial_from_spec(
                new_spec, self.experiment.name,
                self._parser, experiment_tag=tag
            )

            # AutoML specific fields set in Trial
            trial.results = []
            trial.best_result = None
            trial.param_config = param_config
            trial.extra_arg = extra_arg

            trials.append(trial)
            self._running_trials[trial.trial_id] = trial
            # self.summary.on_trial_create(trial)

        ntrial = len(trials)
        self._iteration += 1
        self._undone_count = ntrial
        self._total_trial_num += ntrial
        self._start_ts = time.time()
        logging.info(("=========== BEGIN Experiment-Round: %s "
                      "[%s NEW | %s TOTAL] ===========")
                     % (self._iteration, ntrial, self._total_trial_num))
        return trials

    def _trial_add_result(self, trial, result):
        trial.results.append(result)
        if trial.best_result is None \
                or result[self.reward_attr] > trial.best_result[self.reward_attr]:
            trial.best_result = result

    def on_trial_result(self, trial_id, result):
        if not result:
            return

        # Update trial's best result
        trial = self._running_trials[trial_id]
        self._trial_add_result(trial, result)
        self.summary.on_trial_result(trial, result)

        # Update job's best trial
        if self.best_trial is None \
                or (result[self.reward_attr]
                    > self.best_trial.best_result[self.reward_attr]):
            self.best_trial = self._running_trials[trial_id]

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        self.on_trial_result(trial_id, result)
        self._undone_count -= 1
        if self._undone_count == 0:
            total = len(self._running_trials)
            succ = len(filter(lambda t: t.status == Trial.TERMINATED,
                              self._running_trials.values()))
            this_trial = self._running_trials[trial_id]
            if this_trial.status == Trial.RUNNING and not error:
                succ += 1

            elapsed = time.time() - self._start_ts
            logging.info(("=========== END Experiment-Round: %s [%s TOTAL "
                          "| %s SUCC | %s FAIL] this round, elapsed=%.2fs ===========")
                         % (self._iteration, total, succ, total - succ, elapsed))

            action = self._feedback(self._running_trials.values())
            if action == AutoMLSearcher.TERMINATE:
                self._is_finished = True

            self._completed_trials.update(self._running_trials)
            self._running_trials = {}

    def is_finished(self):
        return self._is_finished

    def _select(self):
        """Select a bunch of parameter permutations to run.

        The permutations should be a list of dict, which contains the <path, value> pair.
        The ``path`` could be a dot separated string, which will be expanded to merge
        into the experiment's config by the framework. For example:
        pair                 : {"path.to.key": 1}
        config in experiment : {"path": {"to": {"key": 1}, ...}, ...}

        The framework generates 1 config for 1 Trial. User could also return an extra list
        to add an additional argument to the trial

        Returns:
            A list of config + a list of extra argument (can be None)
        """
        raise NotImplementedError

    def _feedback(self, trials):
        """Feedback the completed trials corresponding to the last selected
        parameter permutations

        Arguments:
            trials (list): A list of Trial object, where user can fetch the
                result attribute, etc.

        Returns:
            Next action, i.e.: CONTINUE, TERMINATE
        """
        raise NotImplementedError


class GridSearch(AutoMLSearcher):
    """Implement the grid search"""

    def _select(self):
        grid = self.search_space.to_grid_search()
        configs = []
        for _, config in generate_variants(grid):
            configs.append(config)
        return configs, None

    def _feedback(self, trials):
        return AutoMLSearcher.TERMINATE


class RandomSearch(AutoMLSearcher):
    """Implement the random search"""

    def __init__(self, search_space, reward_attr, repeat):
        super(RandomSearch, self).__init__(search_space, reward_attr)
        self.repeat = repeat

    def _select(self):
        choices = self.search_space.to_random_choice()
        configs = []
        for _ in range(self.repeat):
            for _, config in generate_variants(choices):
                configs.append(config)
        return configs, None

    def _feedback(self, trials):
        return AutoMLSearcher.TERMINATE



