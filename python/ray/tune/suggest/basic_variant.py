from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.variant_generator import generate_variants
from ray.tune.suggest.search import SearchAlgorithm


class BasicVariantGenerator(SearchAlgorithm):
    """Uses Tune's variant generation for resolving variables.

    See also: `ray.tune.suggest.variant_generator`.

    Example:
        >>> searcher = BasicVariantGenerator({"experiment": { ... }})
        >>> list_of_trials = searcher.next_trials()
        >>> searcher.is_finished == True
    """

    def __init__(self, experiments=None):
        """Constructs a generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        self._parser = make_parser()
        self._trial_generator = chain.from_iterable([
            self._generate_trials(experiment.spec, experiment.name)
            for experiment in experiment_list
        ])
        self._counter = 0
        self._finished = False

    def next_trials(self):
        """Provides Trial objects to be queued into the TrialRunner.

        Returns:
            trials (list): Returns a list of trials.
        """
        trials = list(self._trial_generator)
        self._finished = True
        return trials

    def _generate_trials(self, unresolved_spec, output_path=""):
        """Generates Trial objects with the variant generation process.

        Uses a fixed point iteration to resolve variants. All trials
        should be able to be generated at once.

        See also: `ray.tune.suggest.variant_generator`.

        Yields:
            Trial object
        """

        if "run" not in unresolved_spec:
            raise TuneError("Must specify `run` in {}".format(unresolved_spec))
        for _ in range(unresolved_spec.get("repeat", 1)):
            for resolved_vars, spec in generate_variants(unresolved_spec):
                experiment_tag = str(self._counter)
                if resolved_vars:
                    experiment_tag += "_{}".format(resolved_vars)
                self._counter += 1
                yield create_trial_from_spec(
                    spec,
                    output_path,
                    self._parser,
                    experiment_tag=experiment_tag)

    def is_finished(self):
        return self._finished
