import itertools
import os
import random
import uuid

from ray.tune.error import TuneError
from ray.tune.experiment import convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.variant_generator import (generate_variants, format_vars,
                                                flatten_resolved_vars)
from ray.tune.suggest.search import SearchAlgorithm


class BasicVariantGenerator(SearchAlgorithm):
    """Uses Tune's variant generation for resolving variables.

    See also: `ray.tune.suggest.variant_generator`.


    Parameters:
        shuffle (bool): Shuffles the generated list of configurations.

    User API:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest import BasicVariantGenerator

        searcher = BasicVariantGenerator()
        tune.run(my_trainable_func, algo=searcher)

    Internal API:

    .. code-block:: python

        from ray.tune.suggest import BasicVariantGenerator

        searcher = BasicVariantGenerator()
        searcher.add_configurations({"experiment": { ... }})
        list_of_trials = searcher.next_trials()
        searcher.is_finished == True
    """

    def __init__(self, shuffle=False):
        """Initializes the Variant Generator.

        """
        self._parser = make_parser()
        self._trial_generator = []
        self._counter = 0
        self._finished = False
        self._shuffle = shuffle

        # Unique prefix for all trials generated, e.g., trial ids start as
        # 2f1e_00001, 2f1ef_00002, 2f1ef_0003, etc. Overridable for testing.
        force_test_uuid = os.environ.get("_TEST_TUNE_TRIAL_UUID")
        if force_test_uuid:
            self._uuid_prefix = force_test_uuid + "_"
        else:
            self._uuid_prefix = str(uuid.uuid1().hex)[:5] + "_"

    def add_configurations(self, experiments):
        """Chains generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        for experiment in experiment_list:
            self._trial_generator = itertools.chain(
                self._trial_generator,
                self._generate_trials(
                    experiment.spec.get("num_samples", 1), experiment.spec,
                    experiment.name))

    def next_trials(self):
        """Provides Trial objects to be queued into the TrialRunner.

        Returns:
            trials (list): Returns a list of trials.
        """
        trials = list(self._trial_generator)
        if self._shuffle:
            random.shuffle(trials)
        self.set_finished()
        return trials

    def _generate_trials(self, num_samples, unresolved_spec, output_path=""):
        """Generates Trial objects with the variant generation process.

        Uses a fixed point iteration to resolve variants. All trials
        should be able to be generated at once.

        See also: `ray.tune.suggest.variant_generator`.

        Yields:
            Trial object
        """

        if "run" not in unresolved_spec:
            raise TuneError("Must specify `run` in {}".format(unresolved_spec))
        for _ in range(num_samples):
            for resolved_vars, spec in generate_variants(unresolved_spec):
                trial_id = self._uuid_prefix + ("%05d" % self._counter)
                experiment_tag = str(self._counter)
                if resolved_vars:
                    experiment_tag += "_{}".format(format_vars(resolved_vars))
                self._counter += 1
                yield create_trial_from_spec(
                    spec,
                    output_path,
                    self._parser,
                    evaluated_params=flatten_resolved_vars(resolved_vars),
                    trial_id=trial_id,
                    experiment_tag=experiment_tag)
