import copy
import itertools
import os
import uuid
from typing import Dict, List, Optional, Union

from ray.tune.error import TuneError
from ray.tune.experiment import Experiment, convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.variant_generator import (
    count_variants, generate_variants, format_vars, flatten_resolved_vars,
    get_preset_variants)
from ray.tune.suggest.search import SearchAlgorithm


class BasicVariantGenerator(SearchAlgorithm):
    """Uses Tune's variant generation for resolving variables.

    This is the default search algorithm used if no other search algorithm
    is specified.


    Args:
        points_to_evaluate (list): Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want to run first to help the algorithm make better suggestions
            for future parameters. Needs to be a list of dicts containing the
            configurations.


    Example:

    .. code-block:: python

        from ray import tune

        # This will automatically use the `BasicVariantGenerator`
        tune.run(
            lambda config: config["a"] + config["b"],
            config={
                "a": tune.grid_search([1, 2]),
                "b": tune.randint(0, 3)
            },
            num_samples=4)

    In the example above, 8 trials will be generated: For each sample
    (``4``), each of the grid search variants for ``a`` will be sampled
    once. The ``b`` parameter will be sampled randomly.

    The generator accepts a pre-set list of points that should be evaluated.
    The points will replace the first samples of each experiment passed to
    the ``BasicVariantGenerator``.

    Each point will replace one sample of the specified ``num_samples``. If
    grid search variables are overwritten with the values specified in the
    presets, the number of samples will thus be reduced.

    Example:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.basic_variant import BasicVariantGenerator


        tune.run(
            lambda config: config["a"] + config["b"],
            config={
                "a": tune.grid_search([1, 2]),
                "b": tune.randint(0, 3)
            },
            search_alg=BasicVariantGenerator(points_to_evaluate=[
                {"a": 2, "b": 2},
                {"a": 1},
                {"b": 2}
            ]),
            num_samples=4)

    The example above will produce six trials via four samples:

    - The first sample will produce one trial with ``a=2`` and ``b=2``.
    - The second sample will produce one trial with ``a=1`` and ``b`` sampled
      randomly
    - The third sample will produce two trials, one for each grid search
      value of ``a``. It will be ``b=2`` for both of these trials.
    - The fourth sample will produce two trials, one for each grid search
      value of ``a``. ``b`` will be sampled randomly and independently for
      both of these trials.

    """

    def __init__(self, points_to_evaluate: Optional[List[Dict]] = None):
        """Initializes the Variant Generator.

        """
        self._parser = make_parser()
        self._trial_generator = []
        self._trial_iter = None
        self._counter = 0
        self._finished = False

        self._points_to_evaluate = points_to_evaluate or []

        # Unique prefix for all trials generated, e.g., trial ids start as
        # 2f1e_00001, 2f1ef_00002, 2f1ef_0003, etc. Overridable for testing.
        force_test_uuid = os.environ.get("_TEST_TUNE_TRIAL_UUID")
        if force_test_uuid:
            self._uuid_prefix = force_test_uuid + "_"
        else:
            self._uuid_prefix = str(uuid.uuid1().hex)[:5] + "_"

        self._total_samples = 0

    @property
    def total_samples(self):
        return self._total_samples

    def add_configurations(
            self,
            experiments: Union[Experiment, List[Experiment], Dict[str, Dict]]):
        """Chains generator given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        experiment_list = convert_to_experiment_list(experiments)
        for experiment in experiment_list:
            points_to_evaluate = copy.deepcopy(self._points_to_evaluate)
            self._total_samples += count_variants(experiment.spec,
                                                  points_to_evaluate)
            self._trial_generator = itertools.chain(
                self._trial_generator,
                self._generate_trials(
                    experiment.spec.get("num_samples", 1), experiment.spec,
                    experiment.dir_name, points_to_evaluate))

    def next_trial(self):
        """Provides one Trial object to be queued into the TrialRunner.

        Returns:
            Trial: Returns a single trial.
        """
        if not self._trial_iter:
            self._trial_iter = iter(self._trial_generator)
        try:
            return next(self._trial_iter)
        except StopIteration:
            self._trial_generator = []
            self._trial_iter = None
            self.set_finished()
            return None

    def _generate_trials(self,
                         num_samples,
                         unresolved_spec,
                         output_path="",
                         points_to_evaluate=None):
        """Generates Trial objects with the variant generation process.

        Uses a fixed point iteration to resolve variants. All trials
        should be able to be generated at once.

        See also: `ray.tune.suggest.variant_generator`.

        Yields:
            Trial object
        """

        if "run" not in unresolved_spec:
            raise TuneError("Must specify `run` in {}".format(unresolved_spec))

        points_to_evaluate = points_to_evaluate or []

        while points_to_evaluate:
            config = points_to_evaluate.pop(0)
            for resolved_vars, spec in get_preset_variants(
                    unresolved_spec, config):
                trial_id = self._uuid_prefix + ("%05d" % self._counter)
                experiment_tag = str(self._counter)
                self._counter += 1
                yield create_trial_from_spec(
                    spec,
                    output_path,
                    self._parser,
                    evaluated_params=flatten_resolved_vars(resolved_vars),
                    trial_id=trial_id,
                    experiment_tag=experiment_tag)
            num_samples -= 1

        if num_samples <= 0:
            return

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
