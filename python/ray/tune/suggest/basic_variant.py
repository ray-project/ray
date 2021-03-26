import copy
import glob
import itertools
import os
import uuid
from typing import Dict, List, Optional, Union
import warnings

from ray.tune.error import TuneError
from ray.tune.experiment import Experiment, convert_to_experiment_list
from ray.tune.config_parser import make_parser, create_trial_from_spec
from ray.tune.suggest.variant_generator import (
    count_variants, count_spec_samples, generate_variants, format_vars,
    flatten_resolved_vars, get_preset_variants)
from ray.tune.suggest.search import SearchAlgorithm
from ray.tune.utils.util import atomic_save, load_newest_checkpoint

SERIALIZATION_THRESHOLD = 1e6


class _VariantIterator:
    """Iterates over generated variants from the search space.

    This object also toggles between lazy evaluation and
    eager evaluation of samples. If lazy evaluation is enabled,
    this object cannot be serialized.
    """

    def __init__(self, iterable, lazy_eval=False):
        self.lazy_eval = lazy_eval
        self.iterable = iterable
        self._has_next = True
        if lazy_eval:
            self._load_value()
        else:
            self.iterable = list(iterable)
            self._has_next = bool(self.iterable)

    def _load_value(self):
        try:
            self.next_value = next(self.iterable)
        except StopIteration:
            self._has_next = False

    def has_next(self):
        return self._has_next

    def __next__(self):
        if self.lazy_eval:
            current_value = self.next_value
            self._load_value()
            return current_value
        current_value = self.iterable.pop(0)
        self._has_next = bool(self.iterable)
        return current_value


class _TrialIterator:
    """Generates trials from the spec.

    Args:
        uuid_prefix (str): Used in creating the trial name.
        num_samples (int): Number of samples from distribution
             (same as tune.run).
        unresolved_spec (dict): Experiment specification
            that might have unresolved distributions.
        output_path (str): A specific output path within the local_dir.
        points_to_evaluate (list): Same as tune.run.
        lazy_eval (bool): Whether variants should be generated
            lazily or eagerly. This is toggled depending
            on the size of the grid search.
        start (int): index at which to start counting trials.
    """

    def __init__(self,
                 uuid_prefix: str,
                 num_samples: int,
                 unresolved_spec: dict,
                 output_path: str = "",
                 points_to_evaluate: Optional[List] = None,
                 lazy_eval: bool = False,
                 start: int = 0):
        self.parser = make_parser()
        self.num_samples = num_samples
        self.uuid_prefix = uuid_prefix
        self.num_samples_left = num_samples
        self.unresolved_spec = unresolved_spec
        self.output_path = output_path
        self.points_to_evaluate = points_to_evaluate or []
        self.num_points_to_evaluate = len(self.points_to_evaluate)
        self.counter = start
        self.lazy_eval = lazy_eval
        self.variants = None

    def create_trial(self, resolved_vars, spec):
        trial_id = self.uuid_prefix + ("%05d" % self.counter)
        experiment_tag = str(self.counter)
        # Always append resolved vars to experiment tag?
        if resolved_vars:
            experiment_tag += "_{}".format(format_vars(resolved_vars))
        self.counter += 1
        return create_trial_from_spec(
            spec,
            self.output_path,
            self.parser,
            evaluated_params=flatten_resolved_vars(resolved_vars),
            trial_id=trial_id,
            experiment_tag=experiment_tag)

    def __next__(self):
        """Generates Trial objects with the variant generation process.

        Uses a fixed point iteration to resolve variants. All trials
        should be able to be generated at once.

        See also: `ray.tune.suggest.variant_generator`.

        Returns:
            Trial object
        """

        if "run" not in self.unresolved_spec:
            raise TuneError("Must specify `run` in {}".format(
                self.unresolved_spec))

        if self.variants and self.variants.has_next():
            # This block will be skipped upon instantiation.
            # `variants` will be set later after the first loop.
            resolved_vars, spec = next(self.variants)
            return self.create_trial(resolved_vars, spec)

        if self.points_to_evaluate:
            config = self.points_to_evaluate.pop(0)
            self.num_samples_left -= 1
            self.variants = _VariantIterator(
                get_preset_variants(self.unresolved_spec, config),
                lazy_eval=self.lazy_eval)
            resolved_vars, spec = next(self.variants)
            return self.create_trial(resolved_vars, spec)
        elif self.num_samples_left > 0:
            self.variants = _VariantIterator(
                generate_variants(self.unresolved_spec),
                lazy_eval=self.lazy_eval)
            self.num_samples_left -= 1
            resolved_vars, spec = next(self.variants)
            return self.create_trial(resolved_vars, spec)
        else:
            raise StopIteration

    def __iter__(self):
        return self


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
    CKPT_FILE_TMPL = "basic-variant-state-{}.json"

    def __init__(self, points_to_evaluate: Optional[List[Dict]] = None):
        self._trial_generator = []
        self._iterators = []
        self._trial_iter = None
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
            grid_vals = count_spec_samples(experiment.spec, num_samples=1)
            lazy_eval = grid_vals > SERIALIZATION_THRESHOLD
            if lazy_eval:
                warnings.warn(
                    f"The number of pre-generated samples ({grid_vals}) "
                    "exceeds the serialization threshold "
                    f"({int(SERIALIZATION_THRESHOLD)}). Resume ability is "
                    "disabled. To fix this, reduce the number of "
                    "dimensions/size of the provided grid search.")

            previous_samples = self._total_samples
            points_to_evaluate = copy.deepcopy(self._points_to_evaluate)
            self._total_samples += count_variants(experiment.spec,
                                                  points_to_evaluate)
            iterator = _TrialIterator(
                uuid_prefix=self._uuid_prefix,
                num_samples=experiment.spec.get("num_samples", 1),
                unresolved_spec=experiment.spec,
                output_path=experiment.dir_name,
                points_to_evaluate=points_to_evaluate,
                lazy_eval=lazy_eval,
                start=previous_samples)
            self._iterators.append(iterator)
            self._trial_generator = itertools.chain(self._trial_generator,
                                                    iterator)

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

    def get_state(self):
        if any(iterator.lazy_eval for iterator in self._iterators):
            return False
        state = self.__dict__.copy()
        del state["_trial_generator"]
        return state

    def set_state(self, state):
        self.__dict__.update(state)
        for iterator in self._iterators:
            self._trial_generator = itertools.chain(self._trial_generator,
                                                    iterator)

    def save_to_dir(self, dirpath, session_str):
        if any(iterator.lazy_eval for iterator in self._iterators):
            return False
        state_dict = self.get_state()
        atomic_save(
            state=state_dict,
            checkpoint_dir=dirpath,
            file_name=self.CKPT_FILE_TMPL.format(session_str),
            tmp_file_name=".tmp_generator")

    def has_checkpoint(self, dirpath: str):
        """Whether a checkpoint file exists within dirpath."""
        return bool(
            glob.glob(os.path.join(dirpath, self.CKPT_FILE_TMPL.format("*"))))

    def restore_from_dir(self, dirpath: str):
        """Restores self + searcher + search wrappers from dirpath."""
        state_dict = load_newest_checkpoint(dirpath,
                                            self.CKPT_FILE_TMPL.format("*"))
        if not state_dict:
            raise RuntimeError(
                "Unable to find checkpoint in {}.".format(dirpath))
        self.set_state(state_dict)
