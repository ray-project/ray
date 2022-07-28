import copy
import logging
from typing import Dict, List, Optional, Union

from ray.tune.error import TuneError
from ray.tune.experiment import Experiment, _convert_to_experiment_list
from ray.tune.experiment.config_parser import _make_parser, _create_trial_from_spec
from ray.tune.search.search_algorithm import SearchAlgorithm
from ray.tune.search import Searcher
from ray.tune.search.util import _set_search_properties_backwards_compatible
from ray.tune.search.variant_generator import format_vars, _resolve_nested_dict
from ray.tune.experiment import Trial
from ray.tune.utils.util import (
    flatten_dict,
    merge_dicts,
    _atomic_save,
    _load_newest_checkpoint,
)
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


def _warn_on_repeater(searcher, total_samples):
    from ray.tune.search.repeater import _warn_num_samples

    _warn_num_samples(searcher, total_samples)


@DeveloperAPI
class SearchGenerator(SearchAlgorithm):
    """Generates trials to be passed to the TrialRunner.

    Uses the provided ``searcher`` object to generate trials. This class
    transparently handles repeating trials with score aggregation
    without embedding logic into the Searcher.

    Args:
        searcher: Search object that subclasses the Searcher base class. This
            is then used for generating new hyperparameter samples.
    """

    CKPT_FILE_TMPL = "search_gen_state-{}.json"

    def __init__(self, searcher: Searcher):
        assert issubclass(
            type(searcher), Searcher
        ), "Searcher should be subclassing Searcher."
        self.searcher = searcher
        self._parser = _make_parser()
        self._experiment = None
        self._counter = 0  # Keeps track of number of trials created.
        self._total_samples = 0  # int: total samples to evaluate.
        self._finished = False

    @property
    def metric(self):
        return self.searcher.metric

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        return _set_search_properties_backwards_compatible(
            self.searcher.set_search_properties, metric, mode, config, **spec
        )

    @property
    def total_samples(self):
        return self._total_samples

    def add_configurations(
        self, experiments: Union[Experiment, List[Experiment], Dict[str, Dict]]
    ):
        """Registers experiment specifications.

        Arguments:
            experiments: Experiments to run.
        """
        assert not self._experiment
        logger.debug("added configurations")
        experiment_list = _convert_to_experiment_list(experiments)
        assert (
            len(experiment_list) == 1
        ), "SearchAlgorithms can only support 1 experiment at a time."
        self._experiment = experiment_list[0]
        experiment_spec = self._experiment.spec
        self._total_samples = self._experiment.spec.get("num_samples", 1)

        _warn_on_repeater(self.searcher, self._total_samples)
        if "run" not in experiment_spec:
            raise TuneError("Must specify `run` in {}".format(experiment_spec))

    def next_trial(self):
        """Provides one Trial object to be queued into the TrialRunner.

        Returns:
            Trial: Returns a single trial.
        """
        if not self.is_finished():
            return self.create_trial_if_possible(
                self._experiment.spec, self._experiment.dir_name
            )
        return None

    def create_trial_if_possible(
        self, experiment_spec: Dict, output_path: str
    ) -> Optional[Trial]:
        logger.debug("creating trial")
        trial_id = Trial.generate_id()
        suggested_config = self.searcher.suggest(trial_id)
        if suggested_config == Searcher.FINISHED:
            self._finished = True
            logger.debug("Searcher has finished.")
            return

        if suggested_config is None:
            return
        spec = copy.deepcopy(experiment_spec)
        spec["config"] = merge_dicts(spec["config"], copy.deepcopy(suggested_config))

        # Create a new trial_id if duplicate trial is created
        flattened_config = _resolve_nested_dict(spec["config"])
        self._counter += 1
        tag = "{0}_{1}".format(str(self._counter), format_vars(flattened_config))
        trial = _create_trial_from_spec(
            spec,
            output_path,
            self._parser,
            evaluated_params=flatten_dict(suggested_config),
            experiment_tag=tag,
            trial_id=trial_id,
        )
        return trial

    def on_trial_result(self, trial_id: str, result: Dict):
        """Notifies the underlying searcher."""
        self.searcher.on_trial_result(trial_id, result)

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        self.searcher.on_trial_complete(trial_id=trial_id, result=result, error=error)

    def is_finished(self) -> bool:
        return self._counter >= self._total_samples or self._finished

    def get_state(self) -> Dict:
        return {
            "counter": self._counter,
            "total_samples": self._total_samples,
            "finished": self._finished,
            "experiment": self._experiment,
        }

    def set_state(self, state: Dict):
        self._counter = state["counter"]
        self._total_samples = state["total_samples"]
        self._finished = state["finished"]
        self._experiment = state["experiment"]

    def has_checkpoint(self, dirpath: str):
        return bool(_load_newest_checkpoint(dirpath, self.CKPT_FILE_TMPL.format("*")))

    def save_to_dir(self, dirpath: str, session_str: str):
        """Saves self + searcher to dir.

        Separates the "searcher" from its wrappers (concurrency, repeating).
        This allows the user to easily restore a given searcher.

        The save operation is atomic (write/swap).

        Args:
            dirpath: Filepath to experiment dir.
            session_str: Unique identifier of the current run
                session.
        """
        searcher = self.searcher
        search_alg_state = self.get_state()
        while hasattr(searcher, "searcher"):
            searcher_name = type(searcher).__name__
            if searcher_name in search_alg_state:
                logger.warning(
                    "There was a duplicate when saving {}. "
                    "Restore may not work properly.".format(searcher_name)
                )
            else:
                search_alg_state["name:" + searcher_name] = searcher.get_state()
            searcher = searcher.searcher
        base_searcher = searcher
        # We save the base searcher separately for users to easily
        # separate the searcher.
        base_searcher.save_to_dir(dirpath, session_str)
        _atomic_save(
            state=search_alg_state,
            checkpoint_dir=dirpath,
            file_name=self.CKPT_FILE_TMPL.format(session_str),
            tmp_file_name=".tmp_search_generator_ckpt",
        )

    def restore_from_dir(self, dirpath: str):
        """Restores self + searcher + search wrappers from dirpath."""

        searcher = self.searcher
        search_alg_state = _load_newest_checkpoint(
            dirpath, self.CKPT_FILE_TMPL.format("*")
        )
        if not search_alg_state:
            raise RuntimeError("Unable to find checkpoint in {}.".format(dirpath))
        while hasattr(searcher, "searcher"):
            searcher_name = "name:" + type(searcher).__name__
            if searcher_name not in search_alg_state:
                names = [
                    key.split("name:")[1]
                    for key in search_alg_state
                    if key.startswith("name:")
                ]
                logger.warning(
                    "{} was not found in the experiment checkpoint "
                    "state when restoring. Found {}.".format(searcher_name, names)
                )
            else:
                searcher.set_state(search_alg_state.pop(searcher_name))
            searcher = searcher.searcher
        base_searcher = searcher

        logger.debug(f"searching base {base_searcher}")
        base_searcher.restore_from_dir(dirpath)
        self.set_state(search_alg_state)
