from ray.tune.trainable import FunctionTrainable
from ray import air

from ray.tune.execution.tune_controller import TuneController
from ray.tune.search.basic_variant import BasicVariantGenerator
from ray.tune.experimental.output import (
    AirVerbosity,
    _detect_reporter as _detect_air_reporter,
)
from ray.tune.tune import _report_air_progress

from pathlib import Path


class _Config:
    def to_dict(self):
        pass


class _Experiment:
    def __init__(self, run_config):
        self._run_config = run_config

    @property
    def path(self):
        return str(Path(self._run_config.storage_path) / self._run_config.name)

    @property
    def name(self):
        return self._run_config.name


class ExperimentRunner:
    def __init__(
        self,
        trainable: FunctionTrainable,
        run_config: air.RunConfig,
        config: _Config,
    ):
        self._config = config
        self._experiment = _Experiment(run_config)
        self._trainable = trainable

    def fit(self):
        search_alg = BasicVariantGenerator(max_concurrent=0)
        search_alg._add_configurations(self._config.to_dict())

        tune_controller = TuneController(
            search_alg=search_alg,
            experiment_path=self._experiment.path,
            experiment_dir_name=self._experiment.name,
        )

        air_progress_reporter = _detect_air_reporter(
            AirVerbosity(1),
            search_alg.total_samples,
        )

        while not tune_controller.is_finished():
            tune_controller.step()
            _report_air_progress(tune_controller, air_progress_reporter)

        tune_controller.cleanup()
