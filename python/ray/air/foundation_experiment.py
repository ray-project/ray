from ray.tune.trainable import FunctionTrainable
from ray import air
from ray.train.data_parallel_trainer import DataParallelTrainable

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


import copy
from ray.tune.search.search_algorithm import SearchAlgorithm
from ray.tune.search.basic_variant import _VariantIterator
from ray.tune.search.variant_generator import (
    _count_variants,
    _count_spec_samples,
    generate_variants,
    _flatten_resolved_vars,
    format_vars,
)
from ray.tune.experiment import Experiment, Trial


class _BasicVariantGenerator(SearchAlgorithm):
    def __init__(self, trainable_name: str, run_config, **kwargs):
        import uuid

        self.trainable_name = trainable_name
        self._run_config = run_config

        self.counter = 0
        self._total_samples = 0
        self.uuid_prefix = str(uuid.uuid1().hex)[:5] + "_"
        self._live_trials = set()
        self.max_concurrent = 0
        self._variant_iter = None

    @property
    def total_samples(self):
        return self._total_samples

    def add_configurations(
        self, param_space, num_samples=1, constant_grid_search=False
    ):
        grid_vals = _count_spec_samples(param_space, num_samples=1)
        # lazy_eval = grid_vals > SERIALIZATION_THRESHOLD
        # if lazy_eval:
        #     warnings.warn(
        #         f"The number of pre-generated samples ({grid_vals}) "
        #         "exceeds the serialization threshold "
        #         f"({int(SERIALIZATION_THRESHOLD)}). Resume ability is "
        #         "disabled. To fix this, reduce the number of "
        #         "dimensions/size of the provided grid search."
        #     )

        # previous_samples = self._total_samples
        # points_to_evaluate = copy.deepcopy(self._points_to_evaluate)
        points_to_evaluate = []
        self._total_samples += _count_variants(param_space, points_to_evaluate)
        self._variant_generator = _VariantIterator(
            generate_variants(
                param_space,
                constant_grid_search=constant_grid_search,
                # random_state=random_state,
            ),
            # lazy_eval=lazy_eval,
        )

        # self._trial_generator = _TrialIterator(
        #     uuid_prefix=self._uuid_prefix,
        #     num_samples=num_samples,
        #     unresolved_spec=param_space,
        #     constant_grid_search=self._constant_grid_search,
        #    output_path="testing",
        #     # points_to_evaluate=points_to_evaluate,
        #     # lazy_eval=lazy_eval,
        #     start=previous_samples,
        #     random_state=self._random_state,
        # )

    def _create_trial(self, resolved_vars, config):
        from pathlib import Path

        trial_id = self.uuid_prefix + ("%05d" % self.counter)
        experiment_tag = str(self.counter)
        experiment_tag += "_{}".format(format_vars(resolved_vars))
        evaluated_params = (_flatten_resolved_vars(resolved_vars),)
        self.counter += 1

        return Trial(
            trainable_name=self.trainable_name,
            config=config,
            stopping_criterion=self._run_config.stop,
            experiment_path=str(
                Path(self._run_config.storage_path) / self._run_config.name
            ),
            experiment_dir_name=self._run_config.name,
            sync_config=self._run_config.sync_config,
            checkpoint_config=self._run_config.checkpoint_config,
            trial_id=trial_id,
            experiment_tag=experiment_tag,
            evaluated_params=evaluated_params,
            # export_formats=spec.get("export_formats", []),
            # str(None) doesn't create None
            # restore_path=spec.get("restore"),
            # trial_name_creator=spec.get("trial_name_creator"),
            # trial_dirname_creator=spec.get("trial_dirname_creator"),
            # log_to_file=spec.get("log_to_file"),
            # str(None) doesn't create None
            # max_failures=args.max_failures,
            # **trial_kwargs,
        )

    def next_trial(self):
        """Provides one Trial object to be queued into the TrialRunner.

        Returns:
            Trial: Returns a single trial.
        """
        if self.is_finished():
            return None
        if self.max_concurrent > 0 and len(self._live_trials) >= self.max_concurrent:
            return None
        if not self._variant_iter:
            self._variant_iter = iter(self._variant_generator)
        try:
            resolved_vars, config = next(self._variant_iter)

            # Create trial with config
            trial = self._create_trial(resolved_vars, config)

            self._live_trials.add(trial.trial_id)
            return trial
        except StopIteration:
            self._variant_generator = []
            self._variant_iter = None
            self.set_finished()
            return None


from ray.train.data_parallel_trainer import DataParallelTrainable

# class _TorchTrainable(DataParallelTrainable):
# def _


def _print(msg):
    print("=" * 50)
    print(msg)
    print("=" * 50)
    print()


class ExperimentRunner:
    def __init__(
        self,
        trainable: FunctionTrainable,
        run_config: air.RunConfig,
        config: _Config,
    ):
        self._config = config
        self._experiment = _Experiment(run_config)
        self._run_config = run_config
        self._trainable = trainable

    def fit(self):
        _print("Registering trainable")

        registered_trainable_name = Experiment.register_if_needed(self._trainable)

        _print("Creating search alg")
        search_alg = _BasicVariantGenerator(
            trainable_name=registered_trainable_name, run_config=self._run_config
        )
        # search_alg.add_configurations(self._config.to_dict())
        search_alg.add_configurations(self._config)

        _print("Creating tune controller")
        tune_controller = TuneController(
            search_alg=search_alg,
            experiment_path=self._experiment.path,
            experiment_dir_name=self._experiment.name,
        )

        air_progress_reporter = _detect_air_reporter(
            AirVerbosity(2),
            search_alg.total_samples,
        )

        _print("Starting main tune loop.")
        while not tune_controller.is_finished():
            tune_controller.step()
            _report_air_progress(tune_controller, air_progress_reporter)

        tune_controller.cleanup()

        _report_air_progress(tune_controller, air_progress_reporter, force=True)

        _print("Experiment finished running!!")


if __name__ == "__main__":
    import ray
    from ray import tune
    from ray.air import session
    import time

    def train_loop(config):
        print(f"\nrank = {session.get_world_rank()}")
        print(f"a = {config['a']}")

        ds = session.get_dataset_shard("train")
        for batch in ds.iter_batches():
            print(batch)

        for i in range(10):
            time.sleep(0.5)
            session.report({"train_session_report": 1})

    runner = ExperimentRunner(
        DataParallelTrainable,
        run_config=air.RunConfig(
            storage_path="~/ray_results", name="foundation_experimentation"
        ),
        config={
            "scaling_config": air.ScalingConfig(num_workers=2),
            "datasets": {
                "train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])
            },
            "train_loop_per_worker": train_loop,
            "train_loop_config": {"a": tune.grid_search([1, 2, 3])},
        },
    )

    runner.fit()


"""
### Ideal API

def train_loop_per_worker(config):
    pass

class TorchTrainerConfig(TuneableConfig):
    pass

runner = ExperimentRunner(
    TorchTrainer,
    config=TorchTrainerConfig(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={"lr": tune.uniform(0, 1)},
        datasets={"train": ray.data.from_items(...)},
        scaling_config=air.ScalingConfig(),
    ),
    run_config=air.RunConfig(
    ),
    tune_config=TuneConfig(searcher=OptunaSearch(), scheduler=ASHA())
)

# Can we make everything a callback??

runner = ExperimentRunner(
    TorchTrainer,
    config=TorchTrainerConfig(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={"lr": tune.uniform(0, 1)},
        datasets={"train": ray.data.from_items(...)},
        scaling_config=air.ScalingConfig(),
        callbacks=[OptunaSearcher(), ]
    ),
    run_config=air.RunConfig(
    ),
    tune_config=TuneConfig(searcher=OptunaSearch(), scheduler=ASHA())
)



## How to deal with Tune specific stuff? Scheduler and searcher?
## How to deal with multiple places to input the search space? E.g. directly through searchers?



runner = (
    ExperimentRunner(TorchTrainer, config=TorchTrainerConfig())
    .config()
    .tuning(searcher=)
)

"""
