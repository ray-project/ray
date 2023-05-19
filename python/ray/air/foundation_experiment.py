import os
from pathlib import Path
import time

import ray
import ray.cloudpickle as pickle
from ray.tune.trainable import FunctionTrainable
from ray import air, tune
from ray.air import session, Checkpoint
from ray.train.data_parallel_trainer import (
    DataParallelTrainable,
    DataParallelTrainerConfig,
    _Config,
)
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experimental.output import (
    AirVerbosity,
    _detect_reporter as _detect_air_reporter,
)
from ray.tune.tune import _report_air_progress
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

    def has_checkpoint(self, dirpath: str) -> bool:
        """Should return False if restoring is not implemented."""
        return "searcher_state.pkl" in os.listdir(dirpath)

    def save_to_dir(self, dirpath: str, **kwargs):
        """Saves a search algorithm."""
        save_path = Path(dirpath) / "searcher_state.pkl"
        with open(save_path, "wb") as f:
            pickle.dump(self.__dict__.copy(), f)

    def restore_from_dir(self, dirpath: str):
        """Restores a search algorithm along with its wrapped state."""
        save_path = Path(dirpath) / "searcher_state.pkl"
        with open(save_path, "rb") as f:
            state = pickle.load(f)

        self.__dict__.update(state)


def _print(msg):
    print("=" * 50)
    print(msg)
    print("=" * 50)
    print()


class ExperimentRunner:
    def __init__(
        self,
        trainable: FunctionTrainable,
        config: _Config,
        run_config: air.RunConfig,
        tune_config: tune.TuneConfig = None,
    ):
        self._config = config
        self._run_config = run_config
        self._trainable = trainable
        self._tune_config = tune_config  # TODO: handle tune config

        self._restored = False

    def fit(self):
        save_path = Path(self._run_config.storage_path) / self._run_config.name
        save_path.mkdir(parents=True, exist_ok=True)

        runner_state = {
            "run_config": self._run_config,
            "tune_config": self._tune_config,
        }
        with open(save_path / "exp_runner.pkl", "wb") as f:
            pickle.dump(runner_state, f)

        self._new_tune_run()

    @classmethod
    def restore(cls, path, trainable, config={}) -> "ExperimentRunner":
        path = Path(path).expanduser().resolve()
        with open(path / "exp_runner.pkl", "rb") as f:
            runner_state = pickle.load(f)
        run_config = runner_state["run_config"]
        tune_config = runner_state["tune_config"]

        restored_runner = ExperimentRunner(
            trainable, config, run_config, tune_config=tune_config
        )
        restored_runner._restored = True
        return restored_runner

    def _new_tune_run(self):
        _print("Registering trainable")

        registered_trainable_name = Experiment.register_if_needed(self._trainable)

        _print("Creating search alg")
        search_alg = _BasicVariantGenerator(
            trainable_name=registered_trainable_name, run_config=self._run_config
        )
        config_as_dict = (
            self._config.to_dict()
            if isinstance(self._config, _Config)
            else self._config
        )
        search_alg.add_configurations(config_as_dict)

        _print("Creating tune controller")

        if self._restored:
            _print("tune controller resuming!!!")

        tune_controller = TuneController(
            search_alg=search_alg,
            experiment_path=str(
                Path(self._run_config.storage_path) / self._run_config.name
            ),
            experiment_dir_name=self._run_config.name,
            run_config=self._run_config,
            resume="AUTO+ERRORED_ONLY" if self._restored else False,
        )

        air_progress_reporter = _detect_air_reporter(
            AirVerbosity(2),
            search_alg.total_samples,
        )

        _print("Starting main tune loop.")
        while not tune_controller.is_finished():
            tune_controller.step()
            _report_air_progress(tune_controller, air_progress_reporter)

        try:
            tune_controller.checkpoint(force=True, wait=True)
        except Exception as e:
            tune_controller.warning(f"Trial Runner checkpointing failed: {str(e)}")

        tune_controller.cleanup()

        _report_air_progress(tune_controller, air_progress_reporter, force=True)

        _print("Experiment finished running!!")


class _BaseTrainer:
    pass


class _DataParallelTrainer(_BaseTrainer):
    _trainable_cls = DataParallelTrainable
    _config_cls = DataParallelTrainerConfig

    def __init__(self, run_config: air.RunConfig = None, **kwargs):
        self.kwargs = kwargs
        self.run_config = run_config

    def fit(self):
        runner = ExperimentRunner(
            self._trainable_cls,
            config=self._config_cls(**self.kwargs),
            run_config=self.run_config,
        )
        return runner.fit()


class _Tuner:
    def __init__(
        self,
        trainable_or_trainer,
        param_space={},
        run_config: air.RunConfig = None,
        tune_config: tune.TuneConfig = None,
    ):
        if isinstance(trainable_or_trainer, _BaseTrainer):
            trainable = trainable_or_trainer._trainable_cls
            trainer_run_config = trainable_or_trainer.run_config
            config = trainable_or_trainer.kwargs
        else:
            trainable = trainable_or_trainer
            config = {}

        self._trainable = trainable
        self._run_config = run_config
        self._tune_config = tune_config

        def merge_dicts(d1, d2):
            for k in d2:
                if k not in d1:
                    d1[k] = d2[k]
            for k, v in d1.items():
                if isinstance(v, dict) and k in d2:
                    merge_dicts(d1[k], d2[k])

        if isinstance(param_space, _Config):
            param_space = param_space.to_dict()

        merge_dicts(config, param_space)
        self._config = config

    def fit(self):
        runner = ExperimentRunner(
            self._trainable,
            config=self._config,
            run_config=self._run_config,
            tune_config=self._tune_config,
        )
        return runner.fit()


def train_loop(config):
    print(f"\nrank = {session.get_world_rank()}")
    print(f"a = {config['a']}")

    ckpt = session.get_checkpoint()
    has_ckpt = bool(ckpt)
    print(f"has checkpoint = {has_ckpt}")

    start = ckpt.to_dict()["it"] + 1 if ckpt else 1

    ds = session.get_dataset_shard("train")
    for batch in ds.iter_batches():
        print(batch)

    for i in range(start, 11):
        if i == 5 and not has_ckpt:
            raise RuntimeError
        time.sleep(1.0)
        session.report(
            {"train_session_report": 1}, checkpoint=Checkpoint.from_dict({"it": i})
        )


def run_with_exp_runner():
    runner = ExperimentRunner(
        DataParallelTrainable,
        run_config=air.RunConfig(
            storage_path="~/ray_results", name="foundation_experiment"
        ),
        # Use a dict
        # config={
        #     "scaling_config": air.ScalingConfig(num_workers=2),
        #     "datasets": {
        #         "train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])
        #     },
        #     "train_loop_per_worker": train_loop,
        #     "train_loop_config": {"a": tune.grid_search([1, 2, 3])},
        # },
        # Or a data parallel trainer config dataclass
        config=DataParallelTrainerConfig(
            train_loop_per_worker=train_loop,
            train_loop_config={"a": tune.grid_search([1, 2, 3])},
            scaling_config=air.ScalingConfig(num_workers=2),
            datasets={
                "train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])
            },
        ),
    )

    runner.fit()


def restore_with_exp_runner():
    runner = ExperimentRunner.restore(
        path="~/ray_results/foundation_experiment",
        trainable=DataParallelTrainable,
    )
    runner.fit()


def run_with_trainer():
    trainer = _DataParallelTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config={"a": 1},
        scaling_config=air.ScalingConfig(num_workers=2),
        datasets={
            "train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])
        },
        run_config=air.RunConfig(
            storage_path="~/ray_results", name="foundation_run_with_trainer"
        ),
    )
    trainer.fit()


def run_with_tuner():
    trainer = _DataParallelTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config={"a": tune.grid_search([1, 2, 3])},
        scaling_config=air.ScalingConfig(num_workers=2),
        datasets={
            "train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])
        },
        # run_config=air.RunConfig(
        #     storage_path="~/ray_results", name="foundation_experimentation"
        # ),
    )

    tuner = _Tuner(
        trainer,
        param_space=DataParallelTrainerConfig(
            train_loop_per_worker=train_loop,
            train_loop_config={"a": tune.grid_search([1, 2, 3])},
            scaling_config=air.ScalingConfig(num_workers=2),
            datasets={
                "train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])
            },
        ),
        run_config=air.RunConfig(
            storage_path="~/ray_results", name="foundation_run_with_tuner"
        ),
    )
    tuner.fit()


if __name__ == "__main__":
    run_with_exp_runner()
    restore_with_exp_runner()

    # run_with_trainer()
    # run_with_tuner()


"""
# Brainstorming

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
