import inspect
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Type, Union
from tabulate import tabulate

import ray
from ray import tune
from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig, CheckpointConfig
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.train import BackendConfig, TrainingIterator
from ray.train._internal.backend_executor import BackendExecutor, TrialInfo
from ray.train._internal.checkpoint import TuneCheckpointManager
from ray.train._internal.dataset_spec import DataParallelIngestSpec
from ray.train._internal.utils import construct_train_func
from ray.train.constants import TRAIN_DATASET_KEY, WILDCARD_KEY
from ray.train.trainer import BaseTrainer, GenDataset
from ray.util.annotations import DeveloperAPI
from ray.widgets import Template
from ray.widgets.util import ensure_notebook_deps

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


# TODO(team-ml): Refactor checkpoint management along with Tune.
class _DataParallelCheckpointManager(TuneCheckpointManager):
    def __init__(
        self,
        preprocessor: "Preprocessor",
        run_dir: Optional[Path] = None,
        checkpoint_strategy: Optional[CheckpointConfig] = None,
    ):
        self.preprocessor = preprocessor
        super(_DataParallelCheckpointManager, self).__init__(
            run_dir=run_dir, checkpoint_strategy=checkpoint_strategy
        )

    def _process_persistent_checkpoint(self, checkpoint: _TrackedCheckpoint):
        if isinstance(checkpoint.dir_or_data, dict):
            checkpoint.dir_or_data[PREPROCESSOR_KEY] = self.preprocessor
        else:
            save_preprocessor_to_dir(self.preprocessor, checkpoint.dir_or_data)
        super(_DataParallelCheckpointManager, self)._process_persistent_checkpoint(
            checkpoint=checkpoint
        )


@DeveloperAPI
class DataParallelTrainer(BaseTrainer):
    """A Trainer for data parallel training.

    You should subclass this Trainer if your Trainer follows SPMD (single program,
    multiple data) programming paradigm - you want multiple processes to run the same
    function, but on different data.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors.

    The ``train_loop_per_worker`` function is expected to take in either 0 or 1
    arguments:

    .. code-block:: python

        def train_loop_per_worker():
            ...

    .. code-block:: python

        def train_loop_per_worker(config: Dict):
            ...

    If ``train_loop_per_worker`` accepts an argument, then
    ``train_loop_config`` will be passed in as the argument. This is useful if you
    want to tune the values in ``train_loop_config`` as hyperparameters.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards that can then be accessed by ``session.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``session.get_dataset_shard(...)`` will return the the entire Dataset.

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray AIR session methods <air-session-ref>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging and
            # checkpoint data.
            session.report(...)

            # Returns dict of last saved checkpoint.
            session.get_checkpoint()

            # Returns the Ray Dataset shard for the given key.
            session.get_dataset_shard("my_dataset")

            # Returns the total number of workers executing training.
            session.get_world_size()

            # Returns the rank of this worker.
            session.get_world_rank()

            # Returns the rank of the worker on the current node.
            session.get_local_rank()

    Any returns from the ``train_loop_per_worker`` will be discarded and not
    used or persisted anywhere.

    **How do I use ``DataParallelTrainer`` or any of its subclasses?**

    Example:

    .. code-block:: python

        import ray
        from ray.air import session

        def train_loop_for_worker():
            dataset_shard_for_this_worker = session.get_dataset_shard("train")

            assert len(dataset_shard_for_this_worker) == 1

        train_dataset = ray.data.from_items([1, 2, 3])
        assert len(train_dataset) == 3
        trainer = DataParallelTrainer(
            ray.air.config.ScalingConfig(num_workers=3),
            datasets={"train": train_dataset},
        )
        result = trainer.fit()

    **How do I develop on top of ``DataParallelTrainer``?**

    In many cases, using DataParallelTrainer directly is sufficient to execute
    functions on multiple actors.

    However, you may want to subclass ``DataParallelTrainer`` and create a custom
    Trainer for the following 2 use cases:

      - **Use Case 1:** You want to do data parallel training, but want to have
        a predefined ``training_loop_per_worker``.

      - **Use Case 2:** You want to implement a custom
        :py:class:`~ray.train.backend.Backend` that automatically handles
        additional setup or teardown logic on each actor, so that the users of this
        new trainer do not have to implement this logic. For example, a
        ``TensorflowTrainer`` can be built on top of ``DataParallelTrainer``
        that automatically handles setting the proper environment variables for
        distributed Tensorflow on each actor.

    For 1, you can set a predefined training loop in __init__

    .. code-block:: python

        from ray.train.data_parallel_trainer import DataParallelTrainer

        class MyDataParallelTrainer(DataParallelTrainer):
            def __init__(self, *args, **kwargs):
                predefined_train_loop_per_worker = lambda: 1
                super().__init__(predefined_train_loop_per_worker, *args, **kwargs)


    For 2, you can implement the ``ray.train.Backend`` and ``ray.train.BackendConfig``
    interfaces.

    .. code-block:: python

        from dataclasses import dataclass
        from ray.train.backend import Backend, BackendConfig

        class MyBackend(Backend):
            def on_start(self, worker_group, backend_config):
                def set_env_var(env_var_value):
                    import os
                    os.environ["MY_ENV_VAR"] = env_var_value

                worker_group.execute(set_env_var, backend_config.env_var)

        @dataclass
        class MyBackendConfig(BackendConfig):
            env_var: str = "default_value"

            def backend_cls(self):
                return MyBackend

        class MyTrainer(DataParallelTrainer):
            def __init__(self, train_loop_per_worker, my_backend_config:
                MyBackendConfig, **kwargs):

                super().__init__(
                    train_loop_per_worker,
                    backend_config=my_backend_config, **kwargs)

    Args:
        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        backend_config: Configuration for setting up a Backend (e.g. Torch,
            Tensorflow, Horovod) on each worker to enable distributed
            communication. If no Backend should be set up, then set this to None.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest. This is merged with the
            default dataset config for the given trainer (`cls._dataset_config`).
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    _checkpoint_manager_cls: Type[
        TuneCheckpointManager
    ] = _DataParallelCheckpointManager

    # Exposed here for testing purposes. Should never need
    # to be overriden.
    _backend_executor_cls: Type[BackendExecutor] = BackendExecutor
    _training_iterator_cls: Type[TrainingIterator] = TrainingIterator

    _scaling_config_allowed_keys = BaseTrainer._scaling_config_allowed_keys + [
        "num_workers",
        "resources_per_worker",
        "use_gpu",
        "placement_strategy",
    ]

    _dataset_config = {
        TRAIN_DATASET_KEY: DatasetConfig(fit=True, split=True),
        WILDCARD_KEY: DatasetConfig(split=False),
    }

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not ray.is_initialized():
            ray.init()

        self._train_loop_per_worker = train_loop_per_worker
        self._train_loop_config = train_loop_config

        backend_config = (
            backend_config if backend_config is not None else BackendConfig()
        )
        self._backend_config = backend_config
        self._dataset_config = DatasetConfig.validated(
            DatasetConfig.merge(self._dataset_config, dataset_config), datasets
        )
        self._ingest_spec = DataParallelIngestSpec(
            dataset_config=self._dataset_config,
        )

        super(DataParallelTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _validate_attributes(self):
        super()._validate_attributes()

        self._validate_train_loop_per_worker(
            self._train_loop_per_worker, "train_loop_per_worker"
        )

    def preprocess_datasets(self) -> None:
        # Evaluate all datasets.
        self.datasets = {k: d() if callable(d) else d for k, d in self.datasets.items()}
        self.datasets = self._ingest_spec.preprocess_datasets(
            self.preprocessor, self.datasets
        )

    def _validate_train_loop_per_worker(
        self, train_loop_per_worker: Callable, fn_name: str
    ) -> None:
        num_params = len(inspect.signature(train_loop_per_worker).parameters)
        if num_params > 1:
            raise ValueError(
                f"{fn_name} should take in 0 or 1 arguments, "
                f"but it accepts {num_params} arguments instead."
            )

    @classmethod
    def _validate_scaling_config(cls, scaling_config: ScalingConfig) -> ScalingConfig:
        scaling_config = super(DataParallelTrainer, cls)._validate_scaling_config(
            scaling_config
        )

        # This validation happens after the scaling config is updated from
        # its specification in the Tuner `param_space`
        if not scaling_config.use_gpu and "GPU" in ray.available_resources():
            logger.info(
                "GPUs are detected in your Ray cluster, but GPU "
                "training is not enabled for this trainer. To enable "
                "GPU training, make sure to set `use_gpu` to True "
                "in your scaling config."
            )

        if scaling_config.num_workers is None:
            raise ValueError(
                "You must specify the 'num_workers' in `scaling_config` as either an "
                f"argument of `{cls.__name__}` or through the `param_space` of a "
                "`Tuner` (if performing hyperparameter tuning)."
            )

        if scaling_config.num_workers <= 0:
            raise ValueError(
                "'num_workers' in `scaling_config` must be a positive "
                f"integer. Received {scaling_config.num_workers}"
            )

        return scaling_config

    def _report(self, training_iterator: TrainingIterator) -> None:
        for results in training_iterator:
            # TODO(ml-team): add ability to report results from multiple workers.
            first_worker_results = results[0]
            tune.report(**first_worker_results)

    def training_loop(self) -> None:
        scaling_config = self._validate_scaling_config(self.scaling_config)

        train_loop_per_worker = construct_train_func(
            self._train_loop_per_worker,
            self._train_loop_config,
            fn_arg_name="train_loop_per_worker",
            discard_returns=True,
        )

        additional_resources_per_worker = scaling_config.additional_resources_per_worker

        trial_info = TrialInfo(
            name=session.get_trial_name(),
            id=session.get_trial_id(),
            resources=session.get_trial_resources(),
            logdir=session.get_trial_dir(),
            experiment_name=session.get_experiment_name(),
        )

        backend_executor = self._backend_executor_cls(
            backend_config=self._backend_config,
            trial_info=trial_info,
            num_workers=scaling_config.num_workers,
            num_cpus_per_worker=scaling_config.num_cpus_per_worker,
            num_gpus_per_worker=scaling_config.num_gpus_per_worker,
            additional_resources_per_worker=additional_resources_per_worker,
            max_retries=0,
        )

        checkpoint_manager = self._checkpoint_manager_cls(
            preprocessor=self.preprocessor
        )

        # Start the remote actors.
        backend_executor.start(initialization_hook=None)

        training_iterator = self._training_iterator_cls(
            backend_executor=backend_executor,
            backend_config=self._backend_config,
            train_func=train_loop_per_worker,
            dataset_spec=self._ingest_spec,
            checkpoint_manager=checkpoint_manager,
            checkpoint=self.resume_from_checkpoint,
            checkpoint_strategy=None,
        )

        self._report(training_iterator)

        # Shutdown workers.
        backend_executor.shutdown()

    def get_dataset_config(self) -> Dict[str, DatasetConfig]:
        """Return a copy of this Trainer's final dataset configs.

        Returns:
            The merged default + user-supplied dataset config.
        """
        return self._dataset_config.copy()

    @ensure_notebook_deps(
        ["tabulate", None],
        ["ipywidgets", "8"],
    )
    def _ipython_display_(self):
        from ipywidgets import HTML, VBox, Tab, Layout
        from IPython.display import display

        title = HTML(f"<h2>{self.__class__.__name__}</h2>")

        children = [
            self._datasets_repr_() if self.datasets else None,
            HTML(self._dataset_config_repr_html_()) if self._dataset_config else None,
            HTML(self._train_loop_config_repr_html_())
            if self._train_loop_config
            else None,
            HTML(self.scaling_config._repr_html_()) if self.scaling_config else None,
            HTML(self.run_config._repr_html_()) if self.run_config else None,
            HTML(self._backend_config._repr_html_()) if self._backend_config else None,
        ]

        tab = Tab(
            children,
            titles=[
                "Datasets",
                "Dataset Config",
                "Train Loop Config",
                "Scaling Config",
                "Run Config",
                "Backend Config",
            ],
        )
        display(VBox([title, tab], layout=Layout(width="100%")))

    def _train_loop_config_repr_html_(self) -> str:
        if self._train_loop_config:
            table_data = {}
            for k, v in self._train_loop_config.items():
                if isinstance(v, str) or str(v).isnumeric():
                    table_data[k] = v
                elif hasattr(v, "_repr_html_"):
                    table_data[k] = v._repr_html_()
                else:
                    table_data[k] = str(v)

            return Template("title_data.html.j2").render(
                title="Train Loop Config",
                data=Template("scrollableTable.html.j2").render(
                    table=tabulate(
                        table_data.items(),
                        headers=["Setting", "Value"],
                        showindex=False,
                        tablefmt="unsafehtml",
                    ),
                    max_height="none",
                ),
            )
        else:
            return ""

    def _dataset_config_repr_html_(self) -> str:
        content = []
        if self._dataset_config:
            for name, config in self._dataset_config.items():
                content.append(
                    config._repr_html_(title=f"DatasetConfig - <code>{name}</code>")
                )

        return Template("rendered_html_common.html.j2").render(content=content)

    @ensure_notebook_deps(["ipywidgets", "8"])
    def _datasets_repr_(self) -> str:
        from ipywidgets import HTML, VBox, Layout

        content = []
        if self.datasets:
            for name, config in self.datasets.items():
                tab = config._tab_repr_()
                if tab:
                    content.append(
                        HTML(
                            Template("title_data.html.j2").render(
                                title=f"Dataset - <code>{name}</code>", data=None
                            )
                        )
                    )
                    content.append(config._tab_repr_())

        return VBox(content, layout=Layout(width="100%"))


def _load_checkpoint_dict(
    checkpoint: Checkpoint, trainer_name: str
) -> Tuple[Any, Optional["Preprocessor"]]:
    """Load a Ray Train Checkpoint (dict based).

    This is a private API.

    Args:
        checkpoint: The checkpoint to load the weights and
            preprocessor from.
        trainer_name: Trainer class name to use in error
            message.

    Returns:
        The model or weights and AIR preprocessor contained within.
    """
    checkpoint_dict = checkpoint.to_dict()
    preprocessor = checkpoint_dict.get(PREPROCESSOR_KEY, None)
    if MODEL_KEY not in checkpoint_dict:
        raise RuntimeError(
            f"No item with key: {MODEL_KEY} is found in the "
            f"Checkpoint. Make sure this key exists when saving the "
            f"checkpoint in ``{trainer_name}``."
        )
    model = checkpoint_dict[MODEL_KEY]
    return model, preprocessor
