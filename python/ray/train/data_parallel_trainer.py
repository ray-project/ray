import copy
import inspect
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Type, Union
from ray._private.thirdparty.tabulate.tabulate import tabulate

import ray
from ray import air, tune
from ray.air._internal.checkpointing import add_preprocessor_to_checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig, CheckpointConfig
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY, LAZY_CHECKPOINT_MARKER_FILE
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.train import BackendConfig, Checkpoint, TrainingIterator
from ray.train._internal import session
from ray.train._internal.session import _TrainingResult, get_session
from ray.train._internal.backend_executor import BackendExecutor, TrialInfo
from ray.train._internal.checkpoint import TuneCheckpointManager
from ray.train._internal.data_config import DataConfig, _LegacyDataConfigWrapper
from ray.train._internal.storage import _use_storage_context
from ray.train._internal.utils import construct_train_func
from ray.train.constants import TRAIN_DATASET_KEY, WILDCARD_KEY
from ray.train.trainer import BaseTrainer, GenDataset
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.widgets import Template
from ray.widgets.util import repr_with_fallback

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
            run_dir=run_dir,
            checkpoint_strategy=checkpoint_strategy,
        )

    def _process_persistent_checkpoint(self, checkpoint: _TrackedCheckpoint):
        air_checkpoint: air.Checkpoint = checkpoint.dir_or_data
        checkpoint.dir_or_data = add_preprocessor_to_checkpoint(
            air_checkpoint, self.preprocessor
        )
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

    .. testcode::

        def train_loop_per_worker():
            ...

    .. testcode::

        def train_loop_per_worker(config: Dict):
            ...

    If ``train_loop_per_worker`` accepts an argument, then
    ``train_loop_config`` will be passed in as the argument. This is useful if you
    want to tune the values in ``train_loop_config`` as hyperparameters.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards that can then be accessed by ``train.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``train.get_dataset_shard(...)`` will return the the entire Dataset.

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray Train loop methods <train-loop-api>`.

    .. testcode::

        from ray import train

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging and
            # checkpoint data.
            train.report(...)

            # Returns dict of last saved checkpoint.
            train.get_checkpoint()

            # Returns the Dataset shard for the given key.
            train.get_dataset_shard("my_dataset")

            # Returns the total number of workers executing training.
            train.get_context().get_world_size()

            # Returns the rank of this worker.
            train.get_context().get_world_rank()

            # Returns the rank of the worker on the current node.
            train.get_context().get_local_rank()

    Any returns from the ``train_loop_per_worker`` will be discarded and not
    used or persisted anywhere.

    **How do I use DataParallelTrainer or any of its subclasses?**

    Example:

    .. testcode::

        import ray
        from ray import train
        from ray.train import ScalingConfig
        from ray.train.data_parallel_trainer import DataParallelTrainer

        def train_loop_for_worker():
            dataset_shard_for_this_worker = train.get_dataset_shard("train")

            # 3 items for 3 workers, each worker gets 1 item
            batches = list(dataset_shard_for_this_worker.iter_batches(batch_size=1))
            assert len(batches) == 1

        train_dataset = ray.data.from_items([1, 2, 3])
        assert train_dataset.count() == 3
        trainer = DataParallelTrainer(
            train_loop_for_worker,
            scaling_config=ScalingConfig(num_workers=3),
            datasets={"train": train_dataset},
        )
        result = trainer.fit()

    .. testoutput::
            :hide:

            ...

    **How do I develop on top of DataParallelTrainer?**

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

    .. testcode::

        from ray.train.data_parallel_trainer import DataParallelTrainer

        class MyDataParallelTrainer(DataParallelTrainer):
            def __init__(self, *args, **kwargs):
                predefined_train_loop_per_worker = lambda: 1
                super().__init__(predefined_train_loop_per_worker, *args, **kwargs)


    For 2, you can implement the ``ray.train.Backend`` and ``ray.train.BackendConfig``
    interfaces.

    .. testcode::

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
        datasets: Any Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        metadata: Dict that should be made available via
            `train.get_context().get_metadata()` and in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
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

    # For backwards compatibility with the legacy dataset config API.
    _dataset_config = None

    _fields_for_tuner_param_space = BaseTrainer._fields_for_tuner_param_space + [
        "train_loop_config"
    ]

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[DataConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        # Deprecated.
        preprocessor: Optional["Preprocessor"] = None,
    ):
        self._train_loop_per_worker = train_loop_per_worker
        self._train_loop_config = train_loop_config

        if isinstance(dataset_config, dict) or self._dataset_config or preprocessor:
            # Warn about deprecated cases (will raise error in future).
            if isinstance(dataset_config, dict):
                logger.warning(
                    "The dict form of `dataset_config` is deprecated. Use the "
                    "DataConfig class instead. Support for this will be dropped "
                    "in a future release."
                )
            # If using the new API, hard-disallow deprecated features.
            if isinstance(dataset_config, DataConfig):
                if self._dataset_config:
                    raise ValueError(
                        "The DataConfig class is not compatible with the "
                        "Trainer._dataset_config field. Remove `_dataset_config` "
                        "from your trainer subclass to use DataConfig."
                    )
                elif preprocessor:
                    raise ValueError(
                        "The DataConfig class is not compatible with the "
                        "Trainer preprocessor arg. Remove the `preprocessor` arg "
                        "to use DataConfig."
                    )
            if self._dataset_config is None:
                base_dataset_config = {
                    TRAIN_DATASET_KEY: DatasetConfig(fit=True, split=True),
                    WILDCARD_KEY: DatasetConfig(split=False),
                }
            else:
                base_dataset_config = self._dataset_config
            self._data_config = _LegacyDataConfigWrapper(
                base_dataset_config, dataset_config, datasets
            )
        elif isinstance(dataset_config, DataConfig):
            self._data_config = dataset_config
        elif dataset_config is None:
            self._data_config = DataConfig()
        else:
            raise ValueError(
                "`dataset_config` must be an instance of ray.train.DataConfig, "
                f"was: {dataset_config}"
            )

        backend_config = (
            backend_config if backend_config is not None else BackendConfig()
        )
        self._backend_config = backend_config

        super(DataParallelTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            metadata=metadata,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    @PublicAPI(stability="beta")
    @classmethod
    def restore(
        cls: Type["DataParallelTrainer"],
        path: str,
        train_loop_per_worker: Optional[
            Union[Callable[[], None], Callable[[Dict], None]]
        ] = None,
        train_loop_config: Optional[Dict] = None,
        **kwargs,
    ) -> "DataParallelTrainer":
        """Restores a DataParallelTrainer from a previously interrupted/failed run.

        Args:
            train_loop_per_worker: Optionally re-specified train loop function.
                This should be used to re-specify a function that is not
                restorable in a new Ray cluster (e.g., it holds onto outdated
                object references). This should be the same training loop
                that was passed to the original trainer constructor.
            train_loop_config: Optionally re-specified train config.
                This should similarly be used if the original `train_loop_config`
                contained outdated object references, and it should not be modified
                from what was originally passed in.

        See :meth:`BaseTrainer.restore() <ray.train.trainer.BaseTrainer.restore>`
        for descriptions of the other arguments.

        Returns:
            DataParallelTrainer: A restored instance of the `DataParallelTrainer`
            subclass that is calling this method.
        """
        return super(DataParallelTrainer, cls).restore(
            path=path,
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            **kwargs,
        )

    def _validate_attributes(self):
        super()._validate_attributes()

        self._validate_train_loop_per_worker(
            self._train_loop_per_worker, "train_loop_per_worker"
        )

    def preprocess_datasets(self) -> None:
        # Evaluate all datasets.
        self.datasets = {k: d() if callable(d) else d for k, d in self.datasets.items()}
        self.datasets = self._data_config._legacy_preprocessing(
            self.datasets, self.preprocessor
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
            first_worker_result = results[0]
            if _use_storage_context():
                assert all(isinstance(result, _TrainingResult) for result in results)

                tune_session = get_session()

                # Check if any workers reported a checkpoint.
                # If so, report a checkpoint pointing to the persisted location
                # to Tune for book-keeping.
                # NOTE: This removes the restriction for any individual worker
                # (ex: global rank 0 worker) from needing to report a checkpoint.
                # All workers reported a checkpoint to the same fs path, so there's
                # no need to report multiple checkpoints to Tune.
                worker_checkpoints = [
                    result.checkpoint
                    for result in results
                    if result.checkpoint is not None
                ]
                at_least_one_reported_checkpoint = len(worker_checkpoints) > 0
                # Make sure that all workers uploaded to the same location.
                assert all(
                    checkpoint.path == tune_session.storage.checkpoint_fs_path
                    for checkpoint in worker_checkpoints
                )

                checkpoint = (
                    Checkpoint(
                        filesystem=tune_session.storage.storage_filesystem,
                        # NOTE: The checkpoint index has not been incremented yet
                        # at this point, which is why `checkpoint_fs_path` points
                        # to the most recent checkpoint.
                        path=tune_session.storage.checkpoint_fs_path,
                    )
                    if at_least_one_reported_checkpoint
                    else None
                )
                tracked_training_result = _TrainingResult(
                    checkpoint=checkpoint,
                    metrics=first_worker_result.metrics,
                )

                logger.debug(
                    "Report (metrics, checkpoint) to the Tune session:\n"
                    f"  metrics={tracked_training_result.metrics}\n"
                    f"  checkpoint={tracked_training_result.checkpoint}"
                )

                # Report the metrics and checkpoint to Tune.
                tune_session._report_training_result(tracked_training_result)
            else:
                tune.report(**first_worker_result)

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
            driver_ip=ray.util.get_node_ip_address(),
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
            checkpoint_config=self.run_config.checkpoint_config,
        )

        def clear_lazy_checkpoint_marker():
            """Clears the stale lazy checkpointing marker on all worker nodes.

            After recovery, the trainer may be scheduled on another node.
            We should delete the marker files created earlier on each node to
            Avoid converting checkpoints to string paths.

            Please note that we need to clear the flag before the initialization
            of the checkpoint_manager, during which it will create a new lazy
            checkpointing marker file.
            """

            marker_file = Path(trial_info.logdir) / LAZY_CHECKPOINT_MARKER_FILE
            if marker_file.exists():
                logger.debug(
                    f"Deleting the stale lazy checkpoint marker file: {marker_file}."
                )
                # Multiple workers on the same node may delete this file at the
                # same time. Return if the marker file has been deleted.
                # TODO(ml-team): replace this try-except block with `missing_ok=True`
                # after we completely drop py37 support.
                try:
                    marker_file.unlink()
                except FileNotFoundError:
                    return

        # Start the remote actors.
        backend_executor.start(initialization_hook=clear_lazy_checkpoint_marker)

        checkpoint_manager = self._checkpoint_manager_cls(
            preprocessor=self.preprocessor
        )

        # Disable TrainingIterator's CheckpointManager from handling
        # checkpoints itself by setting num_to_keep to None.
        # This is important because otherwise Trainer's CheckpointManager
        # may delete a checkpoint prematurely, before the next checkpoint
        # has been fully handled by Tune.
        # TODO(jungong, justinvyu) : Trainer should not own a
        # CheckpointManager.
        checkpoint_strategy = copy.deepcopy(self.run_config.checkpoint_config)
        checkpoint_strategy.num_to_keep = None
        checkpoint_strategy.checkpoint_score_attribute = None

        training_iterator = self._training_iterator_cls(
            backend_executor=backend_executor,
            backend_config=self._backend_config,
            train_func=train_loop_per_worker,
            datasets=self.datasets,
            metadata=self.metadata,
            data_config=self._data_config,
            checkpoint_manager=checkpoint_manager,
            checkpoint=self.starting_checkpoint,
            checkpoint_strategy=checkpoint_strategy,
            storage_path=self.run_config.storage_path,
        )

        self._report(training_iterator)

        # Shutdown workers.
        backend_executor.shutdown()

    def get_dataset_config(self) -> DataConfig:
        """Returns a copy of this Trainer's final dataset configs.

        Returns:
            The merged default + user-supplied dataset config.
        """
        if isinstance(self._data_config, _LegacyDataConfigWrapper):
            return self._data_config._dataset_config
        else:
            return self._data_config

    @repr_with_fallback(["ipywidgets", "8"])
    def _repr_mimebundle_(self, **kwargs):
        """Returns a mimebundle with an ipywidget repr and a simple text repr.

        Depending on the frontend where the data is being displayed,
        different mimetypes will be used from this bundle.
        See https://ipython.readthedocs.io/en/stable/config/integrating.html
        for information about this method, and
        https://ipywidgets.readthedocs.io/en/latest/embedding.html
        for more information about the jupyter widget mimetype.

        Returns:
            A mimebundle containing an ipywidget repr and a simple text repr.
        """
        from ipywidgets import HTML, VBox, Tab, Layout

        title = HTML(f"<h2>{self.__class__.__name__}</h2>")

        children = []
        titles = []

        if self.datasets:
            children.append(self._datasets_repr_())
            titles.append("Datasets")

            children.append(HTML(self._data_config_repr_html_()))
            titles.append("Data Config")

        if self._train_loop_config:
            children.append(HTML(self._train_loop_config_repr_html_()))
            titles.append("Train Loop Config")

        if self.scaling_config:
            children.append(HTML(self.scaling_config._repr_html_()))
            titles.append("Scaling Config")

        if self.run_config:
            children.append(HTML(self.run_config._repr_html_()))
            titles.append("Run Config")

        if self._backend_config:
            children.append(HTML(self._backend_config._repr_html_()))
            titles.append("Backend Config")

        tab = Tab(children, titles=titles)
        widget = VBox([title, tab], layout=Layout(width="100%"))
        bundle = widget._repr_mimebundle_(**kwargs)
        bundle.update(
            {
                "text/plain": repr(self),
            }
        )
        return bundle

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

    def _data_config_repr_html_(self) -> str:
        # TODO make this rendering nicer.
        content = [str(self._data_config)]
        return Template("rendered_html_common.html.j2").render(content=content)

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
    checkpoint: air.Checkpoint, trainer_name: str
) -> Tuple[Any, Optional["Preprocessor"]]:
    """Loads a Ray Train Checkpoint (dict based).

    This is a private API.

    Args:
        checkpoint: The checkpoint to load the weights and
            preprocessor from.
        trainer_name: Trainer class name to use in error
            message.

    Returns:
        The model or weights and preprocessor contained within.
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
