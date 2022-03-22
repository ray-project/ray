import inspect
import logging
from pathlib import Path
from typing import Dict, Callable, Optional, Union

import ray
from ray import tune
from ray.ml.constants import TRAIN_DATASET_KEY, PREPROCESSOR_KEY
from ray.ml.trainer import Trainer
from ray.ml.config import ScalingConfig, RunConfig, ScalingConfigDataClass
from ray.ml.trainer import GenDataset
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.train import BackendConfig, TrainingIterator
from ray.train.backend import BackendExecutor
from ray.train.checkpoint import TuneCheckpointManager
from ray.train.utils import construct_train_func
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class DataParallelTrainer(Trainer):
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
    shards that can then be accessed by ``ray.train.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``ray.train.get_dataset_shard(...)`` will return the the entire Dataset.

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray Train function utils <train-api-func-utils>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging.
            train.report(...)

            # Checkpoints the provided args as restorable state.
            train.save_checkpoint(...)

            # Returns dict of last saved checkpoint.
            train.load_checkpoint()

            # Returns the Ray Dataset shard for the given key.
            train.get_dataset_shard("my_dataset")

            # Returns the total number of workers executing training.
            train.get_world_size()

            # Returns the rank of this worker.
            train.get_world_rank()

            # Returns the rank of the worker on the current node.
            train.get_local_rank()

    **How do I use ``DataParallelTrainer`` or any of its subclasses?**

    Example:

    .. code-block:: python

        import ray
        from ray import train

        def train_loop_for_worker():
            dataset_shard_for_this_worker = train.get_dataset_shard("train")

            assert len(dataset_shard_for_this_worker) == 1

        train_dataset = ray.data.from_items([1, 2, 3])
        assert len(train_dataset) == 3
        trainer = DataParallelTrainer(scaling_config={"num_workers": 3},
            datasets={"train": train_dataset})
        result = trainer.fit()

    **How do I develop on top of ``DataParallelTrainer``?**

    In many cases, using DataParallelTrainer directly is sufficient to execute
    functions on multiple actors.

    However, you may want to subclass ``DataParallelTrainer`` and create a custom
    Trainer for the following 2 use cases:

      - **Use Case 1:** You want to do data parallel training, but want to have
        a predefined ``training_loop_per_worker``.

      - **Use Case 2:** You want to implement a custom :ref:`Training backend
        <train-api-backend-interfaces>` that automatically handles
        additional setup or teardown logic on each actor, so that the users of this
        new trainer do not have to implement this logic. For example, a
        ``TensorflowTrainer`` can be built on top of ``DataParallelTrainer``
        that automatically handles setting the proper environment variables for
        distributed Tensorflow on each actor.

    For 1, you can set a predefined training loop in __init__

    .. code-block:: python

        from ray.ml.train.data_parallel_trainer import DataParallelTrainer

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

                super().__init__(train_loop_per_worker, my_backend_config, **kwargs)

    Args:
        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        backend_config: Configuration for setting up a Backend (e.g. Torch,
            Tensorflow, Horovod) on each worker to enable distributed
            communication. If no Backend should be set up, then set this to None.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        train_loop_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not ray.is_initialized():
            ray.init()

        self.train_loop_per_worker = train_loop_per_worker
        self.train_loop_config = train_loop_config

        super(DataParallelTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

        if (
            not self.scaling_config.get("use_gpu", False)
            and "GPU" in ray.available_resources()
        ):
            logger.info(
                "GPUs are detected in your Ray cluster, but GPU "
                "training is not enabled for this trainer. To enable "
                "GPU training, make sure to set `use_gpu` to True "
                "in your scaling config."
            )

        if "num_workers" not in self.scaling_config:
            raise ValueError("You must specify the 'num_workers' in scaling_config.")

        if self.scaling_config["num_workers"] <= 0:
            raise ValueError(
                "'num_workers' in `scaling_config` must be a positive "
                f"integer. Received {self.scaling_config['num_workers']}"
            )

        num_params = len(inspect.signature(self.train_loop_per_worker).parameters)
        if num_params > 1:
            raise ValueError(
                f"train_loop_per_worker should take in 0 or 1 arguments, "
                f"but it accepts {num_params} arguments instead."
            )

        backend_config = backend_config if backend_config else BackendConfig()
        self.backend_config = backend_config

    def training_loop(self) -> None:
        scaling_config_dataclass = ScalingConfigDataClass(**self.scaling_config)

        train_loop_per_worker = construct_train_func(
            self.train_loop_per_worker,
            self.train_loop_config,
            fn_arg_name="train_loop_per_worker",
        )

        additional_resources_per_worker = (
            scaling_config_dataclass.additional_resources_per_worker
        )

        backend_executor = BackendExecutor(
            backend_config=self.backend_config,
            num_workers=scaling_config_dataclass.num_workers,
            num_cpus_per_worker=scaling_config_dataclass.num_cpus_per_worker,
            num_gpus_per_worker=scaling_config_dataclass.num_gpus_per_worker,
            additional_resources_per_worker=additional_resources_per_worker,
            max_retries=0,
        )

        checkpoint_manager = _DataParallelCheckpointManager()
        checkpoint_manager.on_init(preprocessor=self.preprocessor)

        # Start the remote actors.
        backend_executor.start(initialization_hook=None)

        if self.resume_from_checkpoint:
            resume_checkpoint_dict = self.resume_from_checkpoint.to_dict()
        else:
            resume_checkpoint_dict = None

        # Tell Ray Train to only shard the train dataset and not the other datasets.
        # This is purely an implementation detail and users do not need to know about
        # this.
        # TODO(amog): Refactor this to remove hack and make this more modular.
        #  TrainingIterator should accept a generic custom_ingest_func that contains
        #  the logic for how to split the Datasets.
        updated_dataset_dict = {}
        for key, value in self.datasets.items():
            if key == TRAIN_DATASET_KEY:
                updated_dataset_dict[key] = value
            else:
                # Ray Train will strip out the added string before exposing to users.
                updated_dataset_dict[key + "_NO-SHARD"] = value

        # TODO(amog): Have TrainingIterator also accept a checkpoint ObjectRef instead
        #  of just a Dict.
        training_iterator = TrainingIterator(
            backend_executor=backend_executor,
            backend_config=self.backend_config,
            train_func=train_loop_per_worker,
            dataset=updated_dataset_dict if len(updated_dataset_dict) > 0 else None,
            checkpoint_manager=checkpoint_manager,
            checkpoint=resume_checkpoint_dict,
            checkpoint_strategy=None,
        )

        for results in training_iterator:
            # TODO(ml-team): add ability to report results from multiple workers.
            first_worker_results = results[0]

            tune.report(**first_worker_results)

        # Shutdown workers.
        backend_executor.shutdown()


# TODO(team-ml): Refactor checkpoint management along with Tune.
class _DataParallelCheckpointManager(TuneCheckpointManager):
    def on_init(self, preprocessor: Preprocessor):
        self.preprocessor = preprocessor
        super(_DataParallelCheckpointManager, self).on_init()

    def write_checkpoint(self, checkpoint: Dict):
        self.add_tune_checkpoint_id(checkpoint)

        # Add the preprocessor to the checkpoint.
        checkpoint[PREPROCESSOR_KEY] = self.preprocessor

        checkpoint_obj = Checkpoint.from_dict(checkpoint)
        # If inside a Tune Trainable, then checkpoint with Tune.
        with tune.checkpoint_dir(step=self._latest_checkpoint_id) as checkpoint_dir:
            checkpoint_obj.to_directory(path=checkpoint_dir)

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        raise NotImplementedError
