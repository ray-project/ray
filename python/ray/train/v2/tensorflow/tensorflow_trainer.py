from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

from ray.train import Checkpoint, DataConfig
from ray.train.trainer import GenDataset
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.train.tensorflow import TensorflowConfig


@PublicAPI(stability="beta")
class TensorflowTrainer(DataParallelTrainer):
    """A Trainer for data parallel Tensorflow training.

    At a high level, this Trainer does the following:

    1. Launches multiple workers as defined by the ``scaling_config``.
    2. Sets up a distributed Tensorflow environment
       on these workers as defined by the ``tensorflow_config``.
    3. Ingests the input ``datasets`` based on the ``dataset_config``.
    4. Runs the input ``train_loop_per_worker(train_loop_config)``
       on all workers.

    For more details, see:

    * :ref:`Tensorflow Guide <train-tensorflow-overview>`

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray Train loop methods <train-loop-api>`.

    .. warning::
        Ray will not automatically set any environment variables or configuration
        related to local parallelism / threading
        :ref:`aside from "OMP_NUM_THREADS" <omp-num-thread-note>`.
        If you desire greater control over TensorFlow threading, use
        the ``tf.config.threading`` module (eg.
        ``tf.config.threading.set_inter_op_parallelism_threads(num_cpus)``)
        at the beginning of your ``train_loop_per_worker`` function.


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

    Example:

    .. testcode::

        import os
        import tempfile
        import tensorflow as tf

        import ray
        from ray import train
        from ray.train import Checkpoint, ScalingConfig
        from ray.train.tensorflow import TensorflowTrainer

        def build_model():
            # toy neural network : 1-layer
            return tf.keras.Sequential(
                [tf.keras.layers.Dense(
                    1, activation="linear", input_shape=(1,))]
            )

        def train_loop_per_worker(config):
            dataset_shard = train.get_dataset_shard("train")
            strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
            with strategy.scope():
                model = build_model()
                model.compile(
                    optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

            tf_dataset = dataset_shard.to_tf(
                feature_columns="x",
                label_columns="y",
                batch_size=1
            )
            for epoch in range(config["num_epochs"]):
                model.fit(tf_dataset)

                # Create checkpoint.
                checkpoint_dir = tempfile.mkdtemp()
                model.save_weights(
                    os.path.join(checkpoint_dir, "my_checkpoint")
                )
                checkpoint = Checkpoint.from_directory(checkpoint_dir)

                train.report(
                    {},
                    checkpoint=checkpoint,
                )

        train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
        trainer = TensorflowTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=3, use_gpu=True),
            datasets={"train": train_dataset},
            train_loop_config={"num_epochs": 2},
        )
        result = trainer.fit()

    .. testoutput::
        :options:+ELLIPSIS
        :hide:

        ...

    Args:
        train_loop_per_worker: The training function to execute on each worker.
            This function can either take in zero arguments or a single ``Dict``
            argument which is set by defining ``train_loop_config``.
            Within this function you can use any of the
            :ref:`Ray Train Loop utilities <train-loop-api>`.
        train_loop_config: A configuration ``Dict`` to pass in as an argument to
            ``train_loop_per_worker``.
            This is typically used for specifying hyperparameters. Passing large
            datasets via `train_loop_config` is not recommended and may introduce
            large overhead and unknown issues with serialization and deserialization.
        tensorflow_config: The configuration for setting up the Tensorflow
            Distributed backend. If set to None, a default configuration will be
            used in which GPU training uses NCCL and CPU training uses Gloo.
        scaling_config: The configuration for how to scale data parallel training.
            ``num_workers`` determines how many Python processes are used for training,
            and ``use_gpu`` determines whether or not each process should use GPUs.
            See :class:`~ray.train.ScalingConfig` for more info.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig` for more info.
        datasets: The Ray Datasets to ingest for training.
            Datasets are keyed by name (``{name: dataset}``).
            Each dataset can be accessed from within the ``train_loop_per_worker``
            by calling ``ray.train.get_dataset_shard(name)``.
            Sharding and additional configuration can be done by
            passing in a ``dataset_config``.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available via
            `ray.train.get_context().get_metadata()` and in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        tensorflow_config: Optional["TensorflowConfig"] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[DataConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        # TODO: [Deprecated]
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        from ray.train.tensorflow import TensorflowConfig

        super(TensorflowTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=tensorflow_config or TensorflowConfig(),
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )
