from typing import Any, Callable, Dict, Optional, Union

from ray.train import Checkpoint, DataConfig, RunConfig, ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.tensorflow.config import TensorflowConfig
from ray.train.trainer import GenDataset
from ray.util import PublicAPI


@PublicAPI(stability="beta")
class TensorflowTrainer(DataParallelTrainer):
    """A Trainer for data parallel Tensorflow training.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary TensorFlow process group already
    configured for distributed TensorFlow training.

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
    shards that can then be accessed by ``ray.train.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``ray.train.get_dataset_shard(...)`` will return the entire Dataset.

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

    To save a model to use for the ``TensorflowPredictor``, you must save it under the
    "model" kwarg in ``Checkpoint`` passed to ``train.report()``.

    Example:

    .. testcode::

        import os
        import tempfile
        import tensorflow as tf
        import numpy as np

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

        train_dataset = ray.data.from_items([{"x": np.array([x], dtype=np.float32), "y": x + 1} for x in range(32)])
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
        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        tensorflow_config: Configuration for setting up the TensorFlow backend.
            If set to None, use the default configuration. This replaces the
            ``backend_config`` arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        datasets: Any Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset.
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
        tensorflow_config: Optional[TensorflowConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[DataConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not tensorflow_config:
            tensorflow_config = TensorflowConfig()

        super(TensorflowTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=tensorflow_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )
