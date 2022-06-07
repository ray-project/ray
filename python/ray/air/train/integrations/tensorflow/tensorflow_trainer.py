from typing import Callable, Optional, Dict, Tuple, Type, Union
import tensorflow as tf

from ray.train.tensorflow import TensorflowConfig
from ray.air.trainer import GenDataset
from ray.air.train.data_parallel_trainer import DataParallelTrainer, _load_checkpoint
from ray.air.config import ScalingConfig, RunConfig, DatasetConfig
from ray.air.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint
from ray.util import PublicAPI


@PublicAPI(stability="alpha")
class TensorflowTrainer(DataParallelTrainer):
    """A Trainer for data parallel Tensorflow training.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary TensorFlow process group already
    configured for distributed TensorFlow training.

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

    You can also use any of the :ref:`TensorFlow specific function utils
    <train-api-tensorflow-utils>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Turns off autosharding for a dataset.
            # You should use this if you are doing
            # `train.get_dataset_shard(...).to_tf(...)`
            # as the data will be already sharded.
            train.tensorflow.prepare_dataset_shard(...)

    To save a model to use for the ``TensorflowPredictor``, you must save it under the
    "model" kwarg in ``train.save_checkpoint()``.

    Example:

    .. code-block:: python

        import tensorflow as tf

        import ray
        from ray import train
        from ray.train.tensorflow import prepare_dataset_shard

        from ray.air.train.integrations.tensorflow import TensorflowTrainer

        input_size = 1

        def build_model():
            # toy neural network : 1-layer
            return tf.keras.Sequential(
                [tf.keras.layers.Dense(
                    1, activation="linear", input_shape=(input_size,))]
            )

        def train_loop_for_worker(config):
            dataset_shard = train.get_dataset_shard("train")
            strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
            with strategy.scope():
                model = build_model()
                model.compile(
                    optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

            for epoch in range(config["num_epochs"]):
                tf_dataset = prepare_dataset_shard(
                    dataset_shard.to_tf(
                        label_column="y",
                        output_signature=(
                            tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
                            tf.TensorSpec(shape=(None), dtype=tf.float32),
                        ),
                        batch_size=1,
                    )
                )
                model.fit(tf_dataset)
                train.save_checkpoint(
                    epoch=epoch, model=model.get_weights())

        train_dataset = ray.data.from_items(
            [{"x": x, "y": x + 1} for x in range(32)])
        trainer = TensorflowTrainer(scaling_config={"num_workers": 3},
            datasets={"train": train_dataset},
            train_loop_config={"num_epochs": 2})
        result = trainer.fit()


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
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ray.air.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        tensorflow_config: Optional[TensorflowConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
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
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )


def load_checkpoint(
    checkpoint: Checkpoint,
    model: Union[Callable[[], tf.keras.Model], Type[tf.keras.Model], tf.keras.Model],
) -> Tuple[tf.keras.Model, Optional[Preprocessor]]:
    """Load a Checkpoint from ``TensorflowTrainer``.

    Args:
        checkpoint: The checkpoint to load the model and
            preprocessor from. It is expected to be from the result of a
            ``TensorflowTrainer`` run.
        model: A callable that returns a TensorFlow Keras model
            to use, or an instantiated model.
            Model weights will be loaded from the checkpoint.

    Returns:
        The model with set weights and AIR preprocessor contained within.
    """
    model_weights, preprocessor = _load_checkpoint(checkpoint, "TensorflowTrainer")
    if isinstance(model, type) or callable(model):
        model = model()
    model.set_weights(model_weights)
    return model, preprocessor
