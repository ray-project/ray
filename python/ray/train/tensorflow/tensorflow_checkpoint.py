import os
from typing import TYPE_CHECKING, Callable, Optional, Tuple, Type, Union

from enum import Enum
from os import path
import tensorflow as tf
from tensorflow import keras
import warnings

from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint_dict, _load_checkpoint_dir
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import ray
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class TensorflowCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with TensorFlow-specific
    functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``TensorflowCheckpoint.from_checkpoint(ckpt)``.
    """

    class Flavor(Enum):
        # Various flavors with which TensorflowCheckpoint is generated.
        # This is necessary metadata to decide how to load model from a checkpoint.
        MODEL_WEIGHTS = 1
        SAVED_MODEL = 2
        H5 = 3

    def __init__(self,
        local_path: Optional[str] = None,
        data_dict: Optional[dict] = None,
        uri: Optional[str] = None,
        obj_ref: Optional["ray.ObjectRef"] = None
    ):
        super().__init__(local_path, data_dict, uri, obj_ref)
        self._flavor = None

    @classmethod
    def from_model(
        cls,
        model: keras.Model,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TensorflowCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a Keras
        model.

        Args:
            model: The Keras model, whose weights are stored in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :py:class:`TensorflowCheckpoint` containing the specified model.

        Examples:
            >>> from ray.train.tensorflow import TensorflowCheckpoint
            >>> import tensorflow as tf
            >>>
            >>> model = tf.keras.applications.resnet.ResNet101()  # doctest: +SKIP
            >>> checkpoint = TensorflowCheckpoint.from_model(model)  # doctest: +SKIP
        """
        checkpoint = cls.from_dict(
            {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model.get_weights()}
        )
        checkpoint._flavor = cls.Flavor.MODEL_WEIGHTS
        return checkpoint

    @classmethod
    def from_h5(
        cls, file_path: str, *, preprocessor: Optional["Preprocessor"] = None
    ) -> "TensorflowCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a Keras
        model - from SavedModel format.

        The path must maintain valid even after this function returns.

        Args:
            file_path: The same path as used by ``model.save(path)``.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :py:class:`TensorflowCheckpoint` converted from h5 format.

        Examples:

        .. code-block: python

            import tensorflow as tf

            import ray
            from ray.train.batch_predictor import BatchPredictor
            from ray.train.tensorflow import (
                TensorflowCheckpoint, TensorflowTrainer, TensorflowPredictor
            )
            from ray.air import session
            from ray.air.config import RunConfig, ScalingConfig

            def train_func():
                model = tf.keras.Sequential(
                    [
                        tf.keras.layers.InputLayer(input_shape=()),
                        tf.keras.layers.Flatten(),
                        tf.keras.layers.Dense(10),
                        tf.keras.layers.Dense(1),
                    ]
                )
                model.save("my_model.h5")
                checkpoint = TensorflowCheckpoint.from_h5("my_model.h5")
                session.report({"my_metric": 1}, checkpoint=checkpoint)

            trainer = TensorflowTrainer(
                train_loop_per_worker=train_func,
                scaling_config=ScalingConfig(num_workers=2))

            result_checkpoint = trainer.fit().checkpoint

            batch_predictor = BatchPredictor.from_checkpoint(
                result_checkpoint, TensorflowPredictor)
            batch_predictor.predict(ray.data.range(3))
        """
        assert path.isfile(file_path) and file_path.endswith(
            ".h5"
        ), "Please supply a h5 file path to `TensorflowCheckpoint.from_h5()`."
        dir_path = path.dirname(os.path.abspath(file_path))
        if preprocessor:
            save_preprocessor_to_dir(preprocessor, dir_path)
        checkpoint = cls.from_directory(dir_path)
        checkpoint._flavor = cls.Flavor.H5
        return checkpoint

    @classmethod
    def from_saved_model(
        cls, dir_path: str, *, preprocessor: Optional["Preprocessor"] = None
    ) -> "TensorflowCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a Keras
        model - from SavedModel format.

        The path must maintain valid even after this function returns.

        Args:
            path: The same path as used by ``model.save(path)``.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :py:class:`TensorflowCheckpoint` converted from SavedModel format.

        Examples:

        .. code-block: python

            import tensorflow as tf

            from ray.train.batch_predictor import BatchPredictor
            from ray.train.tensorflow import TensorflowCheckpoint, TensorflowTrainer
            from ray.air import session
            from ray.air.config import RunConfig, ScalingConfig

            def train_fn():
                model = tf.keras.Sequential(
                    [
                        tf.keras.layers.InputLayer(input_shape=()),
                        tf.keras.layers.Flatten(),
                        tf.keras.layers.Dense(10),
                        tf.keras.layers.Dense(1),
                    ]
                )
                model.save("my_model")
                checkpoint = TensorflowCheckpoint.from_saved_model("my_model")
                session.report({"my_metric": 1}, checkpoint=checkpoint)

            trainer = TensorflowTrainer(
                train_loop_per_worker=train_func,
                scaling_config=ScalingConfig(num_workers=2))

            result_checkpoint = trainer.fit().checkpoint

            batch_predictor = BatchPredictor.from_checkpoint(
                result_checkpoint, TensorflowPredictor)
            batch_predictor.predict(ray.data.range(3))
        """
        if preprocessor:
            save_preprocessor_to_dir(preprocessor, dir_path)
        checkpoint = cls.from_directory(dir_path)
        checkpoint._flavor = cls.Flavor.SAVED_MODEL
        return checkpoint

    def get_model_and_preprocessor(
        self,
        model_difinition: Optional[
            Union[Callable[[], tf.keras.Model], Type[tf.keras.Model]]
        ],
    ) -> Tuple[tf.keras.Model, Optional["Preprocessor"]]:
        """Retrieve the model and preprocessor stored in this checkpoint.

        `model` arg is expected if and only if
        the checkpoint's flavor is `MODEL_WEIGHTS`.

        """
        if self._flavor is self.Flavor.MODEL_WEIGHTS:
            assert model_difinition, (
                "Expecting input of `model` argument when checkpoint's flavor "
                "is `MODEL_WEIGHTS`."
            )
            model_weights, preprocessor = _load_checkpoint_dict(
                self, "TensorflowTrainer"
            )
            model = model_difinition()
            model.set_weights(model_weights)
            return model, preprocessor
        else:
            if model_difinition:
                warnings.warn(
                    "Ignoring `model argument` when checkpoint's"
                    " flavor is not `MODEL_WEIGHTS`."
                )
            get_model = None

            if self._flavor == self.Flavor.H5:

                def get_model(path: str):
                    # Walk the directory to find an .h5 path
                    # There should be one and only one that is .h5.
                    h5_path = None
                    files = os.listdir(path)
                    for file in files:
                        if file.endswith(".h5"):
                            assert not h5_path, (
                                "There should be one and only one .h5 file "
                                "to load model from within the directory."
                            )
                            h5_path = file
                    return keras.models.load_model(h5_path)

            else:

                def get_model(path: str):
                    return keras.models.load_model(path)

            model, preprocessor = _load_checkpoint_dir(self, get_model)
            return model, preprocessor
