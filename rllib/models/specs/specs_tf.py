from typing import Tuple, Any, Union

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.models.specs.specs_base import TensorSpecs

_, tf, tfv = try_import_tf()


@DeveloperAPI
class TFSpecs(TensorSpecs):
    def get_shape(self, tensor: tf.Tensor) -> Tuple[int]:
        return tuple(tensor.shape)

    def get_dtype(self, tensor: tf.Tensor) -> Any:
        return tensor.dtype

    def _full(self, shape: Tuple[int], fill_value: Union[float, int] = 0) -> tf.Tensor:
        if self.dtype:
            return tf.ones(shape, dtype=self.dtype) * fill_value
        return tf.fill(shape, fill_value)
