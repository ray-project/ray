from typing import Tuple, Any, Union

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_jax
from ray.rllib.models.specs.specs_base import TensorSpecs

jax, _ = try_import_jax()
jnp = None
if jax is not None:
    jnp = jax.numpy


@DeveloperAPI
class JAXSpecs(TensorSpecs):
    def get_shape(self, tensor: jnp.ndarray) -> Tuple[int]:
        return tuple(tensor.shape)

    def get_dtype(self, tensor: jnp.ndarray) -> Any:
        return tensor.dtype

    def _full(
        self, shape: Tuple[int], fill_value: Union[float, int] = 0
    ) -> jnp.ndarray:
        return jnp.full(shape, fill_value, dtype=self.dtype)
