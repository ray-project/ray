from typing import Tuple, Any, Union, Type

from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.framework import try_import_jax
from ray.rllib.models.specs.specs_base import TensorSpecs

jax, _ = try_import_jax()
jnp = None
if jax is not None:
    jnp = jax.numpy


@DeveloperAPI
class JAXSpecs(TensorSpecs):
    @override(TensorSpecs)
    def get_type(cls) -> Type:
        return jnp.ndarray

    @override(TensorSpecs)
    def get_shape(self, tensor: jnp.ndarray) -> Tuple[int]:
        return tuple(tensor.shape)

    @override(TensorSpecs)
    def get_dtype(self, tensor: jnp.ndarray) -> Any:
        return tensor.dtype

    @override(TensorSpecs)
    def _full(
        self, shape: Tuple[int], fill_value: Union[float, int] = 0
    ) -> jnp.ndarray:
        return jnp.full(shape, fill_value, dtype=self.dtype)
