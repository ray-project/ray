from typing import TYPE_CHECKING

if TYPE_CHECKING:
    try:
        import jax  # noqa: F401
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "Jax isn't installed. To install Jax, please check"
            " `https://github.com/google/jax#installation` for the instructions."
        ) from e

from ray.train.jax.jax_trainer import JaxTrainer
from ray.train.jax.config import JaxConfig

__all__ = ["JaxConfig", "JaxTrainer"]
