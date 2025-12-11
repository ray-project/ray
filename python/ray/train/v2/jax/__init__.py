from typing import TYPE_CHECKING

if TYPE_CHECKING:
    try:
        import jax  # noqa: F401
    except ModuleNotFoundError as exception:
        raise ModuleNotFoundError(
            "Jax isn't installed. To install Jax, please check"
            " `https://github.com/google/jax#installation` for the instructions."
        ) from exception

from ray.train.v2.jax.config import JaxConfig
from ray.train.v2.jax.jax_trainer import JaxTrainer

__all__ = ["JaxConfig", "JaxTrainer"]
