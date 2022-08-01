try:
    import jax  # noqa: F401
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Jax isn't installed. To install Jax on cpu or gpu, please checkout"
        " `https://github.com/google/jax#installation` for the details."
    )

from ray.train.jax.jax_trainer import JaxTrainer
from ray.train.jax.config import JaxConfig

__all__ = ["JaxConfig", "JaxTrainer"]
