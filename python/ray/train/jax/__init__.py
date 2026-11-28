# The JAX trainer only exists on Ray Train V2. Its implementation lives in
# `ray.train.v2.jax`, which stays importable as an alias of this module.
from ray.train.v2.jax.config import JaxConfig
from ray.train.v2.jax.jax_trainer import JaxTrainer

__all__ = ["JaxConfig", "JaxTrainer"]


# DO NOT ADD ANYTHING AFTER THIS LINE.
