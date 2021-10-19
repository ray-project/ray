from ray.rllib.env.wrappers.model_vector_env import model_vector_env as mve
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.model_vector_env.model_vector_env",
    new="ray.rllib.env.wrappers.model_vector_env.model_vector_env",
    error=False,
)

model_vector_env = mve
