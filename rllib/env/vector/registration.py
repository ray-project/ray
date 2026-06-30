import copy
import logging
from functools import partial
from typing import Any, Dict, Optional

import gymnasium as gym
from gymnasium.envs.registration import VectorizeMode

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector.sync_vector_multi_agent_env import SyncVectorMultiAgentEnv
from ray.rllib.env.vector.vector_multi_agent_env import VectorMultiAgentEnv

logger = logging.getLogger(__file__)


def make_vec(
    id: str,
    num_envs: int = 1,
    vectorization_mode: Optional[VectorizeMode] = None,
    vector_kwargs: Optional[Dict[str, Any]] = None,
    # TODO (simon): Add wrappers?
    **kwargs,
) -> VectorMultiAgentEnv:

    if vector_kwargs is None:
        vector_kwargs = {}

    if vectorization_mode is None:
        vectorization_mode = "sync"

    # Create an `gymnasium.envs.registration.EnvSpec` to properly
    # initialize the sub-environments.
    if isinstance(id, gym.envs.registration.EnvSpec):
        env_spec = id
    elif isinstance(id, str):
        env_spec = gym.envs.registration._find_spec(id)
    else:
        raise ValueError(f"Invalid id type: {type(id)}. Expected `str` or `EnvSpec`.")

    env_spec = copy.deepcopy(env_spec)
    env_spec_kwargs = env_spec.kwargs
    env_spec.kwargs = dict()

    num_envs = env_spec.kwargs.get("num_envs", num_envs)
    vectorization_mode = env_spec_kwargs.pop("vectorization_mode", vectorization_mode)
    vector_kwargs = env_spec_kwargs.pop("vector_kwargs", vector_kwargs)
    env_spec_kwargs.update(kwargs)

    # Specify the vectorization mode.
    if vectorization_mode is None:
        vectorization_mode = VectorizeMode.SYNC
    else:
        try:
            vectorization_mode = VectorizeMode(vectorization_mode)
        except ValueError:
            raise ValueError(
                f"Invalid vectorization mode: {vectorization_mode!r}, "
                f"valid modes: {[mode.value for mode in VectorizeMode]}."
            )
    assert isinstance(vectorization_mode, VectorizeMode)

    def create_single_env(index: int) -> MultiAgentEnv:
        single_env_kwargs = env_spec_kwargs.copy()
        # Pass the sub-environment's index within the vector env into the
        # entry point (through `gym.make`), such that each sub-environment
        # can be created with its correct `vector_index`.
        single_env_kwargs["vector_index"] = index
        single_env = gym.make(env_spec, **single_env_kwargs)

        return single_env

    # Check, the vectorization mode.
    if vectorization_mode == VectorizeMode.SYNC:
        # Create the synchronized vector environemnt.
        env = SyncVectorMultiAgentEnv(
            env_fns=(partial(create_single_env, index) for index in range(num_envs)),
            **vector_kwargs,
        )
    # Other modes are not implemented, yet.
    else:
        raise ValueError(
            "For `MultiAgentEnv` only synchronous environment vectorization "
            "is implemented. Use `gym_env_vectorize_mode='sync'`."
        )

    # Add all creation specifications to the environment.
    copied_id_spec = copy.deepcopy(env_spec)
    copied_id_spec.kwargs = env_spec_kwargs.copy()
    if num_envs != 1:
        copied_id_spec.kwargs["num_envs"] = num_envs
    copied_id_spec.kwargs["vectorization_mode"] = vectorization_mode.value
    if len(vector_kwargs) > 0:
        copied_id_spec.kwargs["vector_kwargs"] = vector_kwargs
    env.unwrapped.spec = copied_id_spec

    # Return the `VectorMultiAgentEnv`.
    return env
