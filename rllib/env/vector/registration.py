import copy
import gymnasium as gym
import logging

from enum import Enum
from typing import Any, Dict, Optional

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector.sync_vector_multi_agent_env import SyncVectorMultiAgentEnv
from ray.rllib.env.vector.vector_multi_agent_env import VectorMultiAgentEnv

logger = logging.getLogger(__file__)


class VectorizeMode(Enum):
    """All possible vectorization modes used in make_vec.

    Note, RLlib does not implement an custom entry point.
    """

    ASYNC = "async"
    SYNC = "sync"


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

    def create_single_env() -> MultiAgentEnv:
        single_env = gym.make(env_spec, **env_spec_kwargs.copy())

        return single_env

    # Check, the vectorization mode.
    if vectorization_mode == VectorizeMode.SYNC:
        # Create the synchronized vector environemnt.
        env = SyncVectorMultiAgentEnv(
            env_fns=(create_single_env for _ in range(num_envs)),
            **vector_kwargs,
        )
    # Other modes are not implemented, yet.
    else:
        print(
            f"For MultiAgentEnv only synchronous environment vectorization "
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

    # Check the autoreset mode.
    # TODO (simon): This is upcoming in `gymnasium` in the next version.
    # if "autoreset_mode" not in env.metadata:
    #     logger.warning(
    #         f"The `VectorMultiAgentEnv` ({env}) is missing AutoresetMode "
    #         f"metadata, metadata={env.metadata}."
    #     )
    # elif not isinstance(env.metadata["autoreset_mode"], AutoresetMode):
    #     logger.warning(
    #         f"The `VectorMultiAgentEnv` ({env}) metadata['autoreset_mode'] "
    #         "is not an instance of gymnasium.vector.AutoresetMode, "
    #         f"{type(env.metadata['autoreset_mode'])}."
    #     )

    # Return the `VectorMultiAgentEnv`.
    return env
