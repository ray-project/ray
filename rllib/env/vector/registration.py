import gymnasium as gym
from typing import Any, Dict, Optional

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector.sync_vector_multi_agent_env import SyncVectorMultiAgentEnv
from ray.rllib.env.vector.vector_multi_agent_env import VectorMultiAgentEnv

def make_vec(
    id: str,
    num_envs: int = 1,
    vectorization_mode: Optional[str] = None,
    vector_kwargs: Optional[Dict[str, Any]] = None,
    # TODO (simon): Add wrappers?
) -> VectorMultiAgentEnv:
    
    if vector_kwargs is None:
        vector_kwargs = {}

    if vectorization_mode is None:
        vectorization_mode = "sync"
    
    def create_single_env() -> MultiAgentEnv:
        single_env = gym.make(id)

        return single_env

    if vectorization_mode == "sync":
        
        env = SyncVectorMultiAgentEnv(
            env_fns=(create_single_env for _ in range(num_envs)),
            **vector_kwargs,
        )

    return env
