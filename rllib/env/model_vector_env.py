import logging
import numpy as np
from gym.spaces import Discrete
from ray.rllib.utils.annotations import override
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.utils.typing import EnvType

logger = logging.getLogger(__name__)


def model_vector_env(env: EnvType) -> BaseEnv:
    """Returns a VectorizedEnv wrapper around the given environment.

    To obtain worker configs, one can call get_global_worker().

    Args:
        env (EnvType): The input environment (of any supported environment
            type) to be convert to a _VectorizedModelGymEnv (wrapped as
            an RLlib BaseEnv).

    Returns:
        BaseEnv: The BaseEnv converted input `env`.
    """
    worker = get_global_worker()
    worker_index = worker.worker_index
    if worker_index:
        env = _VectorizedModelGymEnv(
            make_env=worker.make_env_fn,
            existing_envs=[env],
            num_envs=worker.num_envs,
            observation_space=env.observation_space,
            action_space=env.action_space,
        )
    return BaseEnv.to_base_env(
        env,
        make_env=worker.make_env_fn,
        num_envs=worker.num_envs,
        remote_envs=False,
        remote_env_batch_wait_ms=0)


class _VectorizedModelGymEnv(VectorEnv):
    """Vectorized Environment Wrapper for MB-MPO.

    Primary change is in the `vector_step` method, which calls the dynamics
    models for next_obs "calculation" (instead of the actual env). Also, the
    actual envs need to have two extra methods implemented: `reward(obs)` and
    (optionally) `done(obs)`. If `done` is not implemented, we will assume
    that episodes in the env do not terminate, ever.
    """

    def __init__(self,
                 make_env=None,
                 existing_envs=None,
                 num_envs=1,
                 *,
                 observation_space=None,
                 action_space=None,
                 env_config=None):
        self.make_env = make_env
        self.envs = existing_envs
        self.num_envs = num_envs
        while len(self.envs) < num_envs:
            self.envs.append(self.make_env(len(self.envs)))

        super().__init__(
            observation_space=observation_space
            or self.envs[0].observation_space,
            action_space=action_space or self.envs[0].action_space,
            num_envs=num_envs)
        worker = get_global_worker()
        self.model, self.device = worker.foreach_policy(
            lambda x, y: (x.dynamics_model, x.device))[0]

    @override(VectorEnv)
    def vector_reset(self):
        """Override parent to store actual env obs for upcoming predictions.
        """
        self.cur_obs = [e.reset() for e in self.envs]
        return self.cur_obs

    @override(VectorEnv)
    def reset_at(self, index):
        """Override parent to store actual env obs for upcoming predictions.
        """
        obs = self.envs[index].reset()
        self.cur_obs[index] = obs
        return obs

    @override(VectorEnv)
    def vector_step(self, actions):
        if self.cur_obs is None:
            raise ValueError("Need to reset env first")

        # If discrete, need to one-hot actions
        if isinstance(self.action_space, Discrete):
            act = np.array(actions)
            new_act = np.zeros((act.size, act.max() + 1))
            new_act[np.arange(act.size), act] = 1
            actions = new_act.astype("float32")

        # Batch the TD-model prediction.
        obs_batch = np.stack(self.cur_obs, axis=0)
        action_batch = np.stack(actions, axis=0)
        # Predict the next observation, given previous a) real obs
        # (after a reset), b) predicted obs (any other time).
        next_obs_batch = self.model.predict_model_batches(
            obs_batch, action_batch, device=self.device)
        next_obs_batch = np.clip(next_obs_batch, -1000, 1000)

        # Call env's reward function.
        # Note: Each actual env must implement one to output exact rewards.
        rew_batch = self.envs[0].reward(obs_batch, action_batch,
                                        next_obs_batch)

        # If env has a `done` method, use it.
        if hasattr(self.envs[0], "done"):
            dones_batch = self.envs[0].done(next_obs_batch)
        # Otherwise, assume the episode does not end.
        else:
            dones_batch = np.asarray([False for _ in range(self.num_envs)])

        info_batch = [{} for _ in range(self.num_envs)]

        self.cur_obs = next_obs_batch

        return list(next_obs_batch), list(rew_batch), list(
            dones_batch), info_batch

    @override(VectorEnv)
    def get_unwrapped(self):
        return self.envs
