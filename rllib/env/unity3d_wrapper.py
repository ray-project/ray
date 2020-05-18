from gym.spaces import Box, Dict, Tuple
import numpy as np
from mlagents.envs.environment import UnityEnvironment

from ray.rllib.env.remote_vector_env import RemoteVectorEnv


class MLAgentsEnv(RemoteVectorEnv):
    """
    An Environment sitting behind a tcp connection and communicating through this adapter.
    Note: Communication between Unity and Python takes place over an open socket without authentication.
    Ensure that the network where training takes place is secure.
    """
    def __init__(self, num_envs, remote_env_batch_wait_ms, file_name=None,
                 worker_id=0, base_port=5005, seed=0,
                 docker_training=False, no_graphics=False, **kwargs):
        """
        Args:
            file_name (Optional[str]): Name of Unity environment binary.
            base_port (int): Port number to connect to Unity environment.
            #`worker_id` increments on top of this.
            worker_id (int): Number to add to `base_port`. Used for asynchronous agent scenarios.
            docker_training (bool): Informs this class, whether the process is being run within a container.
                Default: False.
            no_graphics (bool): Whether to run the Unity simulator in no-graphics mode. Default: False.
            timeout_wait (int): Time (in seconds) to wait for connection from environment.
            train_mode (bool): Whether to run in training mode, speeding up the simulation. Default: True.
        """
        # Factory function to create one UnityMLAgentsEnvironment.
        make_env = lambda: UnityEnvironment(
            file_name, worker_id, base_port, seed, docker_training, no_graphics
        )

        super().__init__(
            make_env=make_env, num_envs=num_envs,
            remote_env_batch_wait_ms=remote_env_batch_wait_ms,
            **kwargs
        )

        #self.mlagents_env =
        all_brain_info = self.mlagents_env.reset()
        # Get all possible information from AllBrainInfo.
        # TODO: Which scene do we pick?
        self.scene_key = next(iter(all_brain_info))
        first_brain_info = all_brain_info[self.scene_key]
        num_environments = len(first_brain_info.agents)

        state_space = {}
        if len(first_brain_info.vector_observations[0]) > 0:
            state_space["vector"] = get_space_from_op(first_brain_info.vector_observations[0])
            # TODO: This is a hack.
            if state_space["vector"].dtype == np.float64:
                state_space["vector"].dtype = np.float32
        if len(first_brain_info.visual_observations) > 0:
            state_space["visual"] = get_space_from_op(first_brain_info.visual_observations[0])
        if first_brain_info.text_observations[0]:
            state_space["text"] = get_space_from_op(first_brain_info.text_observations[0])

        if len(state_space) == 1:
            self.state_key = next(iter(state_space))
            state_space = state_space[self.state_key]
        else:
            self.state_key = None
            state_space = Dict(state_space)
        brain_params = next(iter(self.mlagents_env.brains.values()))
        if brain_params.vector_action_space_type == "discrete":
            highs = brain_params.vector_action_space_size
            # MultiDiscrete (Tuple(IntBox)).
            if any(h != highs[0] for h in highs):
                action_space = Tuple([IntBox(h) for h in highs])
            # Normal IntBox:
            else:
                action_space = IntBox(
                    low=np.zeros_like(highs, dtype=np.int32),
                    high=np.array(highs, dtype=np.int32),
                    shape=(len(highs),)
                )
        else:
            action_space = get_space_from_op(first_brain_info.action_masks[0])
        if action_space.dtype == np.float64:
            action_space.dtype = np.float32

        #super(MLAgentsEnv, self).__init__(
        #    num_environments=num_environments, state_space=state_space, action_space=action_space, **kwargs
        #)

        # Caches the last observation we made (after stepping or resetting).
        self.last_state = None

    def get_env(self):
        return self

    def reset(self, index=0):
        # Reset entire MLAgentsEnv iff global_done is True.
        if self.mlagents_env.global_done is True or self.last_state is None:
            self.reset_all()
        return self.last_state[index]

    def reset_all(self):
        all_brain_info = self.mlagents_env.reset()
        self.last_state = self._get_state_from_brain_info(all_brain_info)
        return self.last_state

    def step(self, actions, text_actions=None, **kwargs):
        # MLAgents Envs don't like tuple-actions.
        if isinstance(actions[0], tuple):
            actions = [list(a) for a in actions]
        all_brain_info = self.mlagents_env.step(
            # TODO: Only support vector actions for now.
            vector_action=actions, memory=None, text_action=text_actions, value=None
        )
        self.last_state = self._get_state_from_brain_info(all_brain_info)
        r = self._get_reward_from_brain_info(all_brain_info)
        t = self._get_terminal_from_brain_info(all_brain_info)
        return self.last_state, r, t, None

    def render(self):
        # TODO: If no_graphics is True, maybe user can render through this method manually?
        pass

    def terminate(self):
        self.mlagents_env.close()

    def terminate_all(self):
        return self.terminate()

    def __str__(self):
        return "MLAgentsEnv(port={}{})".format(
            self.mlagents_env.port, " [loaded]" if self.mlagents_env._loaded else ""
        )

    def _get_state_from_brain_info(self, all_brain_info):
        brain_info = all_brain_info[self.scene_key]
        if self.state_key is None:
            return {"vector": list(brain_info.vector_observations), "visual": list(brain_info.visual_observations),
                    "text": list(brain_info.text_observations)}
        elif self.state_key == "vector":
            return list(brain_info.vector_observations)
        elif self.state_key == "visual":
            return list(brain_info.visual_observations)
        elif self.state_key == "text":
            return list(brain_info.text_observations)

    def _get_reward_from_brain_info(self, all_brain_info):
        brain_info = all_brain_info[self.scene_key]
        return [np.array(r_, dtype=np.float32) for r_ in brain_info.rewards]

    def _get_terminal_from_brain_info(self, all_brain_info):
        brain_info = all_brain_info[self.scene_key]
        return brain_info.local_done
