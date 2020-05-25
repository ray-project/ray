import mlagents_envs
from mlagents_envs.environment import UnityEnvironment

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import override


class Unity3DEnv(MultiAgentEnv):
    """A MultiAgentEnv representing a single Unity3D game instance.

    Supports all Unity3D (MLAgents) examples, multi- or single-agent and
    will be converted automatically into an ExternalMultiAgentEnv, when used
    inside an RLlib PolicyClient for cloud training of Unity3D games.
    """

    def __init__(self,
                 file_name=None,
                 worker_id=0,
                 base_port=5004,
                 seed=0,
                 no_graphics=False,
                 timeout_wait=60,
                 episode_horizon=1000):
        """Initializes a Unity3DEnv object.

        Args:
            file_name (Optional[str]): Name of the Unity game binary.
                If None, will assume a locally running Unity3D editor
                to be used, instead.
            worker_id (int): Number to add to `base_port`. Used when more than
                one Unity3DEnv (games) are running on the same machine. This
                will be determined automatically, if possible, so a value
                of 0 should always suffice here.
            base_port (int): Port number to connect to Unity environment.
                `worker_id` increments on top of this.
            seed (int): A random seed value to use for the Unity3D game.
            no_graphics (bool): Whether to run the Unity3D simulator in
                no-graphics mode. Default: False.
            timeout_wait (int): Time (in seconds) to wait for connection from
                the Unity3D instance.
            episode_horizon (int): A hard horizon to abide to. After at most
                this many steps (per-agent episode `step()` calls), the
                Unity3D game is reset and will start again (finishing the
                multi-agent episode that the game represents).
                Note: The game itself may contain its own episode length
                limits, which are always obeyed (on top of this value here).
        """

        super().__init__()

        # Try connecting to the Unity3D game instance. If a port
        while True:
            self.worker_id = worker_id
            try:
                self.unity_env = UnityEnvironment(
                    file_name=file_name,
                    worker_id=worker_id,
                    base_port=base_port,
                    seed=seed,
                    no_graphics=no_graphics,
                    timeout_wait=timeout_wait,
                )
            except mlagents_envs.exception.UnityWorkerInUseException as e:
                worker_id += 1
                # Hard limit.
                if worker_id > 100:
                    raise e
            else:
                break

        # Reset entire env every this number of step calls.
        self.episode_horizon = episode_horizon
        # Keep track of how many times we have called `step` so far.
        self.episode_timesteps = 0

    @override(MultiAgentEnv)
    def step(self, action_dict):
        """

        Args:
            action_dict (dict): Double keyed dict with:
                upper key (int): RLlib Episode (MLAgents: "agent") ID.
                lower key (str): RLlib Agent (MLAgents: "behavior") name.

        Returns:
            tuple:
                obs: Only those observations for which to get new actions.
                rewards: Rewards dicts matching the returned obs.
                dones: Done dicts matching the returned obs.
        """

        # Set only the required actions (from the DecisionSteps) in Unity3D.
        for behavior_name in self.unity_env.get_behavior_names():
            for agent_id in self.unity_env.get_steps(behavior_name)[
                    0].agent_id_to_index.keys():
                key = behavior_name + "_{}".format(agent_id)
                self.unity_env.set_action_for_agent(behavior_name, agent_id,
                                                    action_dict[key])
        # Do the step.
        self.unity_env.step()

        obs, rewards, dones, infos = self._get_step_results()

        # Global horizon reached? -> Return __all__ done=True, so user
        # can reset.
        self.episode_timesteps += 1
        if self.episode_timesteps > self.episode_horizon:
            return obs, rewards, {"__all__": True}, infos

        return obs, rewards, dones, infos

    @override(MultiAgentEnv)
    def reset(self):
        self.episode_timesteps = 0
        self.unity_env.reset()
        obs, _, _, _ = self._get_step_results()
        return obs

    def _get_step_results(self):
        obs = {}
        rewards = {}
        infos = {}
        for behavior_name in self.unity_env.get_behavior_names():
            decision_steps, terminal_steps = self.unity_env.get_steps(
                behavior_name)
            # Important: Only update those sub-envs that are currently
            # available within _env_state.
            # Loop through all envs ("agents") and fill in, whatever
            # information we have.
            for agent_id, idx in decision_steps.agent_id_to_index.items():
                key = behavior_name + "_{}".format(agent_id)
                os = tuple(o[idx] for o in decision_steps.obs)
                os = os[0] if len(os) == 1 else os
                obs[key] = os
                rewards[key] = decision_steps.reward[idx]  # rewards vector
            for agent_id, idx in terminal_steps.agent_id_to_index.items():
                key = behavior_name + "_{}".format(agent_id)
                # Only overwrite rewards (last reward in episode), b/c as obs
                # here is the last obs (which doesn't matter anyways).
                rewards[key] = terminal_steps.reward[idx]  # rewards vector

        # Only use dones if all agents are done, then we should do a reset.
        return obs, rewards, {"__all__": False}, infos
