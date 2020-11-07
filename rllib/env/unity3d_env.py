from gym.spaces import Box, MultiDiscrete, Tuple as TupleSpace
import logging
import numpy as np
import random
import time
from typing import Callable, Optional, Tuple

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import MultiAgentDict, PolicyID, AgentID

logger = logging.getLogger(__name__)


class Unity3DEnv(MultiAgentEnv):
    """A MultiAgentEnv representing a single Unity3D game instance.

    For an example on how to use this Env with a running Unity3D editor
    or with a compiled game, see:
    `rllib/examples/unity3d_env_local.py`
    For an example on how to use it inside a Unity game client, which
    connects to an RLlib Policy server, see:
    `rllib/examples/serving/unity3d_[client|server].py`

    Supports all Unity3D (MLAgents) examples, multi- or single-agent and
    gets converted automatically into an ExternalMultiAgentEnv, when used
    inside an RLlib PolicyClient for cloud/distributed training of Unity games.
    """

    _BASE_PORT = 5004

    def __init__(self,
                 file_name: str = None,
                 port: Optional[int] = None,
                 seed: int = 0,
                 no_graphics: bool = False,
                 timeout_wait: int = 300,
                 episode_horizon: int = 1000):
        """Initializes a Unity3DEnv object.

        Args:
            file_name (Optional[str]): Name of the Unity game binary.
                If None, will assume a locally running Unity3D editor
                to be used, instead.
            port (Optional[int]): Port number to connect to Unity environment.
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

        if file_name is None:
            print(
                "No game binary provided, will use a running Unity editor "
                "instead.\nMake sure you are pressing the Play (|>) button in "
                "your editor to start.")

        import mlagents_envs
        from mlagents_envs.environment import UnityEnvironment

        # Try connecting to the Unity3D game instance. If a port is blocked
        while True:
            # Sleep for random time to allow for concurrent startup of many
            # environments (num_workers >> 1). Otherwise, would lead to port
            # conflicts sometimes.
            time.sleep(random.randint(1, 10))
            port_ = port or self._BASE_PORT
            self._BASE_PORT += 1
            try:
                self.unity_env = UnityEnvironment(
                    file_name=file_name,
                    worker_id=0,
                    base_port=port_,
                    seed=seed,
                    no_graphics=no_graphics,
                    timeout_wait=timeout_wait,
                )
                print("Created UnityEnvironment for port {}".format(port_))
            except mlagents_envs.exception.UnityWorkerInUseException:
                pass
            else:
                break

        # Reset entire env every this number of step calls.
        self.episode_horizon = episode_horizon
        # Keep track of how many times we have called `step` so far.
        self.episode_timesteps = 0

    @override(MultiAgentEnv)
    def step(
            self, action_dict: MultiAgentDict
    ) -> Tuple[MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict]:
        """Performs one multi-agent step through the game.

        Args:
            action_dict (dict): Multi-agent action dict with:
                keys=agent identifier consisting of
                [MLagents behavior name, e.g. "Goalie?team=1"] + "_" +
                [Agent index, a unique MLAgent-assigned index per single agent]

        Returns:
            tuple:
                - obs: Multi-agent observation dict.
                    Only those observations for which to get new actions are
                    returned.
                - rewards: Rewards dict matching `obs`.
                - dones: Done dict with only an __all__ multi-agent entry in
                    it. __all__=True, if episode is done for all agents.
                - infos: An (empty) info dict.
        """

        # Set only the required actions (from the DecisionSteps) in Unity3D.
        all_agents = []
        for behavior_name in self.unity_env.behavior_specs:
            for agent_id in self.unity_env.get_steps(behavior_name)[
                    0].agent_id_to_index.keys():
                key = behavior_name + "_{}".format(agent_id)
                all_agents.append(key)
                self.unity_env.set_action_for_agent(behavior_name, agent_id,
                                                    action_dict[key])
        # Do the step.
        self.unity_env.step()

        obs, rewards, dones, infos = self._get_step_results()

        # Global horizon reached? -> Return __all__ done=True, so user
        # can reset. Set all agents' individual `done` to True as well.
        self.episode_timesteps += 1
        if self.episode_timesteps > self.episode_horizon:
            return obs, rewards, dict({
                "__all__": True
            }, **{agent_id: True
                  for agent_id in all_agents}), infos

        return obs, rewards, dones, infos

    @override(MultiAgentEnv)
    def reset(self) -> MultiAgentDict:
        """Resets the entire Unity3D scene (a single multi-agent episode)."""
        self.episode_timesteps = 0
        self.unity_env.reset()
        obs, _, _, _ = self._get_step_results()
        return obs

    def _get_step_results(self):
        """Collects those agents' obs/rewards that have to act in next `step`.

        Returns:
            Tuple:
                obs: Multi-agent observation dict.
                    Only those observations for which to get new actions are
                    returned.
                rewards: Rewards dict matching `obs`.
                dones: Done dict with only an __all__ multi-agent entry in it.
                    __all__=True, if episode is done for all agents.
                infos: An (empty) info dict.
        """
        obs = {}
        rewards = {}
        infos = {}
        for behavior_name in self.unity_env.behavior_specs:
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
                # Only overwrite rewards (last reward in episode), b/c obs
                # here is the last obs (which doesn't matter anyways).
                # Unless key does not exist in obs.
                if key not in obs:
                    os = tuple(o[idx] for o in terminal_steps.obs)
                    obs[key] = os = os[0] if len(os) == 1 else os
                rewards[key] = terminal_steps.reward[idx]  # rewards vector

        # Only use dones if all agents are done, then we should do a reset.
        return obs, rewards, {"__all__": False}, infos

    @staticmethod
    def get_policy_configs_for_game(
            game_name: str) -> Tuple[dict, Callable[[AgentID], PolicyID]]:

        # The RLlib server must know about the Spaces that the Client will be
        # using inside Unity3D, up-front.
        obs_spaces = {
            # 3DBall.
            "3DBall": Box(float("-inf"), float("inf"), (8, )),
            # 3DBallHard.
            "3DBallHard": Box(float("-inf"), float("inf"), (45, )),
            # Pyramids.
            "Pyramids": TupleSpace([
                Box(float("-inf"), float("inf"), (56, )),
                Box(float("-inf"), float("inf"), (56, )),
                Box(float("-inf"), float("inf"), (56, )),
                Box(float("-inf"), float("inf"), (4, )),
            ]),
            # SoccerStrikersVsGoalie.
            "Goalie": Box(float("-inf"), float("inf"), (738, )),
            "Striker": TupleSpace([
                Box(float("-inf"), float("inf"), (231, )),
                Box(float("-inf"), float("inf"), (63, )),
            ]),
            # Tennis.
            "Tennis": Box(float("-inf"), float("inf"), (27, )),
            # VisualHallway.
            "VisualHallway": Box(float("-inf"), float("inf"), (84, 84, 3)),
            # Walker.
            "Walker": Box(float("-inf"), float("inf"), (212, )),
            # FoodCollector.
            "FoodCollector": TupleSpace([
                Box(float("-inf"), float("inf"), (49, )),
                Box(float("-inf"), float("inf"), (4, )),
            ]),
        }
        action_spaces = {
            # 3DBall.
            "3DBall": Box(
                float("-inf"), float("inf"), (2, ), dtype=np.float32),
            # 3DBallHard.
            "3DBallHard": Box(
                float("-inf"), float("inf"), (2, ), dtype=np.float32),
            # Pyramids.
            "Pyramids": MultiDiscrete([5]),
            # SoccerStrikersVsGoalie.
            "Goalie": MultiDiscrete([3, 3, 3]),
            "Striker": MultiDiscrete([3, 3, 3]),
            # Tennis.
            "Tennis": Box(float("-inf"), float("inf"), (3, )),
            # VisualHallway.
            "VisualHallway": MultiDiscrete([5]),
            # Walker.
            "Walker": Box(float("-inf"), float("inf"), (39, )),
            # FoodCollector.
            "FoodCollector": MultiDiscrete([3, 3, 3, 2]),
        }

        # Policies (Unity: "behaviors") and agent-to-policy mapping fns.
        if game_name == "SoccerStrikersVsGoalie":
            policies = {
                "Goalie": (None, obs_spaces["Goalie"], action_spaces["Goalie"],
                           {}),
                "Striker": (None, obs_spaces["Striker"],
                            action_spaces["Striker"], {}),
            }

            def policy_mapping_fn(agent_id):
                return "Striker" if "Striker" in agent_id else "Goalie"

        else:
            policies = {
                game_name: (None, obs_spaces[game_name],
                            action_spaces[game_name], {}),
            }

            def policy_mapping_fn(agent_id):
                return game_name

        return policies, policy_mapping_fn
