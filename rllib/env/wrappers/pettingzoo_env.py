from ray.rllib.env.multi_agent_env import MultiAgentEnv


class PettingZooEnv(MultiAgentEnv):
    """An interface to the PettingZoo MARL environment library.

    See: https://github.com/Farama-Foundation/PettingZoo

    Inherits from MultiAgentEnv and exposes a given AEC
    (actor-environment-cycle) game from the PettingZoo project via the
    MultiAgentEnv public API.

    Note that the wrapper has some important limitations:

    1. All agents have the same action_spaces and observation_spaces.
       Note: If, within your aec game, agents do not have homogeneous action /
       observation spaces, apply SuperSuit wrappers
       to apply padding functionality: https://github.com/Farama-Foundation/
       SuperSuit#built-in-multi-agent-only-functions
    2. Environments are positive sum games (-> Agents are expected to cooperate
       to maximize reward). This isn't a hard restriction, it just that
       standard algorithms aren't expected to work well in highly competitive
       games.

    Examples:
        >>> from pettingzoo.butterfly import prison_v3
        >>> env = PettingZooEnv(prison_v3.env())
        >>> obs = env.reset()
        >>> print(obs)
        # only returns the observation for the agent which should be stepping
        {
            'prisoner_0': array([[[0, 0, 0],
                [0, 0, 0],
                [0, 0, 0],
                ...,
                [0, 0, 0],
                [0, 0, 0],
                [0, 0, 0]]], dtype=uint8)
        }
        >>> obs, rewards, dones, infos = env.step({
        ...                 "prisoner_0": 1
        ...             })
        # only returns the observation, reward, info, etc, for
        # the agent who's turn is next.
        >>> print(obs)
        {
            'prisoner_1': array([[[0, 0, 0],
                [0, 0, 0],
                [0, 0, 0],
                ...,
                [0, 0, 0],
                [0, 0, 0],
                [0, 0, 0]]], dtype=uint8)
        }
        >>> print(rewards)
        {
            'prisoner_1': 0
        }
        >>> print(dones)
        {
            'prisoner_1': False, '__all__': False
        }
        >>> print(infos)
        {
            'prisoner_1': {'map_tuple': (1, 0)}
        }
    """

    def __init__(self, env):
        self.env = env
        env.reset()

        # Get first observation space, assuming all agents have equal space
        self.observation_space = self.env.observation_space(self.env.agents[0])

        # Get first action space, assuming all agents have equal space
        self.action_space = self.env.action_space(self.env.agents[0])

        assert all(self.env.observation_space(agent) == self.observation_space
                   for agent in self.env.agents), \
            "Observation spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_observations wrapper can help (useage: " \
            "`supersuit.aec_wrappers.pad_observations(env)`"

        assert all(self.env.action_space(agent) == self.action_space
                   for agent in self.env.agents), \
            "Action spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_action_space wrapper can help (usage: " \
            "`supersuit.aec_wrappers.pad_action_space(env)`"

    def reset(self):
        self.env.reset()
        return {
            self.env.agent_selection: self.env.observe(
                self.env.agent_selection)
        }

    def step(self, action):
        self.env.step(action[self.env.agent_selection])
        obs_d = {}
        rew_d = {}
        done_d = {}
        info_d = {}
        while self.env.agents:
            obs, rew, done, info = self.env.last()
            a = self.env.agent_selection
            obs_d[a] = obs
            rew_d[a] = rew
            done_d[a] = done
            info_d[a] = info
            if self.env.dones[self.env.agent_selection]:
                self.env.step(None)
            else:
                break

        all_done = not self.env.agents
        done_d["__all__"] = all_done

        return obs_d, rew_d, done_d, info_d

    def close(self):
        self.env.close()

    def seed(self, seed=None):
        self.env.seed(seed)

    def render(self, mode="human"):
        return self.env.render(mode)

    @property
    def get_sub_environments(self):
        return self.env.unwrapped


class ParallelPettingZooEnv(MultiAgentEnv):
    def __init__(self, env):
        self.par_env = env
        self.par_env.reset()

        # Get first observation space, assuming all agents have equal space
        self.observation_space = self.par_env.observation_space(
            self.par_env.agents[0])

        # Get first action space, assuming all agents have equal space
        self.action_space = self.par_env.action_space(self.par_env.agents[0])

        assert all(
            self.par_env.observation_space(agent) == self.observation_space
            for agent in self.par_env.agents), \
            "Observation spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_observations wrapper can help (useage: " \
            "`supersuit.aec_wrappers.pad_observations(env)`"

        assert all(self.par_env.action_space(agent) == self.action_space
                   for agent in self.par_env.agents), \
            "Action spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_action_space wrapper can help (useage: " \
            "`supersuit.aec_wrappers.pad_action_space(env)`"

    def reset(self):
        return self.par_env.reset()

    def step(self, action_dict):
        obss, rews, dones, infos = self.par_env.step(action_dict)
        dones["__all__"] = all(dones.values())
        return obss, rews, dones, infos

    def close(self):
        self.par_env.close()

    def seed(self, seed=None):
        self.par_env.seed(seed)

    def render(self, mode="human"):
        return self.par_env.render(mode)

    @property
    def unwrapped(self):
        return self.par_env.unwrapped
