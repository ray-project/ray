from ray.rllib.env.multi_agent_env import MultiAgentEnv


class PettingZooEnv(MultiAgentEnv):
    """An interface to the PettingZoo MARL environment library.

    See: https://github.com/PettingZoo-Team/PettingZoo

    Inherits from MultiAgentEnv and exposes a given AEC
    (actor-environment-cycle) game from the PettingZoo project via the
    MultiAgentEnv public API.

    It reduces the class of AEC games to Partially Observable Markov (POM)
    games by imposing the following important restrictions onto an AEC
    environment:

    1. Each agent steps in order specified in agents list (unless they are
       done, in which case, they should be skipped).
    2. Agents act simultaneously (-> No hard-turn games like chess).
    3. All agents have the same action_spaces and observation_spaces.
       Note: If, within your aec game, agents do not have homogeneous action /
       observation spaces, apply SuperSuit wrappers
       to apply padding functionality: https://github.com/PettingZoo-Team/
       SuperSuit#built-in-multi-agent-only-functions
    4. Environments are positive sum games (-> Agents are expected to cooperate
       to maximize reward). This isn't a hard restriction, it just that
       standard algorithms aren't expected to work well in highly competitive
       games.

    Examples:
        >>> from pettingzoo.gamma import prison_v0
        >>> env = POMGameEnv(env_creator=prison_v0})
        >>> obs = env.reset()
        >>> print(obs)
            {
                "0": [110, 119],
                "1": [105, 102],
                "2": [99, 95],
            }
        >>> obs, rewards, dones, infos = env.step(
            action_dict={
                "0": 1, "1": 0, "2": 2,
            })
        >>> print(rewards)
            {
                "0": 0,
                "1": 1,
                "2": 0,
            }
        >>> print(dones)
            {
                "0": False,    # agent 0 is still running
                "1": True,     # agent 1 is done
                "__all__": False,  # the env is not done
            }
        >>> print(infos)
            {
                "0": {},  # info for agent 0
                "1": {},  # info for agent 1
            }
    """

    def __init__(self, env):
        """
        Parameters:
        -----------
        env:  AECenv object.
        """
        self.aec_env = env

        # agent idx list
        self.agents = self.aec_env.agents

        # Get dictionaries of obs_spaces and act_spaces
        self.observation_spaces = self.aec_env.observation_spaces
        self.action_spaces = self.aec_env.action_spaces

        # Get first observation space, assuming all agents have equal space
        self.observation_space = self.observation_spaces[self.agents[0]]

        # Get first action space, assuming all agents have equal space
        self.action_space = self.action_spaces[self.agents[0]]

        assert all(obs_space == self.observation_space
                   for obs_space
                   in self.aec_env.observation_spaces.values()), \
            "Observation spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_observations wrapper can help (useage: " \
            "`supersuit.aec_wrappers.pad_observations(env)`"

        assert all(act_space == self.action_space
                   for act_space in self.aec_env.action_spaces.values()), \
            "Action spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_action_space wrapper can help (useage: " \
            "`supersuit.aec_wrappers.pad_action_space(env)`"

        self.rewards = {}
        self.dones = {}
        self.obs = {}
        self.infos = {}

        _ = self.reset()

    def _init_dicts(self):
        # initialize with zero
        self.rewards = dict(zip(self.agents, [0 for _ in self.agents]))
        # initialize with False
        self.dones = dict(zip(self.agents, [False for _ in self.agents]))
        self.dones["__all__"] = False

        # initialize with None info object
        self.infos = dict(zip(self.agents, [{} for _ in self.agents]))

        # initialize empty observations
        self.obs = dict(zip(self.agents, [None for _ in self.agents]))

    def reset(self):
        """
        Resets the env and returns observations from ready agents.

        Returns:
            obs (dict): New observations for each ready agent.
        """
        # 1. Reset environment; agent pointer points to first agent.
        self.aec_env.reset()

        # 2. Copy agents from environment
        self.agents = self.aec_env.agents

        # 3. Reset dictionaries
        self._init_dicts()

        # 4. Get initial observations
        for agent in self.agents:

            # For each agent get initial observations
            self.obs[agent] = self.aec_env.observe(agent)

        return self.obs

    def step(self, action_dict):
        """
        Executes input actions from RL agents and returns observations from
        environment agents.

        The returns are dicts mapping from agent_id strings to values. The
        number of agents in the env can vary over time.

        Returns
        -------
            obs (dict): New observations for each ready agent.
            rewards (dict): Reward values for each ready agent. If the
                episode is just started, the value will be None.
            dones (dict): Done values for each ready agent. The special key
                "__all__" (required) is used to indicate env termination.
            infos (dict): Optional info values for each agent id.
        """
        stepped_agents = set()
        while (self.aec_env.agent_selection not in stepped_agents
               and self.aec_env.dones[self.aec_env.agent_selection]):
            agent = self.aec_env.agent_selection
            self.aec_env.step(None)
            stepped_agents.add(agent)
        stepped_agents = set()
        # print(action_dict)
        while (self.aec_env.agent_selection not in stepped_agents):
            agent = self.aec_env.agent_selection
            assert agent in action_dict or self.aec_env.dones[agent], \
                "Live environment agent is not in actions dictionary"
            self.aec_env.step(action_dict[agent])
            stepped_agents.add(agent)
        # print(self.aec_env.dones)
        # print(stepped_agents)
        assert all(agent in stepped_agents or self.aec_env.dones[agent]
                   for agent in action_dict), \
            "environment has a nontrivial ordering, and cannot be used with"\
            " the POMGameEnv wrapper"

        self.obs = {}
        self.rewards = {}
        self.dones = {}
        self.infos = {}

        # update self.agents
        self.agents = list(action_dict.keys())

        for agent in self.agents:
            self.obs[agent] = self.aec_env.observe(agent)
            self.dones[agent] = self.aec_env.dones[agent]
            self.rewards[agent] = self.aec_env.rewards[agent]
            self.infos[agent] = self.aec_env.infos[agent]

        self.dones["__all__"] = all(self.aec_env.dones.values())

        return self.obs, self.rewards, self.dones, self.infos

    def render(self, mode="human"):
        return self.aec_env.render(mode=mode)

    def close(self):
        self.aec_env.close()

    def seed(self, seed=None):
        self.aec_env.seed(seed)

    def with_agent_groups(self, groups, obs_space=None, act_space=None):
        raise NotImplementedError


class ParallelPettingZooEnv(MultiAgentEnv):
    def __init__(self, env):
        self.par_env = env
        # agent idx list
        self.agents = self.par_env.agents

        # Get dictionaries of obs_spaces and act_spaces
        self.observation_spaces = self.par_env.observation_spaces
        self.action_spaces = self.par_env.action_spaces

        # Get first observation space, assuming all agents have equal space
        self.observation_space = self.observation_spaces[self.agents[0]]

        # Get first action space, assuming all agents have equal space
        self.action_space = self.action_spaces[self.agents[0]]

        assert all(obs_space == self.observation_space
                   for obs_space
                   in self.par_env.observation_spaces.values()), \
            "Observation spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_observations wrapper can help (useage: " \
            "`supersuit.aec_wrappers.pad_observations(env)`"

        assert all(act_space == self.action_space
                   for act_space in self.par_env.action_spaces.values()), \
            "Action spaces for all agents must be identical. Perhaps " \
            "SuperSuit's pad_action_space wrapper can help (useage: " \
            "`supersuit.aec_wrappers.pad_action_space(env)`"

        self.reset()

    def reset(self):
        return self.par_env.reset()

    def step(self, action_dict):
        aobs, arew, adones, ainfo = self.par_env.step(action_dict)
        obss = {}
        rews = {}
        dones = {}
        infos = {}
        for agent in action_dict:
            obss[agent] = aobs[agent]
            rews[agent] = arew[agent]
            dones[agent] = adones[agent]
            infos[agent] = ainfo[agent]
        dones["__all__"] = all(adones.values())
        return obss, rews, dones, infos

    def close(self):
        self.par_env.close()

    def seed(self, seed=None):
        self.par_env.seed(seed)

    def render(self, mode="human"):
        return self.par_env.render(mode)
