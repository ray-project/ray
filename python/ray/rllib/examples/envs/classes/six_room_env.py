import gymnasium as gym

from ray.rllib.env.multi_agent_env import MultiAgentEnv

# Map representation: Always six rooms (as the name suggests) with doors in between.
MAPS = {
    "small": [
        "WWWWWWWWWWWWW",
        "W   W   W   W",
        "W       W   W",
        "W   W       W",
        "W WWWW WWWW W",
        "W   W   W   W",
        "W   W       W",
        "W       W  GW",
        "WWWWWWWWWWWWW",
    ],
    "medium": [
        "WWWWWWWWWWWWWWWWWWW",
        "W     W     W     W",
        "W           W     W",
        "W     W           W",
        "W WWWWWWW WWWWWWW W",
        "W     W     W     W",
        "W     W           W",
        "W           W    GW",
        "WWWWWWWWWWWWWWWWWWW",
    ],
    "large": [
        "WWWWWWWWWWWWWWWWWWWWWWWWW",
        "W       W       W       W",
        "W       W       W       W",
        "W               W       W",
        "W       W               W",
        "W       W       W       W",
        "WW WWWWWWWWW WWWWWWWWWW W",
        "W       W       W       W",
        "W       W               W",
        "W       W       W       W",
        "W               W       W",
        "W       W       W      GW",
        "WWWWWWWWWWWWWWWWWWWWWWWWW",
    ],
}


class SixRoomEnv(gym.Env):
    """A grid-world with six rooms (arranged as 2x3), which are connected by doors.

    The agent starts in the upper left room and has to reach a designated goal state
    in one of the rooms using primitive actions up, left, down, and right.

    The agent receives a small penalty of -0.01 on each step and a reward of +10.0 when
    reaching the goal state.
    """

    def __init__(self, config=None):
        super().__init__()

        # User can provide a custom map or a recognized map name (small, medium, large).
        self.map = config.get("custom_map", MAPS.get(config.get("map"), MAPS["small"]))
        self.time_limit = config.get("time_limit", 50)

        # Define observation space: Discrete, index fields.
        self.observation_space = gym.spaces.Discrete(len(self.map) * len(self.map[0]))
        # Primitive actions: up, down, left, right.
        self.action_space = gym.spaces.Discrete(4)

        # Initialize environment state.
        self.reset()

    def reset(self, *, seed=None, options=None):
        self._agent_pos = (1, 1)
        self._ts = 0
        # Return high-level observation.
        return self._agent_discrete_pos, {}

    def step(self, action):
        next_pos = _get_next_pos(action, self._agent_pos)

        self._ts += 1

        # Check if the move ends up in a wall. If so -> Ignore the move and stay
        # where we are right now.
        if self.map[next_pos[0]][next_pos[1]] != "W":
            self._agent_pos = next_pos

        # Check if the agent has reached the global goal state.
        if self.map[self._agent_pos[0]][self._agent_pos[1]] == "G":
            return self._agent_discrete_pos, 10.0, True, False, {}

        # Small step penalty.
        return self._agent_discrete_pos, -0.01, False, self._ts >= self.time_limit, {}

    @property
    def _agent_discrete_pos(self):
        x = self._agent_pos[0]
        y = self._agent_pos[1]
        # discrete position = row idx * columns + col idx
        return x * len(self.map[0]) + y


class HierarchicalSixRoomEnv(MultiAgentEnv):
    def __init__(self, config=None):
        super().__init__()

        # User can provide a custom map or a recognized map name (small, medium, large).
        self.map = config.get("custom_map", MAPS.get(config.get("map"), MAPS["small"]))
        self.max_steps_low_level = config.get("max_steps_low_level", 15)
        self.time_limit = config.get("time_limit", 50)
        self.num_low_level_agents = config.get("num_low_level_agents", 3)

        self.agents = self.possible_agents = ["high_level_agent"] + [
            f"low_level_agent_{i}" for i in range(self.num_low_level_agents)
        ]

        # Define basic observation space: Discrete, index fields.
        observation_space = gym.spaces.Discrete(len(self.map) * len(self.map[0]))
        # Low level agents always see where they are right now and what the target
        # state should be.
        low_level_observation_space = gym.spaces.Tuple(
            (observation_space, observation_space)
        )
        # Primitive actions: up, down, left, right.
        low_level_action_space = gym.spaces.Discrete(4)

        self.observation_spaces = {"high_level_agent": observation_space}
        self.observation_spaces.update(
            {
                f"low_level_agent_{i}": low_level_observation_space
                for i in range(self.num_low_level_agents)
            }
        )
        self.action_spaces = {
            "high_level_agent": gym.spaces.Tuple(
                (
                    # The new target observation.
                    observation_space,
                    # Low-level policy that should get us to the new target observation.
                    gym.spaces.Discrete(self.num_low_level_agents),
                )
            )
        }
        self.action_spaces.update(
            {
                f"low_level_agent_{i}": low_level_action_space
                for i in range(self.num_low_level_agents)
            }
        )

        # Initialize environment state.
        self.reset()

    def reset(self, *, seed=None, options=None):
        self._agent_pos = (1, 1)
        self._low_level_steps = 0
        self._high_level_action = None
        # Number of times the low-level agent reached the given target (by the high
        # level agent).
        self._num_targets_reached = 0

        self._ts = 0

        # Return high-level observation.
        return {
            "high_level_agent": self._agent_discrete_pos,
        }, {}

    def step(self, action_dict):
        self._ts += 1

        terminateds = {"__all__": self._ts >= self.time_limit}
        truncateds = {"__all__": False}

        # High-level agent acted: Set next goal and next low-level policy to use.
        # Note that the agent does not move in this case and stays at its current
        # location.
        if "high_level_agent" in action_dict:
            self._high_level_action = action_dict["high_level_agent"]
            low_level_agent = f"low_level_agent_{self._high_level_action[1]}"
            self._low_level_steps = 0
            # Return next low-level observation for the now-active agent.
            # We want this agent to act next.
            return (
                {
                    low_level_agent: (
                        self._agent_discrete_pos,  # current
                        self._high_level_action[0],  # target
                    )
                },
                # Penalty for a target state that's close to the current state.
                {
                    "high_level_agent": (
                        self.eucl_dist(
                            self._agent_discrete_pos,
                            self._high_level_action[0],
                            self.map,
                        )
                        / (len(self.map) ** 2 + len(self.map[0]) ** 2) ** 0.5
                    )
                    - 1.0,
                },
                terminateds,
                truncateds,
                {},
            )
        # Low-level agent made a move (primitive action).
        else:
            assert len(action_dict) == 1

            # Increment low-level step counter.
            self._low_level_steps += 1

            target_discrete_pos, low_level_agent = self._high_level_action
            low_level_agent = f"low_level_agent_{low_level_agent}"
            next_pos = _get_next_pos(action_dict[low_level_agent], self._agent_pos)

            # Check if the move ends up in a wall. If so -> Ignore the move and stay
            # where we are right now.
            if self.map[next_pos[0]][next_pos[1]] != "W":
                self._agent_pos = next_pos

            # Check if the agent has reached the global goal state.
            if self.map[self._agent_pos[0]][self._agent_pos[1]] == "G":
                rewards = {
                    "high_level_agent": 10.0,
                    # +1.0 if the goal position was also the target position for the
                    # low level agent.
                    low_level_agent: float(
                        self._agent_discrete_pos == target_discrete_pos
                    ),
                }
                terminateds["__all__"] = True
                return (
                    {"high_level_agent": self._agent_discrete_pos},
                    rewards,
                    terminateds,
                    truncateds,
                    {},
                )

            # Low-level agent has reached its target location (given by the high-level):
            # - Hand back control to high-level agent.
            # - Reward low level agent and high-level agent with small rewards.
            elif self._agent_discrete_pos == target_discrete_pos:
                self._num_targets_reached += 1
                rewards = {
                    "high_level_agent": 1.0,
                    low_level_agent: 1.0,
                }
                return (
                    {"high_level_agent": self._agent_discrete_pos},
                    rewards,
                    terminateds,
                    truncateds,
                    {},
                )

            # Low-level agent has not reached anything.
            else:
                # Small step penalty for low-level agent.
                rewards = {low_level_agent: -0.01}
                # Reached time budget -> Hand back control to high level agent.
                if self._low_level_steps >= self.max_steps_low_level:
                    rewards["high_level_agent"] = -0.01
                    return (
                        {"high_level_agent": self._agent_discrete_pos},
                        rewards,
                        terminateds,
                        truncateds,
                        {},
                    )
                else:
                    return (
                        {
                            low_level_agent: (
                                self._agent_discrete_pos,  # current
                                target_discrete_pos,  # target
                            ),
                        },
                        rewards,
                        terminateds,
                        truncateds,
                        {},
                    )

    @property
    def _agent_discrete_pos(self):
        x = self._agent_pos[0]
        y = self._agent_pos[1]
        # discrete position = row idx * columns + col idx
        return x * len(self.map[0]) + y

    @staticmethod
    def eucl_dist(pos1, pos2, map):
        x1, y1 = pos1 % len(map[0]), pos1 // len(map)
        x2, y2 = pos2 % len(map[0]), pos2 // len(map)
        return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5


def _get_next_pos(action, pos):
    x, y = pos
    # Up.
    if action == 0:
        return x - 1, y
    # Down.
    elif action == 1:
        return x + 1, y
    # Left.
    elif action == 2:
        return x, y - 1
    # Right.
    else:
        return x, y + 1
