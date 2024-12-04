from gymnasium import spaces
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv

# Map representation: Six rooms with entrances in between.
MAP = [
    "WWWWWWWWWWWWW",
    "W   W   W   W",
    "W       W   W",
    "W   W       W",
    "W WWWW WWWW W",
    "W   W   W   W",
    "W   W       W",
    "W       W  GW",
    "WWWWWWWWWWWWW"
]

# Room positions (top-left corner coordinates of each room)
#ROOMS = {
#    0: {(1, 1), (2, 1), (3, 1), (2, 1), (2, 2), (3, 2)},
#    1: (1, 5),
#    2: (1, 9),
#    3: (5, 1),
#    4: (5, 5),
#    5: (5, 9),
#}

# Room connections (doors)
DOORS = {
    (0, 1): [(2, 4)],  # Door between room 0 and room 1
    (1, 2): [(2, 8)],  # Door between room 1 and room 2
    (3, 4): [(6, 4)],  # Door between room 3 and room 4
    (4, 5): [(6, 8)],  # Door between room 4 and room 5
    (0, 3): [(4, 2)],  # Door between room 0 and room 3
    (1, 4): [(4, 6)],  # Door between room 1 and room 4
    (2, 5): [(4, 10)],  # Door between room 2 and room 5
}

# Number of steps allowed for a low-level policy to reach the given goal.
MAX_STEPS_LOW_LEVEL = 10


class SixRoomEnv(MultiAgentEnv):
    def __init__(self, config=None):
        super().__init__()

        # self.flat = config.get("flat", False)
        self.possible_agents = [
            "high_level_agent",
            "low_level_agent_0",
            "low_level_agent_1",
            "low_level_agent_2",
        ]# if not self.flat else ["agent"]

        # Define low-level agent observation and action spaces.
        low_level_observation_space = spaces.Box(
            low=0, high=1, shape=(len(MAP), len(MAP[0])), dtype=np.float32
        )
        # Primitive actions: up, down, left, right.
        low_level_action_space = spaces.Discrete(4)

        self.observation_spaces = {
            "high_level_agent": spaces.Discrete(6),
            "low_level_agent_0": low_level_observation_space,
            "low_level_agent_1": low_level_observation_space,
            "low_level_agent_2": low_level_observation_space,
        }
        self.action_spaces = {
            "high_level_agent": spaces.Tuple((
                # The new target room.
                spaces.Discrete(6),
                # Low-level policy that should get us to target room.
                spaces.Discrete(3)
            )),
            "low_level_agent_0": low_level_action_space,
            "low_level_agent_1": low_level_action_space,
            "low_level_agent_2": low_level_action_space,
        }

        #self.goal_room = 5

        # Initialize environment state.
        self.reset()

    def reset(self, *, seed=None, options=None):
        self._agent_pos = (1, 1)
        self._low_level_steps = 0
        self._high_level_action = None

        # Return high-level observation.
        return {
            "high_level_agent": self._current_room,
        }, {}

    def step(self, action_dict):
        rewards = {"high_level_agent": 0.0, "low_level_agent": 0.0}
        terminateds = {"__all__": False}
        truncateds = {"__all__": False}

        # High-level agent acted: Set next goal and next low-level policy to use.
        if "high_level_agent" in action_dict:
            self._high_level_action = action_dict["high_level_agent"]
            low_level_agent = f"low_level_agent_{self._high_level_action[1]}"
            self._low_level_steps = 0
            # Return next low-level observation for the now-active agent.
            return (
                {low_level_agent: self._get_low_level_obs()},
                rewards,
                terminateds,
                truncateds,
                {},
            )
        # Low-level agent made a move (sent primitive action).
        else:
            assert "low_level_agent" in action_dict
            target_room, low_level_policy = self._high_level_action

            low_level_action = action_dict["low_level_agent"]
            next_pos = self._get_next_pos(low_level_action)

            # Check if the move ends up in a wall. If so -> correct.
            if MAP[next_pos[0]][next_pos[1]] != "W":
                self._agent_pos = next_pos

            # Low-level agent has reached its target room:
            # - Hand back control to high-level agent.
            # - Reward low level agent and high-level agent.
            if

            # Check if the agent has reached the global goal state.
            if MAP[self._agent_pos[0]][self._agent_pos[1]] == "G":
                self._current_room = target_room
                rewards["low_level_agent"] = 1.0
                rewards["high_level_agent"] = 0.0
                terminateds["low_level_agent"] = True
                if self._current_room == self.goal_room:
                    rewards["high_level_agent"] += 10.0
                    terminateds["__all__"] = True

                return {
                    "high_level_agent": self._current_room,
                    "low_level_agent": self._get_low_level_obs()
                }, rewards, terminateds, truncateds, {}
            #
            else:
                # Increment low-level step counter
                self._low_level_steps += 1
                if self._low_level_steps >= MAX_STEPS_LOW_LEVEL:
                    terminateds["low_level_agent"] = True

                return {
                    "high_level_agent": self._current_room,
                    "low_level_agent": self._get_low_level_obs()
                }, rewards, terminateds, truncateds, {}

    @property
    def _current_room(self):
        top_rooms = self._agent_pos[1] < 4
        left_rooms = self._agent_pos[0] < 4
        center_rooms = not left_rooms and self._agent_pos[0] < 8
        if top_rooms:
            return 0 if left_rooms else 1 if center_rooms else 2
        else:
            return 3 if left_rooms else 4 if center_rooms else 5

    def _get_next_pos(self, action):
        x, y = self._agent_pos
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

    def _get_low_level_obs(self):
        obs = np.zeros((len(MAP), len(MAP[0])), dtype=np.float32)
        obs[self._agent_pos] = 1.0
        return obs
