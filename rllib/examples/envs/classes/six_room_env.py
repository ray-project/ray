import numpy as np
from gymnasium import spaces

from ray.rllib.env.multi_agent_env import MultiAgentEnv

# Corrected map representation
MAP = [
    "WWWWWWWWWWWWW",
    "W   W   W   W",
    "W       W   W",
    "W   W       W",
    "W WWWW WWWW W",
    "W   W   W   W",
    "W   W       W",
    "W       W   W",
    "WWWWWWWWWWWWW"
]

# Room positions (top-left corner coordinates of each room)
ROOMS = {
    0: (1, 1),
    1: (1, 5),
    2: (1, 9),
    3: (5, 1),
    4: (5, 5),
    5: (5, 9),
}

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

MAX_STEPS_LOW_LEVEL = 50  # Number of steps allowed for a low-level policy


class SixRoomEnv(MultiAgentEnv):
    def __init__(self):
        super().__init__()

        # Define high-level agent observation and action spaces
        self.high_level_observation_space = spaces.Discrete(len(ROOMS))
        self.high_level_action_space = spaces.Tuple((
            # The new target room.
            spaces.Discrete(len(ROOMS)),
            # Low-level policy that should get us to target room.
            spaces.Discrete(3)
        ))

        # Define low-level agent observation and action spaces.
        self.low_level_observation_space = spaces.Box(
            low=0, high=1, shape=(len(MAP), len(MAP[0])), dtype=np.float32
        )
        # Primitive actions: up, down, left, right.
        self.low_level_action_space = spaces.Discrete(4)
        self.goal_room = 5

        # Initialize environment state
        self.reset()
        #self.agent_pos = ROOMS[0]  # Start in room 0
        #self.current_room = 0
        #self.low_level_steps = 0
        #self.high_level_action = None

    def reset(self, *, seed=None, options=None):
        self.agent_pos = ROOMS[0]
        self.current_room = 0
        self.low_level_steps = 0
        self.high_level_action = None

        # Return initial observations
        return {
            "high_level_agent": self.current_room,
            "low_level_agent": self._get_low_level_obs()
        }

    def _get_low_level_obs(self):
        obs = np.zeros((len(MAP), len(MAP[0])), dtype=np.float32)
        obs[self.agent_pos] = 1.0
        return obs

    def step(self, action_dict):
        rewards = {"high_level_agent": 0.0, "low_level_agent": 0.0}
        terminateds = {"__all__": False}
        truncateds = {"__all__": False}

        if "high_level_agent" in action_dict:
            self.high_level_action = action_dict["high_level_agent"]
            self.low_level_steps = 0
            terminateds["low_level_agent"] = False
            return {
                "high_level_agent": self.current_room,
                "low_level_agent": self._get_low_level_obs()
            }, rewards, terminateds, truncateds, {}

        if "low_level_agent" in action_dict:
            low_level_action = action_dict["low_level_agent"]
            next_pos = self._get_next_pos(low_level_action)

            # Check if the move is valid
            if MAP[next_pos[0]][next_pos[1]] != "W":
                self.agent_pos = next_pos

            # Check if the agent has reached the target room
            target_room, low_level_policy = self.high_level_action
            if self.agent_pos in DOORS.get((self.current_room, target_room), []):
                self.current_room = target_room
                rewards["low_level_agent"] = 1.0
                rewards["high_level_agent"] = 1.0
                terminateds["low_level_agent"] = True
                if self.current_room == self.goal_room:
                    rewards["high_level_agent"] += 10.0
                    terminateds["__all__"] = True
                return {
                    "high_level_agent": self.current_room,
                    "low_level_agent": self._get_low_level_obs()
                }, rewards, terminateds, truncateds, {}

            # Increment low-level step counter
            self.low_level_steps += 1
            if self.low_level_steps >= MAX_STEPS_LOW_LEVEL:
                terminateds["low_level_agent"] = True

            return {
                "high_level_agent": self.current_room,
                "low_level_agent": self._get_low_level_obs()
            }, rewards, terminateds, truncateds, {}

    def _get_next_pos(self, action):
        x, y = self.agent_pos
        if action == 0:  # Up
            return x - 1, y
        elif action == 1:  # Down
            return x + 1, y
        elif action == 2:  # Left
            return x, y - 1
        elif action == 3:  # Right
            return x, y + 1
        else:
            return x, y
