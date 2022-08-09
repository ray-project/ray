import gym
from gym import spaces

ACTION_UP = 0
ACTION_RIGHT = 1
ACTION_DOWN = 2
ACTION_LEFT = 3


class CliffWalkingWallEnv(gym.Env):
    """Modified version of the CliffWalking environment from OpenAI Gym
    with walls instead of a cliff.

    ### Description
    The board is a 4x12 matrix, with (using NumPy matrix indexing):
    - [3, 0] or obs==36 as the start at bottom-left
    - [3, 11] or obs==47 as the goal at bottom-right
    - [3, 1..10] or obs==37...46 as the cliff at bottom-center

    An episode terminates when the agent reaches the goal.

    ### Actions
    There are 4 discrete deterministic actions:
    - 0: move up
    - 1: move right
    - 2: move down
    - 3: move left
    You can also use the constants ACTION_UP, ACTION_RIGHT, ... defined above.

    ### Observations
    There are 3x12 + 2 possible states, not including the walls. If an action
    would move an agent into one of the walls, it simply stays in the same position.

    ### Reward
    Each time step incurs -1 reward, except reaching the goal which gives +10 reward.
    """

    def __init__(self, seed=42) -> None:
        self.observation_space = spaces.Discrete(48)
        self.action_space = spaces.Discrete(4)
        self.observation_space.seed(seed)
        self.action_space.seed(seed)

    def reset(self):
        self.position = 36
        return self.position

    def step(self, action):
        x = self.position // 12
        y = self.position % 12
        # UP
        if action == ACTION_UP:
            x = max(x - 1, 0)
        # RIGHT
        elif action == ACTION_RIGHT:
            if self.position != 36:
                y = min(y + 1, 11)
        # DOWN
        elif action == ACTION_DOWN:
            if self.position < 25 or self.position > 34:
                x = min(x + 1, 3)
        # LEFT
        elif action == ACTION_LEFT:
            if self.position != 47:
                y = max(y - 1, 0)
        else:
            raise ValueError(f"action {action} not in {self.action_space}")
        self.position = x * 12 + y
        done = self.position == 47
        reward = -1 if not done else 10
        return self.position, reward, done, {}
