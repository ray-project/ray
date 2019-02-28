from gym import spaces
import numpy as np

OBS_SPACE = spaces.Box(low=-10, high=10, shape=(4, ), dtype=np.float32)
ACT_SPACE = spaces.Discrete(2)
NUM_AGENTS = 8
AGENTS = list(range(NUM_AGENTS))

NUM_CPUS = 1