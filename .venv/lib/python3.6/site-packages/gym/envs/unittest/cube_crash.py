import numpy as np
import gym
from gym import spaces
from gym.utils import seeding

# Unit test environment for CNNs and CNN+RNN algorithms.
# Looks like this (RGB observations):
#
#  ---------------------------
# |                           |
# |                           |
# |                           |
# |          **               |
# |          **               |
# |                           |
# |                           |
# |                           |
# |                           |
# |                           |
#  ========     ==============
#
# Goal is to go through the hole at the bottom. Agent controls square using Left-Nop-Right actions.
# It falls down automatically, episode length is a bit less than FIELD_H
#
# CubeCrash-v0                    # shaped reward
# CubeCrashSparse-v0              # reward 0 or 1 at the end
# CubeCrashScreenBecomesBlack-v0  # for RNNs
#
# To see how it works, run:
#
# python examples/agents/keyboard_agent.py CubeCrashScreen-v0

FIELD_W = 32
FIELD_H = 40
HOLE_WIDTH = 8

color_black = np.array((0,0,0)).astype('float32')
color_white = np.array((255,255,255)).astype('float32')
color_green = np.array((0,255,0)).astype('float32')

class CubeCrash(gym.Env):
    metadata = {
        'render.modes': ['human', 'rgb_array'],
        'video.frames_per_second' : 60,
        'video.res_w' : FIELD_W,
        'video.res_h' : FIELD_H,
    }

    use_shaped_reward = True
    use_black_screen  = False
    use_random_colors = False   # Makes env too hard

    def __init__(self):
        self.seed()
        self.viewer = None

        self.observation_space = spaces.Box(0, 255, (FIELD_H,FIELD_W,3), dtype=np.uint8)
        self.action_space = spaces.Discrete(3)

        self.reset()

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def random_color(self):
        return np.array([
            self.np_random.randint(low=0, high=255),
            self.np_random.randint(low=0, high=255),
            self.np_random.randint(low=0, high=255),
            ]).astype('uint8')

    def reset(self):
        self.cube_x = self.np_random.randint(low=3, high=FIELD_W-3)
        self.cube_y = self.np_random.randint(low=3, high=FIELD_H//6)
        self.hole_x = self.np_random.randint(low=HOLE_WIDTH, high=FIELD_W-HOLE_WIDTH)
        self.bg_color = self.random_color() if self.use_random_colors else color_black
        self.potential  = None
        self.step_n = 0
        while 1:
            self.wall_color = self.random_color() if self.use_random_colors else color_white
            self.cube_color = self.random_color() if self.use_random_colors else color_green
            if np.linalg.norm(self.wall_color - self.bg_color) < 50 or np.linalg.norm(self.cube_color - self.bg_color) < 50: continue
            break
        return self.step(0)[0]

    def step(self, action):
        if action==0: pass
        elif action==1: self.cube_x -= 1
        elif action==2: self.cube_x += 1
        else: assert 0, "Action %i is out of range" % action
        self.cube_y += 1
        self.step_n += 1

        obs = np.zeros( (FIELD_H,FIELD_W,3), dtype=np.uint8 )
        obs[:,:,:] = self.bg_color
        obs[FIELD_H-5:FIELD_H,:,:] = self.wall_color
        obs[FIELD_H-5:FIELD_H, self.hole_x-HOLE_WIDTH//2:self.hole_x+HOLE_WIDTH//2+1, :] = self.bg_color
        obs[self.cube_y-1:self.cube_y+2, self.cube_x-1:self.cube_x+2, :] = self.cube_color
        if self.use_black_screen and self.step_n > 4:
            obs[:] = np.zeros((3,), dtype=np.uint8)

        done = False
        reward = 0
        dist = np.abs(self.cube_x - self.hole_x)
        if self.potential is not None and self.use_shaped_reward:
            reward = (self.potential - dist) * 0.01
        self.potential = dist

        if self.cube_x-1 < 0 or self.cube_x+1 >= FIELD_W:
            done = True
            reward = -1
        elif self.cube_y+1 >= FIELD_H-5:
            if dist >= HOLE_WIDTH//2:
                done = True
                reward = -1
            elif self.cube_y == FIELD_H:
                done = True
                reward = +1
        self.last_obs = obs
        return obs, reward, done, {}

    def render(self, mode='human'):
        if mode == 'rgb_array':
            return self.last_obs

        elif mode == 'human':
            from gym.envs.classic_control import rendering
            if self.viewer is None:
                self.viewer = rendering.SimpleImageViewer()
            self.viewer.imshow(self.last_obs)
            return self.viewer.isopen

        else:
            assert 0, "Render mode '%s' is not supported" % mode

    def close(self):
        if self.viewer is not None:
            self.viewer.close()
            self.viewer = None

class CubeCrashSparse(CubeCrash):
    use_shaped_reward = False

class CubeCrashScreenBecomesBlack(CubeCrash):
    use_shaped_reward = False
    use_black_screen = True

