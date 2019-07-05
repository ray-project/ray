import numpy as np
import gym
from gym import spaces
from gym.utils import seeding

# Unit test environment for CNNs.
# Looks like this (RGB observations):
#
#  ---------------------------
# |                           |
# |         ******            |
# |         ******            |
# |       **      **          |
# |       **      **          |
# |               **          |
# |               **          |
# |           ****            |
# |           ****            |
# |       ****                |
# |       ****                |
# |       **********          |
# |       **********          |
# |                           |
#  ---------------------------
#
# Agent should hit action 2 to gain reward. Catches off-by-one errors in your agent.
#
# To see how it works, run:
#
# python examples/agents/keyboard_agent.py MemorizeDigits-v0

FIELD_W = 32
FIELD_H = 24

bogus_mnist = \
[[
" **** ",
"*    *",
"*    *",
"*    *",
"*    *",
" **** "
], [
"  **  ",
" * *  ",
"   *  ",
"   *  ",
"   *  ",
"  *** "
], [
" **** ",
"*    *",
"     *",
"  *** ",
"**    ",
"******"
], [
" **** ",
"*    *",
"   ** ",
"     *",
"*    *",
" **** "
], [
" *  * ",
" *  * ",
" *  * ",
" **** ",
"    * ",
"    * "
], [
" **** ",
" *    ",
" **** ",
"    * ",
"    * ",
" **** "
], [
"  *** ",
" *    ",
" **** ",
" *  * ",
" *  * ",
" **** "
], [
" **** ",
"    * ",
"   *  ",
"   *  ",
"  *   ",
"  *   "
], [
" **** ",
"*    *",
" **** ",
"*    *",
"*    *",
" **** "
], [
" **** ",
"*    *",
"*    *",
" *****",
"     *",
" **** "
]]

color_black = np.array((0,0,0)).astype('float32')
color_white = np.array((255,255,255)).astype('float32')

class MemorizeDigits(gym.Env):
    metadata = {
        'render.modes': ['human', 'rgb_array'],
        'video.frames_per_second' : 60,
        'video.res_w' : FIELD_W,
        'video.res_h' : FIELD_H,
    }

    use_random_colors = False

    def __init__(self):
        self.seed()
        self.viewer = None
        self.observation_space = spaces.Box(0, 255, (FIELD_H,FIELD_W,3), dtype=np.uint8)
        self.action_space = spaces.Discrete(10)
        self.bogus_mnist = np.zeros( (10,6,6), dtype=np.uint8 )
        for digit in range(10):
            for y in range(6):
                self.bogus_mnist[digit,y,:] = [ord(char) for char in bogus_mnist[digit][y]]
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
        self.digit_x = self.np_random.randint(low=FIELD_W//5, high=FIELD_W//5*4)
        self.digit_y = self.np_random.randint(low=FIELD_H//5, high=FIELD_H//5*4)
        self.color_bg = self.random_color() if self.use_random_colors else color_black
        self.step_n = 0
        while 1:
            self.color_digit = self.random_color() if self.use_random_colors else color_white
            if np.linalg.norm(self.color_digit - self.color_bg) < 50: continue
            break
        self.digit = -1
        return self.step(0)[0]

    def step(self, action):
        reward = -1
        done = False
        self.step_n += 1
        if self.digit==-1:
            pass
        else:
            if self.digit==action:
                reward = +1
            done = self.step_n > 20 and 0==self.np_random.randint(low=0, high=5)
        self.digit = self.np_random.randint(low=0, high=10)
        obs = np.zeros( (FIELD_H,FIELD_W,3), dtype=np.uint8 )
        obs[:,:,:] = self.color_bg
        digit_img = np.zeros( (6,6,3), dtype=np.uint8 )
        digit_img[:] = self.color_bg
        xxx = self.bogus_mnist[self.digit]==42
        digit_img[xxx] = self.color_digit
        obs[self.digit_y-3:self.digit_y+3, self.digit_x-3:self.digit_x+3] = digit_img
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

