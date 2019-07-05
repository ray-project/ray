"""
Algorithmic environments have the following traits in common:

- A 1-d "input tape" or 2-d "input grid" of characters
- A target string which is a deterministic function of the input characters

Agents control a read head that moves over the input tape. Observations consist
of the single character currently under the read head. The read head may fall
off the end of the tape in any direction. When this happens, agents will observe
a special blank character (with index=env.base) until they get back in bounds.

Actions consist of 3 sub-actions:
    - Direction to move the read head (left or right, plus up and down for 2-d envs)
    - Whether to write to the output tape
    - Which character to write (ignored if the above sub-action is 0)

An episode ends when:
    - The agent writes the full target string to the output tape.
    - The agent writes an incorrect character.
    - The agent runs out the time limit. (Which is fairly conservative.)

Reward schedule:
    write a correct character: +1
    write a wrong character: -.5
    run out the clock: -1
    otherwise: 0

In the beginning, input strings will be fairly short. After an environment has
been consistently solved over some window of episodes, the environment will
increase the average length of generated strings. Typical env specs require
leveling up many times to reach their reward threshold.
"""
from gym import Env, logger
from gym.spaces import Discrete, Tuple
from gym.utils import colorize, seeding
import sys
from contextlib import closing
import numpy as np
from six import StringIO


class AlgorithmicEnv(Env):

    metadata = {'render.modes': ['human', 'ansi']}
    # Only 'promote' the length of generated input strings if the worst of the
    # last n episodes was no more than this far from the maximum reward
    MIN_REWARD_SHORTFALL_FOR_PROMOTION = -1.0

    def __init__(self, base=10, chars=False, starting_min_length=2):
        """
        base: Number of distinct characters.
        chars: If True, use uppercase alphabet. Otherwise, digits. Only affects
               rendering.
        starting_min_length: Minimum input string length. Ramps up as episodes
                             are consistently solved.
        """
        self.base = base
        # Keep track of this many past episodes
        self.last = 10
        # Cumulative reward earned this episode
        self.episode_total_reward = None
        # Running tally of reward shortfalls. e.g. if there were 10 points to
        # earn and we got 8, we'd append -2
        AlgorithmicEnv.reward_shortfalls = []
        if chars:
            self.charmap = [chr(ord('A')+i) for i in range(base)]
        else:
            self.charmap = [str(i) for i in range(base)]
        self.charmap.append(' ')
        # TODO: Not clear why this is a class variable rather than instance.
        # Could lead to some spooky action at a distance if someone is working
        # with multiple algorithmic envs at once. Also makes testing tricky.
        AlgorithmicEnv.min_length = starting_min_length
        # Three sub-actions:
        #       1. Move read head left or right (or up/down)
        #       2. Write or not
        #       3. Which character to write. (Ignored if should_write=0)
        self.action_space = Tuple(
            [Discrete(len(self.MOVEMENTS)), Discrete(2), Discrete(self.base)]
        )
        # Can see just what is on the input tape (one of n characters, or
        # nothing)
        self.observation_space = Discrete(self.base + 1)
        self.seed()
        self.reset()

    @classmethod
    def _movement_idx(kls, movement_name):
        return kls.MOVEMENTS.index(movement_name)

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def _get_obs(self, pos=None):
        """Return an observation corresponding to the given read head position
        (or the current read head position, if none is given)."""
        raise NotImplementedError

    def _get_str_obs(self, pos=None):
        ret = self._get_obs(pos)
        return self.charmap[ret]

    def _get_str_target(self, pos):
        """Return the ith character of the target string (or " " if index
        out of bounds)."""
        if pos < 0 or len(self.target) <= pos:
            return " "
        else:
            return self.charmap[self.target[pos]]

    def render_observation(self):
        """Return a string representation of the input tape/grid."""
        raise NotImplementedError

    def render(self, mode='human'):
        outfile = StringIO() if mode == 'ansi' else sys.stdout
        inp = "Total length of input instance: %d, step: %d\n" % (self.input_width, self.time)
        outfile.write(inp)
        x, y, action = self.read_head_position, self.write_head_position, self.last_action
        if action is not None:
            inp_act, out_act, pred = action
        outfile.write("=" * (len(inp) - 1) + "\n")
        y_str =      "Output Tape         : "
        target_str = "Targets             : "
        if action is not None:
            pred_str = self.charmap[pred]
        x_str = self.render_observation()
        for i in range(-2, len(self.target) + 2):
            target_str += self._get_str_target(i)
            if i < y - 1:
                y_str += self._get_str_target(i)
            elif i == (y - 1):
                if action is not None and out_act == 1:
                    color = 'green' if pred == self.target[i] else 'red'
                    y_str += colorize(pred_str, color, highlight=True)
                else:
                    y_str += self._get_str_target(i)
        outfile.write(x_str)
        outfile.write(y_str + "\n")
        outfile.write(target_str + "\n\n")

        if action is not None:
            outfile.write("Current reward      :   %.3f\n" % self.last_reward)
            outfile.write("Cumulative reward   :   %.3f\n" % self.episode_total_reward)
            move = self.MOVEMENTS[inp_act]
            outfile.write("Action              :   Tuple(move over input: %s,\n" % move)
            out_act = out_act == 1
            outfile.write("                              write to the output tape: %s,\n" % out_act)
            outfile.write("                              prediction: %s)\n" % pred_str)
        else:
            outfile.write("\n" * 5)

        if mode != 'human':
            with closing(outfile):
                return outfile.getvalue()

    @property
    def input_width(self):
        return len(self.input_data)

    def step(self, action):
        assert self.action_space.contains(action)
        self.last_action = action
        inp_act, out_act, pred = action
        done = False
        reward = 0.0
        self.time += 1
        assert 0 <= self.write_head_position
        if out_act == 1:
            try:
                correct = pred == self.target[self.write_head_position]
            except IndexError:
                logger.warn(
                    "It looks like you're calling step() even though this "
                    "environment has already returned done=True. You should "
                    "always call reset() once you receive done=True. Any "
                    "further steps are undefined behaviour.")
                correct = False
            if correct:
                reward = 1.0
            else:
                # Bail as soon as a wrong character is written to the tape
                reward = -0.5
                done = True
            self.write_head_position += 1
            if self.write_head_position >= len(self.target):
                done = True
        self._move(inp_act)
        if self.time > self.time_limit:
            reward = -1.0
            done = True
        obs = self._get_obs()
        self.last_reward = reward
        self.episode_total_reward += reward
        return (obs, reward, done, {})

    @property
    def time_limit(self):
        """If an agent takes more than this many timesteps, end the episode
        immediately and return a negative reward."""
        # (Seemingly arbitrary)
        return self.input_width + len(self.target) + 4

    def _check_levelup(self):
        """Called between episodes. Update our running record of episode rewards
        and, if appropriate, 'level up' minimum input length."""
        if self.episode_total_reward is None:
            # This is before the first episode/call to reset(). Nothing to do
            return
        AlgorithmicEnv.reward_shortfalls.append(self.episode_total_reward - len(self.target))
        AlgorithmicEnv.reward_shortfalls = AlgorithmicEnv.reward_shortfalls[-self.last:]
        if len(AlgorithmicEnv.reward_shortfalls) == self.last and \
                min(AlgorithmicEnv.reward_shortfalls) >= self.MIN_REWARD_SHORTFALL_FOR_PROMOTION and \
                AlgorithmicEnv.min_length < 30:
            AlgorithmicEnv.min_length += 1
            AlgorithmicEnv.reward_shortfalls = []

    def reset(self):
        self._check_levelup()
        self.last_action = None
        self.last_reward = 0
        self.read_head_position = self.READ_HEAD_START
        self.write_head_position = 0
        self.episode_total_reward = 0.0
        self.time = 0
        length = self.np_random.randint(3) + AlgorithmicEnv.min_length
        self.input_data = self.generate_input_data(length)
        self.target = self.target_from_input_data(self.input_data)
        return self._get_obs()

    def generate_input_data(self, size):
        raise NotImplementedError

    def target_from_input_data(self, input_data):
        raise NotImplementedError("Subclasses must implement")

    def _move(self, movement):
        raise NotImplementedError


class TapeAlgorithmicEnv(AlgorithmicEnv):
    """An algorithmic env with a 1-d input tape."""
    MOVEMENTS = ['left', 'right']
    READ_HEAD_START = 0

    def _move(self, movement):
        named = self.MOVEMENTS[movement]
        self.read_head_position += 1 if named == 'right' else -1

    def _get_obs(self, pos=None):
        if pos is None:
            pos = self.read_head_position
        if pos < 0:
            return self.base
        if isinstance(pos, np.ndarray):
            pos = pos.item()
        try:
            return self.input_data[pos]
        except IndexError:
            return self.base

    def generate_input_data(self, size):
        return [self.np_random.randint(self.base) for _ in range(size)]

    def render_observation(self):
        x = self.read_head_position
        x_str = "Observation Tape    : "
        for i in range(-2, self.input_width + 2):
            if i == x:
                x_str += colorize(self._get_str_obs(np.array([i])), 'green', highlight=True)
            else:
                x_str += self._get_str_obs(np.array([i]))
        x_str += "\n"
        return x_str


class GridAlgorithmicEnv(AlgorithmicEnv):
    """An algorithmic env with a 2-d input grid."""
    MOVEMENTS = ['left', 'right', 'up', 'down']
    READ_HEAD_START = (0, 0)

    def __init__(self, rows, *args, **kwargs):
        self.rows = rows
        AlgorithmicEnv.__init__(self, *args, **kwargs)

    def _move(self, movement):
        named = self.MOVEMENTS[movement]
        x, y = self.read_head_position
        if named == 'left':
            x -= 1
        elif named == 'right':
            x += 1
        elif named == 'up':
            y -= 1
        elif named == 'down':
            y += 1
        else:
            raise ValueError("Unrecognized direction: {}".format(named))
        self.read_head_position = x, y

    def generate_input_data(self, size):
        return [
            [self.np_random.randint(self.base) for _ in range(self.rows)]
            for __ in range(size)
        ]

    def _get_obs(self, pos=None):
        if pos is None:
            pos = self.read_head_position
        x, y = pos
        if any(idx < 0 for idx in pos):
            return self.base
        try:
            return self.input_data[x][y]
        except IndexError:
            return self.base

    def render_observation(self):
        x = self.read_head_position
        label = "Observation Grid    : "
        x_str = ""
        for j in range(-1, self.rows+1):
            if j != -1:
                x_str += " " * len(label)
            for i in range(-2, self.input_width + 2):
                if i == x[0] and j == x[1]:
                    x_str += colorize(self._get_str_obs((i, j)), 'green', highlight=True)
                else:
                    x_str += self._get_str_obs((i, j))
            x_str += "\n"
        x_str = label + x_str
        return x_str
