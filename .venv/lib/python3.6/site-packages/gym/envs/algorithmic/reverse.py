"""
Task is to reverse content over the input tape.
http://arxiv.org/abs/1511.07275
"""
from gym.envs.algorithmic import algorithmic_env


class ReverseEnv(algorithmic_env.TapeAlgorithmicEnv):
    MIN_REWARD_SHORTFALL_FOR_PROMOTION = -.1

    def __init__(self, base=2):
        super(ReverseEnv, self).__init__(base=base, chars=True, starting_min_length=1)
        self.last = 50

    def target_from_input_data(self, input_str):
        return list(reversed(input_str))
