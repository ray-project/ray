"""
Task is to copy content multiple times from the input tape to
the output tape. http://arxiv.org/abs/1511.07275
"""
from gym.envs.algorithmic import algorithmic_env


class RepeatCopyEnv(algorithmic_env.TapeAlgorithmicEnv):
    MIN_REWARD_SHORTFALL_FOR_PROMOTION = -.1

    def __init__(self, base=5):
        super(RepeatCopyEnv, self).__init__(base=base, chars=True)
        self.last = 50

    def target_from_input_data(self, input_data):
        return input_data + list(reversed(input_data)) + input_data
