"""
Task is to copy content from the input tape to
the output tape. http://arxiv.org/abs/1511.07275
"""
from gym.envs.algorithmic import algorithmic_env


class CopyEnv(algorithmic_env.TapeAlgorithmicEnv):
    def __init__(self, base=5, chars=True):
        super(CopyEnv, self).__init__(base=base, chars=chars)

    def target_from_input_data(self, input_data):
        return input_data
