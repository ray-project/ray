"""
Task is to return every nth character from the input tape.
http://arxiv.org/abs/1511.07275
"""
from __future__ import division
from gym.envs.algorithmic import algorithmic_env


class DuplicatedInputEnv(algorithmic_env.TapeAlgorithmicEnv):
    def __init__(self, duplication=2, base=5):
        self.duplication = duplication
        super(DuplicatedInputEnv, self).__init__(base=base, chars=True)

    def generate_input_data(self, size):
        res = []
        if size < self.duplication:
            size = self.duplication
        for i in range(size//self.duplication):
            char = self.np_random.randint(self.base)
            for _ in range(self.duplication):
                res.append(char)
        return res

    def target_from_input_data(self, input_data):
        return [input_data[i] for i in range(0, len(input_data), self.duplication)]
