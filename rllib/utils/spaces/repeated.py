import gym

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class Repeated(gym.Space):
    """Represents a variable-length list of child spaces.

    Example:
        self.observation_space = spaces.Repeated(spaces.Box(4,), max_len=10)
            --> from 0 to 10 boxes of shape (4,)

    See also: documentation for rllib.models.RepeatedValues, which shows how
        the lists are represented as batched input for ModelV2 classes.
    """

    def __init__(self, child_space: gym.Space, max_len: int):
        super().__init__()
        self.child_space = child_space
        self.max_len = max_len

    def sample(self):
        return [
            self.child_space.sample()
            for _ in range(self.np_random.randint(1, self.max_len + 1))
        ]

    def contains(self, x):
        return (isinstance(x, list) and len(x) <= self.max_len
                and all(self.child_space.contains(c) for c in x))
