import gym

from ray.rllib.utils.annotations import override, PublicAPI


@PublicAPI
class FlexDict(gym.spaces.Dict):
    """Gym Dict with arbitrary keys updatable after instantiation.

    Adds the __setitem__ method so new keys can be inserted after
    instantiation. See also: documentation for gym.spaces.Dict

    Example:
    >>> space = FlexDict({})
    >>> space["key"] = spaces.Box(4,)
    """

    def assert_space(self, space):
        err = "Values of the dict should be instances of gym.Space"
        assert issubclass(type(space), gym.spaces.Space), err

    @override(gym.spaces.Dict)
    def sample(self):
        return {k: space.sample() for k, space in self.spaces.items()}

    def __setitem__(self, key, space):
        self.assert_space(space)
        self.spaces[key] = space

    @override(gym.spaces.Dict)
    def __repr__(self):
        return "FlexDict(" + ", ".join(
            [str(k) + ":" + str(s) for k, s in self.spaces.items()]) + ")"

    @override(gym.spaces.Dict)
    def __eq__(self, other):
        return isinstance(other, FlexDict) and self.spaces == other.spaces
