from typing import Dict, Optional

import gymnasium as gym

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class FlexDict(gym.spaces.Dict):
    """Gym Dictionary with arbitrary keys updatable after instantiation

    Example:
       space = FlexDict({})
       space['key'] = spaces.Box(4,)
    See also: documentation for gym.spaces.Dict
    """

    def __init__(
        self,
        spaces: Optional[Dict[str, gym.spaces.Space]] = None,
        **spaces_kwargs: gym.spaces.Space,
    ):
        """Initializes a FlexDict instance.

        Args:
            spaces: Dict mapping keys to the sub-spaces of this Dict space. Mutually
                exclusive with `spaces_kwargs`.
            **spaces_kwargs: Alternative way of providing the sub-spaces, in which
                each keyword argument name is a key of this Dict space.
        """
        err = "Use either Dict(spaces=dict(...)) or Dict(foo=x, bar=z)"
        assert (spaces is None) or (not spaces_kwargs), err

        if spaces is None:
            spaces = spaces_kwargs

        for space in spaces.values():
            self.assertSpace(space)

        super().__init__(spaces=spaces)

    def assertSpace(self, space):
        err = "Values of the dict should be instances of gym.Space"
        assert issubclass(type(space), gym.spaces.Space), err

    def sample(self):
        return {k: space.sample() for k, space in self.spaces.items()}

    def __getitem__(self, key):
        return self.spaces[key]

    def __setitem__(self, key, space):
        self.assertSpace(space)
        self.spaces[key] = space

    def __repr__(self):
        return (
            "FlexDict("
            + ", ".join([str(k) + ":" + str(s) for k, s in self.spaces.items()])
            + ")"
        )
