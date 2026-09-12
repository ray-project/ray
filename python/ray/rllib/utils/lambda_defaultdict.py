from collections import defaultdict
from typing import Any, Callable


class LambdaDefaultDict(defaultdict):
    """A defaultdict that creates default values based on the associated key.

    Note that the standard defaultdict can only produce default values (via its factory)
    that are independent of the key under which they are stored.
    As opposed to that, the lambda functions used as factories for this
    `LambdaDefaultDict` class do accept a single argument: The missing key.
    If a missing key is accessed by the user, the provided lambda function is called
    with this missing key as its argument. The returned value is stored in the
    dictionary under that key and returned.

    Example:

        In this example, if you try to access a key that doesn't exist, it will call
        the lambda function, passing it the missing key. The function will return a
        string, which will be stored in the dictionary under that key.

        .. testcode::

            from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict

            default_dict = LambdaDefaultDict(lambda missing_key: f"Value for {missing_key}")
            print(default_dict["a"])

        .. testoutput::

            Value for a
    """  # noqa: E501

    def __init__(self, default_factory: Callable[[str], Any], *args, **kwargs):
        """Initializes a LambdaDefaultDict instance.

        Args:
            default_factory: The default factory callable, taking a string (key)
                and returning the default value to use for that key.
        """
        if not callable(default_factory):
            raise TypeError("First argument must be a Callable!")

        # We will handle the factory in __missing__ method.
        super().__init__(None, *args, **kwargs)

        self.default_factory = default_factory

    def __missing__(self, key):
        # Call default factory with the key as argument.
        self[key] = value = self.default_factory(key)
        return value
