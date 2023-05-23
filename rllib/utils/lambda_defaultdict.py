from collections import defaultdict
from typing import Any, Callable


class LambdaDefaultDict(defaultdict):
    """A defaultdict subclass that takes a lambda function as a default_factory.

    The lambda function is expected to accept a single argument: the missing key.

    If a missing key is accessed, the provided lambda function is called with the
    missing key as its argument. The returned value is stored in the dictionary
    under that key and returned.

    If no lambda function is provided and a missing key is accessed, a KeyError is
    raised.

    Example:

    >>> # In this example, if you try to access a key that doesn't exist, it will call
    >>> # the lambda function, passing it the missing key. The function will return a
    >>> # string, which will be stored in the dictionary under that key.
    >>> default_dict = LambdaDefaultDict(lambda missing_key: f"Value for {missing_key}")
    >>> print(default_dict["a"])
    ... "Value for a"
    """

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
