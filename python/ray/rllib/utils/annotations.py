from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def abstractmethod(cls):
    """Annotation for documenting that a method is meant to be overriden."""

    def wrapper(method):
        return method

    return wrapper


def override(cls):
    """Annotation for documenting method overrides."""

    def wrapper(method):
        if method.__name__ not in dir(cls):
            raise NameError("{} does not override any method of {}".format(
                method, cls))
        return method

    return wrapper
