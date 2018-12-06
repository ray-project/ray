from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def abstractmethod(method):
    """Annotation for documenting that a method is meant to be overriden."""

    return method


def override(cls):
    """Annotation for documenting method overrides."""

    def check_override(method):
        if method.__name__ not in dir(cls):
            raise NameError("{} does not override any method of {}".format(
                method, cls))
        return method

    return check_override
