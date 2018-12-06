from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def override(cls):
    """Annotation for documenting method overrides."""
    def overrider(method):
        if method.__name__ not in dir(cls):
            raise NameError(
                "{} does not override any method of {}".format(method, cls))
        return method
    return overrider
