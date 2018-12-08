from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def override(cls):
    """Annotation for documenting method overrides.
    
    Arguments:
        cls (type): The superclass that provides the overriden method. If this
            cls does not actually have the method, an error is raised.
    """

    def check_override(method):
        if method.__name__ not in dir(cls):
            raise NameError("{} does not override any method of {}".format(
                method, cls))
        return method

    return check_override
