from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class RayNotDictionarySerializable(Exception):
    pass


# This exception is used to represent situations where cloudpickle fails to
# pickle an object (cloudpickle can fail in many different ways).
class CloudPickleError(Exception):
    pass


def check_serializable(cls):
    """Throws an exception if Ray cannot serialize this class efficiently.

    Args:
        cls (type): The class to be serialized.

    Raises:
        Exception: An exception is raised if Ray cannot serialize this class
            efficiently.
    """
    if is_named_tuple(cls):
        # This case works.
        return
    if not hasattr(cls, "__new__"):
        print("The class {} does not have a '__new__' attribute and is "
              "probably an old-stye class. Please make it a new-style class "
              "by inheriting from 'object'.")
        raise RayNotDictionarySerializable("The class {} does not have a "
                                           "'__new__' attribute and is "
                                           "probably an old-style class. We "
                                           "do not support this. Please make "
                                           "it a new-style class by "
                                           "inheriting from 'object'."
                                           .format(cls))
    try:
        obj = cls.__new__(cls)
    except Exception:
        raise RayNotDictionarySerializable("The class {} has overridden "
                                           "'__new__', so Ray may not be able "
                                           "to serialize it efficiently."
                                           .format(cls))
    if not hasattr(obj, "__dict__"):
        raise RayNotDictionarySerializable("Objects of the class {} do not "
                                           "have a '__dict__' attribute, so "
                                           "Ray cannot serialize it "
                                           "efficiently.".format(cls))
    if hasattr(obj, "__slots__"):
        raise RayNotDictionarySerializable("The class {} uses '__slots__', so "
                                           "Ray may not be able to serialize "
                                           "it efficiently.".format(cls))


def is_named_tuple(cls):
    """Return True if cls is a namedtuple and False otherwise."""
    b = cls.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(cls, "_fields", None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)
