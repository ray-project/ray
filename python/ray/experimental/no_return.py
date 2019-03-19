from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class NoReturn(object):
    """Do not store the return value in the object store.

    If a task returns this object, then Ray will not store this object in the
    object store. Calling `ray.get` on the task's return ObjectIDs may block
    indefinitely unless the task manually stores an object for the
    corresponding ObjectID.
    """

    def __init__(self):
        raise TypeError("The `NoReturn` object should not be instantiated")
