class NoReturn(object):
    """Do not store the return value in the object store.

    If a task returns this object, then Ray will not store this object in the
    object store. Calling `ray.get` on the task's return ObjectIDs may block
    indefinitely unless the task manually stores a objects corresponding to the
    ObjectIds.
    """

    def __init__(self):
        raise TypeError("The `NoReturn` object should not be instantiated")
