from ray import ObjectRef


class ObjectRefWrapper:
    def __init__(self, o: ObjectRef):
        self._o = o

    def __reduce__(self):
        return lambda: self._o, tuple()


def pass_by_reference(o: ObjectRef) -> ObjectRefWrapper:
    return ObjectRefWrapper(o)
