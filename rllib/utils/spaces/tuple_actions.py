from collections import namedtuple


class TupleActions(namedtuple("TupleActions", ["batches"])):
    """Used to return tuple actions as a list of batches per tuple element."""

    def __new__(cls, batches):
        return super(TupleActions, cls).__new__(cls, batches)

    def numpy(self):
        return TupleActions([b.numpy() for b in self.batches])
