from collections import namedtuple
from ray.rllib.utils.deprecation import deprecation_warning


# NOTE: This is a deprecated class. Use native python tuples
# or dicts (both arbitrarily nested) for multi-actions from here on.
class TupleActions(namedtuple("TupleActions", ["batches"])):
    """Used to return tuple actions as a list of batches per tuple element."""

    def __new__(cls, batches):
        # Throw an informative error if used.
        deprecation_warning(
            old="TupleActions",
            new="`native python tuples (arbitrarily nested)`",
            error=True)
        return super(TupleActions, cls).__new__(cls, batches)

    def numpy(self):
        return TupleActions([b.numpy() for b in self.batches])
