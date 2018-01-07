from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def standardized(value):
    # Divide by the maximum of value.std() and 1e-4
    # to guard against the case where all values are equal
    return (value - value.mean()) / max(1e-4, value.std())
