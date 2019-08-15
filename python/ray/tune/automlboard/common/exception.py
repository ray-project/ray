from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class CollectorError(Exception):
    """Error raised from the collector service."""

    pass


class DatabaseError(Exception):
    """Error raised from the database manager."""

    pass
