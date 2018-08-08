from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class CollectorError(Exception):
    """ error raised from the collector service """
    pass


class DatabaseError(Exception):
    """ error raised from the database manager """
    pass
