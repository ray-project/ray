from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class UsageTrackingDict(dict):
    """Dict that tracks which keys have been accessed.
    
    We make the simplifying assumption only __getitem__ is used to access
    values.
    """

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.accessed_keys = set()

    def __getitem__(self, key):
        self.accessed_keys.add(key)
        return dict.__getitem__(self, key)
