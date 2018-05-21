from __future__ import print_function


class TimeSuite(object):
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    def setup(self):
        self.d = {}
        for x in range(500):
            self.d[x] = None

    def time_keys(self):
        for key in self.d.keys():
            pass

    def time_range(self):
        d = self.d
        for key in range(500):
            x = d[key]



class MemSuite(object):
    def mem_list(self):
        return [0] * 256
