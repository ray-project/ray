from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np 
from datetime import datetime
import cProfile, pstats, io

def timestamp():
    return datetime.now().timestamp()

class Profiler(object):
    def __init__(self):
        self.pr = cProfile.Profile()
        pass

    def __enter__(self):
        self.pr.enable()

    def __exit__(self ,type, value, traceback):
        self.pr.disable()
        s = io.StringIO()
        sortby = 'cumtime'
        ps = pstats.Stats(self.pr, stream=s).sort_stats(sortby)
        ps.print_stats(.2)
        print(s.getvalue())
