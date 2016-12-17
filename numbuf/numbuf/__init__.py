from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# See https://github.com/ray-project/ray/issues/131.
helpful_message = """

If you are using Anaconda, try fixing this problem by running:

  conda install libgcc
"""

try:
  from numbuf.libnumbuf import *
except ImportError as e:
  if not hasattr(e, "msg") or not isinstance(e.msg, str) or not "GLIBCXX" in e.msg:
    raise
  e.msg += helpful_message
  raise
