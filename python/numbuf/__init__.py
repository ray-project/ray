from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# See https://github.com/ray-project/ray/issues/131.
helpful_message = """

If you are using Anaconda, try fixing this problem by running:

  conda install libgcc
"""

try:
  from core.src.numbuf.libnumbuf import *
except ImportError as e:
  if hasattr(e, "msg") and isinstance(e.msg, str) and ("libstdc++" in e.msg or "CXX" in e.msg):
    # This code path should be taken with Python 3.
    e.msg += helpful_message
  elif hasattr(e, "message") and isinstance(e.message, str) and ("libstdc++" in e.message or "CXX" in e.message):
    # This code path should be taken with Python 2.
    if hasattr(e, "args") and isinstance(e.args, tuple) and len(e.args) == 1 and isinstance(e.args[0], str):
      e.args = (e.args[0] + helpful_message,)
    else:
      if not hasattr(e, "args"):
        e.args = ()
      elif not isinstance(e.args, tuple):
        e.args = (e.args,)
      e.args += (helpful_message,)
  raise
