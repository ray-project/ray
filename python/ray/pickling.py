# Note that a little bit of code here is taken and slightly modified from the
# pickler because it was not possible to change its behavior otherwise.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from cloudpickle import load, loads, dump, dumps
