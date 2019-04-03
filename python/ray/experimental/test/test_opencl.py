from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.experimental.opencl import OpenCL

c = OpenCL()

# test if opencl drivers are available
c.get_gpu_ids()
