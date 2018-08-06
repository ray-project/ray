from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import copy


DONE = "done"
HOSTNAME = "hostname"
NODE_IP = "node_ip"
PID = "pid"
TIMESTEPS_THIS_ITER = "timesteps_this_iter"
TIMESTEPS_TOTAL = "timesteps_total"
TIME_THIS_ITER_S = "time_this_iter_s"
TIME_TOTAL_S = "time_total_s"
TRAINING_ITERATION = "training_iteration"

# Where Tune writes result files by default
DEFAULT_RESULTS_DIR = os.path.expanduser("~/ray_results")
