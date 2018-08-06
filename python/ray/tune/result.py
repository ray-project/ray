from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

# (Optional/Auto-filled) training is terminated. Filled only if not provided.
DONE = "done"

# (Auto-filled) The hostname of the machine hosting the training process.
HOSTNAME = "hostname"

# (Auto-filled) The node ip of the machine hosting the training process.
NODE_IP = "node_ip"

# (Auto-filled) The pid of the training process.
PID = "pid"

# Number of timesteps in this iteration.
TIMESTEPS_THIS_ITER = "timesteps_this_iter"

# (Optional/Auto-filled) Accumulated time in seconds for this experiment.
TIMESTEPS_TOTAL = "timesteps_total"

# (Auto-filled) Time in seconds this iteration took to run.
# This may be overriden to override the system-computed time difference.
TIME_THIS_ITER_S = "time_this_iter_s"

# (Auto-filled) Accumulated time in seconds for this entire experiment.
TIME_TOTAL_S = "time_total_s"

# (Auto-filled) The index of thistraining iteration.
TRAINING_ITERATION = "training_iteration"

# Where Tune writes result files by default
DEFAULT_RESULTS_DIR = os.path.expanduser("~/ray_results")
