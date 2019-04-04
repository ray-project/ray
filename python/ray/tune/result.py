from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

# yapf: disable
# __sphinx_doc_begin__
# (Optional/Auto-filled) training is terminated. Filled only if not provided.
DONE = "done"

# (Auto-filled) The hostname of the machine hosting the training process.
HOSTNAME = "hostname"

# (Auto-filled) The node ip of the machine hosting the training process.
NODE_IP = "node_ip"

# (Auto-filled) The pid of the training process.
PID = "pid"

# (Optional) Mean reward for current training iteration
EPISODE_REWARD_MEAN = "episode_reward_mean"

# (Optional) Mean loss for training iteration
MEAN_LOSS = "mean_loss"

# (Optional) Mean accuracy for training iteration
MEAN_ACCURACY = "mean_accuracy"

# Number of episodes in this iteration.
EPISODES_THIS_ITER = "episodes_this_iter"

# (Optional/Auto-filled) Accumulated number of episodes for this experiment.
EPISODES_TOTAL = "episodes_total"

# Number of timesteps in this iteration.
TIMESTEPS_THIS_ITER = "timesteps_this_iter"

# (Auto-filled) Accumulated number of timesteps for this entire experiment.
TIMESTEPS_TOTAL = "timesteps_total"

# (Auto-filled) Time in seconds this iteration took to run.
# This may be overriden to override the system-computed time difference.
TIME_THIS_ITER_S = "time_this_iter_s"

# (Auto-filled) Accumulated time in seconds for this entire experiment.
TIME_TOTAL_S = "time_total_s"

# (Auto-filled) The index of this training iteration.
TRAINING_ITERATION = "training_iteration"
# __sphinx_doc_end__
# yapf: enable

# __duplicate__ is a magic keyword used internally to
# avoid double-logging results when using the Function API.
RESULT_DUPLICATE = "__duplicate__"

# Where Tune writes result files by default
DEFAULT_RESULTS_DIR = (os.environ.get("TUNE_RESULT_DIR")
                       or os.path.expanduser("~/ray_results"))

# Meta file about status under each experiment directory, can be
# parsed by automlboard if exists.
JOB_META_FILE = "job_status.json"

# Meta file about status under each trial directory, can be parsed
# by automlboard if exists.
EXPR_META_FILE = "trial_status.json"

# File that stores parameters of the trial.
EXPR_PARARM_FILE = "params.json"

# File that stores the progress of the trial.
EXPR_PROGRESS_FILE = "progress.csv"

# File that stores results of the trial.
EXPR_RESULT_FILE = "result.json"
