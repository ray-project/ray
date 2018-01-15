from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

"""Ray constants used in the Python code."""


# Abort autoscaling if more than this number of errors are encountered. This
# is a safety feature to prevent e.g. runaway node launches.
AUTOSCALER_MAX_NUM_FAILURES = 5

# Max number of nodes to launch at a time.
AUTOSCALER_MAX_CONCURRENT_LAUNCHES = 10

# Interval at which to perform autoscaling updates.
AUTOSCALER_UPDATE_INTERVAL_S = 5

# The autoscaler will attempt to restart Ray on nodes it hasn't heard from
# in more than this interval.
AUTOSCALER_HEARTBEAT_TIMEOUT_S = 30
