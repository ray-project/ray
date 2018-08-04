from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import os
"""
When using Tune with custom training scripts, you must periodically report
training status back to Ray by calling reporter(result).
"""

# Where Tune writes result files by default
DEFAULT_RESULTS_DIR = os.path.expanduser("~/ray_results")

class TrainingResult(dict):
    def __init__(self, training_iteration=None, metric=None, **kwargs):
        super(TrainingResult, self).__init__(
            training_iteration=None, metric=None, **kwargs)
