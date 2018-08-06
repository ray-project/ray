from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import copy

# Where Tune writes result files by default
DEFAULT_RESULTS_DIR = os.path.expanduser("~/ray_results")


class TrainingResult(dict):
    """Dict for storing results for Tune, with extra properties.

    When using Tune with custom training scripts, you must periodically report
    training status back to Ray by calling reporter(result).

    See also: ray.tune.Trainable.

    Attributes:
        done (bool): (Optional) If training is terminated.
        time_this_iter_s (float): (Auto-filled) Time in seconds
            this iteration took to run. This may be overriden in order to
            override the system-computed time difference.
        time_total_s (float): (Auto-filled) Accumulated time in seconds
            for this entire experiment.
        experiment_id (str): (Auto-filled) Unique string identifier
            for this experiment. This id is preserved
            across checkpoint / restore calls.
        training_iteration (int): (Auto-filled) The index of this
            training iteration, e.g. call to train().
        pid (str): (Auto-filled) The pid of the training process.
        date (str): (Auto-filled) A formatted date of
            when the result was processed.
        timestamp (str): (Auto-filled) A UNIX timestamp of
            when the result was processed.
        hostname (str): (Auto-filled) The hostname of the machine
            hosting the training process.
        node_ip (str): (Auto-filled) The node ip of the machine
            hosting the training process.
    """
    def __init__(self, *args, **kwargs):
        # `done` has special treatment because the responsibility
        # to set it is shared between user and system, and to ensure
        # CSV Writer doesn't fail, this is defaulted just in case.
        default_values = {
            "done": False
        }
        kwargs.update(default_values)
        super(TrainingResult, self).__init__(*args, **kwargs)

    def copy(self):
        return TrainingResult(**self)

    def __repr__(self):
        return "TrainingResult({})".format(super(TrainingResult, self).__repr__())

    def __getattribute__(self, name):
        """Overrides default to first query internal dict state.

        This is needed to support Pickle/PyArrow serialization."""
        if name in self:
            return self[name]
        elif hasattr(super(TrainingResult, self), name):
            return super(TrainingResult, self).__getattribute__(name)
        else:
            raise AttributeError("'{}' not found in TrainingResult!".format(name))
