from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune import TuneError


class StoppingCriteria():
    def __init__(self, criteria):
        self.stopping_criterion = criteria

    def __call__(self, result):
        for criteria, stop_value in self.stopping_criterion.items():
            if criteria not in result:
                raise TuneError(
                    "Stopping criteria {} not provided in result {}.".format(
                        criteria, result))
            if result[criteria] >= stop_value:
                return True
        return False
