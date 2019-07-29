from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ..utils import renamed_class
from ..evaluation import RolloutWorker

PolicyEvaluator = renamed_class(
    RolloutWorker, old_name="rllib.evaluation.PolicyEvaluator")
