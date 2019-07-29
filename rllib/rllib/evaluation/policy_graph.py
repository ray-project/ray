from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ..policy.policy import Policy
from ..utils import renamed_class

PolicyGraph = renamed_class(Policy, old_name="PolicyGraph")
