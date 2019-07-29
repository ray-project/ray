from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ..agents.trainer import Trainer
from ..utils import renamed_agent

Agent = renamed_agent(Trainer)
